/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apiserver

import (
	"context"
	"fmt"
	"k8s.io/klog/v2"
	"net/http"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	genericfeatures "k8s.io/apiserver/pkg/features"
	peerreconcilers "k8s.io/apiserver/pkg/reconcilers"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/egressselector"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/transport"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/tracing"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	v1helper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	informers "k8s.io/kube-aggregator/pkg/client/informers/externalversions"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	openapicontroller "k8s.io/kube-aggregator/pkg/controllers/openapi"
	openapiaggregator "k8s.io/kube-aggregator/pkg/controllers/openapi/aggregator"
	openapiv3controller "k8s.io/kube-aggregator/pkg/controllers/openapiv3"
	openapiv3aggregator "k8s.io/kube-aggregator/pkg/controllers/openapiv3/aggregator"
	localavailability "k8s.io/kube-aggregator/pkg/controllers/status/local"
	availabilitymetrics "k8s.io/kube-aggregator/pkg/controllers/status/metrics"
	remoteavailability "k8s.io/kube-aggregator/pkg/controllers/status/remote"
	apiservicerest "k8s.io/kube-aggregator/pkg/registry/apiservice/rest"
	openapicommon "k8s.io/kube-openapi/pkg/common"
)

// making sure we only register metrics once into legacy registry
var registerIntoLegacyRegistryOnce sync.Once

func init() {
	// we need to add the options (like ListOptions) to empty v1
	metav1.AddToGroupVersion(aggregatorscheme.Scheme, schema.GroupVersion{Group: "", Version: "v1"})

	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	aggregatorscheme.Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

const (
	// legacyAPIServiceName is the fixed name of the only non-groupified API version
	legacyAPIServiceName = "v1."
	// StorageVersionPostStartHookName is the name of the storage version updater post start hook.
	StorageVersionPostStartHookName = "built-in-resources-storage-version-updater"
)

// ExtraConfig represents APIServices-specific configuration
type ExtraConfig struct {
	// PeerAdvertiseAddress is the IP for this kube-apiserver which is used by peer apiservers to route a request
	// to this apiserver. This happens in cases where the peer is not able to serve the request due to
	// version skew. If unset, AdvertiseAddress/BindAddress will be used.
	PeerAdvertiseAddress peerreconcilers.PeerAdvertiseAddress

	// ProxyClientCert/Key are the client cert used to identify this proxy. Backing APIServices use
	// this to confirm the proxy's identity
	ProxyClientCertFile string
	ProxyClientKeyFile  string

	// If present, the Dial method will be used for dialing out to delegate
	// apiservers.
	ProxyTransport *http.Transport

	// Mechanism by which the Aggregator will resolve services. Required.
	ServiceResolver ServiceResolver

	RejectForwardingRedirects bool

	// DisableRemoteAvailableConditionController disables the controller that updates the Available conditions for
	// remote APIServices via querying endpoints of the referenced services. In generic controlplane use-cases,
	// the concept of services and endpoints might differ, and might require another implementation of this
	// controller. Local APIService are reconciled nevertheless.
	DisableRemoteAvailableConditionController bool
}

// Config represents the configuration needed to create an APIAggregator.
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig same as Config, just to swap private object.
type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

type runnable interface {
	RunWithContext(ctx context.Context) error
}

// preparedGenericAPIServer is a private wrapper that enforces a call of PrepareRun() before Run can be invoked.
type preparedAPIAggregator struct {
	*APIAggregator
	runnable runnable
}

// APIAggregator contains state for a Kubernetes cluster master/api server.
type APIAggregator struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	// provided for easier embedding
	APIRegistrationInformers informers.SharedInformerFactory

	delegateHandler http.Handler

	// proxyCurrentCertKeyContent holds he client cert used to identify this proxy. Backing APIServices use this to confirm the proxy's identity
	proxyCurrentCertKeyContent certKeyFunc
	proxyTransportDial         *transport.DialHolder

	// proxyHandlers are the proxy handlers that are currently registered, keyed by apiservice.name
	proxyHandlers map[string]*proxyHandler
	// handledGroupVersions contain the groups that already have routes. The key is the name of the group and the value
	// is the versions for the group.
	handledGroupVersions map[string]sets.Set[string]

	// lister is used to add group handling for /apis/<group> aggregator lookups based on
	// controller state
	lister listers.APIServiceLister

	// Information needed to determine routing for the aggregator
	serviceResolver ServiceResolver

	// Enable swagger and/or OpenAPI if these configs are non-nil.
	openAPIConfig *openapicommon.Config

	// Enable OpenAPI V3 if these configs are non-nil
	openAPIV3Config *openapicommon.OpenAPIV3Config

	// openAPIAggregationController downloads and merges OpenAPI v2 specs.
	openAPIAggregationController *openapicontroller.AggregationController

	// openAPIV3AggregationController downloads and caches OpenAPI v3 specs.
	openAPIV3AggregationController *openapiv3controller.AggregationController

	// discoveryAggregationController downloads and caches discovery documents
	// from all aggregated apiservices so they are available from /apis endpoint
	// when discovery with resources are requested
	discoveryAggregationController DiscoveryAggregationController

	// rejectForwardingRedirects is whether to allow to forward redirect response
	rejectForwardingRedirects bool

	// tracerProvider is used to wrap the proxy transport and handler with tracing
	tracerProvider tracing.TracerProvider
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	// the kube aggregator wires its own discovery mechanism
	// TODO eventually collapse this by extracting all of the discovery out
	c.GenericConfig.EnableDiscovery = false

	return CompletedConfig{&c}
}

// NewWithDelegate returns a new instance of APIAggregator from the given config.
// NewWithDelegate 方法的作用是：基于已完成的配置和委托目标（delegate），创建并初始化一个功能完备的 APIAggregator 服务器。
// 这个函数是 Aggregator 功能的“总装配车间”，它不仅组装了服务器，还启动了所有必要的后台控制器
func (c completedConfig) NewWithDelegate(delegationTarget genericapiserver.DelegationTarget) (*APIAggregator, error) {
	klog.InfoS("Creating new aggregator server with delegate")
	// 步骤 1: 创建底层的 GenericAPIServer 实例
	// 每个 apiserver（kube-apiserver, aggregator, apiextensions）都内嵌了一个 GenericAPIServer。
	// 这里为 aggregator 创建它的 GenericAPIServer，并将其命名为 "kube-aggregator"。
	// 最关键的是，它将请求的委托目标（delegate）设置为 kube-apiserver，形成了请求处理链。
	genericServer, err := c.GenericConfig.New("kube-aggregator", delegationTarget)
	if err != nil {
		return nil, err
	}
	// 步骤 2: 创建用于操作 APIService 资源的客户端和 Informer 工厂
	// Aggregator 的核心职责就是处理 APIService 资源，所以它需要自己的客户端和 Informer 来与 apiregistration.k8s.io API 组交互。
	klog.V(4).InfoS("Creating client and informer factory for apiregistration.k8s.io")
	apiregistrationClient, err := clientset.NewForConfig(c.GenericConfig.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	informerFactory := informers.NewSharedInformerFactory(
		apiregistrationClient,
		5*time.Minute, // this is effectively used as a refresh interval right now.  Might want to do something nicer later on. // 这个 resync 周期用于定期刷新，确保状态最终一致
	)

	// apiServiceRegistrationControllerInitiated is closed when APIServiceRegistrationController has finished "installing" all known APIServices.
	// At this point we know that the proxy handler knows about APIServices and can handle client requests.
	// Before it might have resulted in a 404 response which could have serious consequences for some controllers like  GC and NS
	//
	// Note that the APIServiceRegistrationController waits for APIServiceInformer to synced before doing its work.
	// 步骤 3: 设置一个信号 channel，用于指示 APIService 注册控制器已完成初始同步
	// 这是一个非常关键的同步机制。在所有 APIService 被正确注册之前，apiserver 的 discovery 信息是不完整的。
	// 通过这个信号，可以确保依赖 discovery 的其他组件（如 discovery controller）在该信号被触发后再开始工作。
	apiServiceRegistrationControllerInitiated := make(chan struct{})
	if err := genericServer.RegisterMuxAndDiscoveryCompleteSignal("APIServiceRegistrationControllerInitiated", apiServiceRegistrationControllerInitiated); err != nil {
		return nil, err
	}
	// 步骤 4: 配置用于代理请求到外部 APIService 的 HTTP Transport
	// 如果配置了 EgressSelector，则使用它来获取一个自定义的 dialer，确保网络策略生效。
	var proxyTransportDial *transport.DialHolder
	if c.GenericConfig.EgressSelector != nil {
		egressDialer, err := c.GenericConfig.EgressSelector.Lookup(egressselector.Cluster.AsNetworkContext())
		if err != nil {
			return nil, err
		}
		if egressDialer != nil {
			proxyTransportDial = &transport.DialHolder{Dial: egressDialer}
		}
	} else if c.ExtraConfig.ProxyTransport != nil && c.ExtraConfig.ProxyTransport.DialContext != nil {
		// 备用方案，如果 egress selector 未配置，则使用传入的 transport。
		proxyTransportDial = &transport.DialHolder{Dial: c.ExtraConfig.ProxyTransport.DialContext}
	}
	// 步骤 5: 初始化 APIAggregator 结构体
	// 这是 Aggregator 服务器的核心数据结构。
	klog.V(4).InfoS("Initializing APIAggregator struct")
	s := &APIAggregator{
		GenericAPIServer:           genericServer,                                                 // 内嵌的 GenericAPIServer
		delegateHandler:            delegationTarget.UnprotectedHandler(),                         // 委托的 handler
		proxyTransportDial:         proxyTransportDial,                                            // 代理请求时用的 dialer
		proxyHandlers:              map[string]*proxyHandler{},                                    // 缓存已创建的 proxy handler
		handledGroupVersions:       map[string]sets.Set[string]{},                                 // 记录已处理的 group/version
		lister:                     informerFactory.Apiregistration().V1().APIServices().Lister(), // 用于快速查询 APIService
		APIRegistrationInformers:   informerFactory,                                               // APIService 的 informer
		serviceResolver:            c.ExtraConfig.ServiceResolver,                                 /// 用于将 Service 名称解析为 IP 和端口
		openAPIConfig:              c.GenericConfig.OpenAPIConfig,
		openAPIV3Config:            c.GenericConfig.OpenAPIV3Config,
		proxyCurrentCertKeyContent: func() (bytes []byte, bytes2 []byte) { return nil, nil }, // 用于获取代理客户端证书内容的函数
		rejectForwardingRedirects:  c.ExtraConfig.RejectForwardingRedirects,                  // 是否拒绝代理请求中的重定向
		tracerProvider:             c.GenericConfig.TracerProvider,                           // 分布式追踪提供者
	}
	// 步骤 6: 安装 apiregistration.k8s.io API 组
	// Aggregator 服务器自己也需要提供 API，即 APIService 资源本身 (`/apis/apiregistration.k8s.io/v1/apiservices`)。
	// 这里将 APIService 资源的 REST Storage 安装到 GenericAPIServer 中，使其能够处理对 APIService 的 CRUD 请求。
	klog.V(2).InfoS("Installing apiregistration.k8s.io API group")
	apiGroupInfo := apiservicerest.NewRESTStorage(c.GenericConfig.MergedResourceConfig, c.GenericConfig.RESTOptionsGetter, false)
	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}
	// 确保 apiregistration.k8s.io/v1 这个版本是启用的，因为 aggregator 的所有逻辑都依赖它。
	enabledVersions := sets.NewString()
	for v := range apiGroupInfo.VersionedResourcesStorageMap {
		enabledVersions.Insert(v)
	}
	if !enabledVersions.Has(v1.SchemeGroupVersion.Version) {
		return nil, fmt.Errorf("API group/version %s must be enabled", v1.SchemeGroupVersion.String())
	}
	// 步骤 7: 安装用于服务 /apis 路径的 Handler
	// `/apis` 这个路径返回所有可用的 API Group 列表，它由一个特殊的 handler 处理。
	klog.V(4).InfoS("Installing /apis discovery handler")
	apisHandler := &apisHandler{
		codecs:         aggregatorscheme.Codecs,
		lister:         s.lister, // 使用 APIService lister 来获取数据，构建 API Group 列表
		discoveryGroup: discoveryGroup(enabledVersions),
	}
	// 将 handler 包装一下，使其支持聚合发现（即合并来自不同来源的 discovery 信息）。
	apisHandlerWithAggregationSupport := aggregated.WrapAggregatedDiscoveryToHandler(apisHandler, s.GenericAPIServer.AggregatedDiscoveryGroupManager)
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/apis", apisHandlerWithAggregationSupport)
	s.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandle("/apis/", apisHandler) // 处理 /apis/ 路径

	// 步骤 8: 创建并配置 APIServiceRegistrationController
	// 这个控制器是 Aggregator 的核心！它的职责是：
	// 1. 监听 APIService 资源的变化。
	// 2. 当 APIService 创建或更新时，为它创建一个对应的 proxy handler，并动态地注册到 HTTP Mux 中。
	// 3. 当 APIService 删除时，注销对应的 handler。
	// 这就是 API 聚合的动态实现。
	klog.V(2).InfoS("Creating APIService registration controller")
	apiserviceRegistrationController := NewAPIServiceRegistrationController(informerFactory.Apiregistration().V1().APIServices(), s)
	// 如果配置了代理客户端证书（用于 apiserver 安全地连接到外部 apiservice），则设置动态重载。
	if len(c.ExtraConfig.ProxyClientCertFile) > 0 && len(c.ExtraConfig.ProxyClientKeyFile) > 0 {
		aggregatorProxyCerts, err := dynamiccertificates.NewDynamicServingContentFromFiles("aggregator-proxy-cert", c.ExtraConfig.ProxyClientCertFile, c.ExtraConfig.ProxyClientKeyFile)
		if err != nil {
			return nil, err
		}
		// We are passing the context to ProxyCerts.RunOnce as it needs to implement RunOnce(ctx) however the
		// context is not used at all. So passing a empty context shouldn't be a problem
		// 立即加载一次证书
		if err := aggregatorProxyCerts.RunOnce(context.Background()); err != nil {
			return nil, err
		}
		// 将 registration controller 添加为监听者，当证书变化时通知它。
		aggregatorProxyCerts.AddListener(apiserviceRegistrationController)
		s.proxyCurrentCertKeyContent = aggregatorProxyCerts.CurrentCertKeyContent
		// 添加一个 PostStartHook，在服务器启动后，启动一个 goroutine 来定期检查证书文件是否更新。
		s.GenericAPIServer.AddPostStartHookOrDie("aggregator-reload-proxy-client-cert", func(postStartHookContext genericapiserver.PostStartHookContext) error {
			go aggregatorProxyCerts.Run(postStartHookContext, 1)
			return nil
		})
	}

	// 步骤 9: 注册 PostStartHook 来启动所有后台任务
	klog.InfoS("Registering PostStartHooks to start background controllers")
	// 9a. 启动 Informer 工厂
	s.GenericAPIServer.AddPostStartHookOrDie("start-kube-aggregator-informers", func(context genericapiserver.PostStartHookContext) error {
		informerFactory.Start(context.Done())
		// 注意：这里也启动了 `genericConfig.SharedInformerFactory`，即主 apiserver 的 informer。
		// 确保所有 informer 都已启动。
		c.GenericConfig.SharedInformerFactory.Start(context.Done())
		return nil
	})

	// create shared (remote and local) availability metrics
	// TODO: decouple from legacyregistry
	// 9b. 创建并启动 APIService 可用性控制器 (local 和 remote)
	// 这些控制器负责监控每个 APIService 的健康状况，并更新其 `.status.conditions` 中的 `Available` 状态。
	metrics := availabilitymetrics.New()
	registerIntoLegacyRegistryOnce.Do(func() { err = metrics.Register(legacyregistry.Register, legacyregistry.CustomRegister) })
	if err != nil {
		return nil, err
	}

	// always run local availability controller
	// 启动 local 可用性控制器，它检查由本 apiserver 提供的 APIService (spec.service 为 nil) 是否可用。
	local, err := localavailability.New(
		informerFactory.Apiregistration().V1().APIServices(),
		apiregistrationClient.ApiregistrationV1(),
		metrics,
	)
	if err != nil {
		return nil, err
	}
	// 9c. 启动核心的 APIServiceRegistrationController
	s.GenericAPIServer.AddPostStartHookOrDie("apiservice-status-local-available-controller", func(context genericapiserver.PostStartHookContext) error {
		// if we end up blocking for long periods of time, we may need to increase workers.
		// 启动控制器，并传入 `apiServiceRegistrationControllerInitiated` channel。
		// 控制器在完成初始同步后会 close 这个 channel。
		go local.Run(5, context.Done())
		return nil
	})

	// conditionally run remote availability controller. This could be replaced in certain
	// generic controlplane use-cases where there is another concept of services and/or endpoints.
	// 启动 remote 可用性控制器，它检查由外部服务提供的 APIService，通过定期向其 `healthz` 端点发送探测请求来判断可用性。
	if !c.ExtraConfig.DisableRemoteAvailableConditionController {
		remote, err := remoteavailability.New(
			informerFactory.Apiregistration().V1().APIServices(),
			c.GenericConfig.SharedInformerFactory.Core().V1().Services(),
			c.GenericConfig.SharedInformerFactory.Discovery().V1().EndpointSlices(),
			apiregistrationClient.ApiregistrationV1(),
			proxyTransportDial,
			(func() ([]byte, []byte))(s.proxyCurrentCertKeyContent),
			s.serviceResolver,
			metrics,
		)
		if err != nil {
			return nil, err
		}
		s.GenericAPIServer.AddPostStartHookOrDie("apiservice-status-remote-available-controller", func(context genericapiserver.PostStartHookContext) error {
			// if we end up blocking for long periods of time, we may need to increase workers.
			go remote.Run(5, context.Done())
			return nil
		})
	}
	// 9c. 启动核心的 APIServiceRegistrationController
	s.GenericAPIServer.AddPostStartHookOrDie("apiservice-registration-controller", func(context genericapiserver.PostStartHookContext) error {
		// 启动控制器，并传入 `apiServiceRegistrationControllerInitiated` channel。
		// 控制器在完成初始同步后会 close 这个 channel。
		go apiserviceRegistrationController.Run(context.Done(), apiServiceRegistrationControllerInitiated)
		select {
		// 阻塞，直到初始同步完成或上下文被取消。
		case <-context.Done():
		case <-apiServiceRegistrationControllerInitiated:
		}

		return nil
	})

	// 9d. 创建并启动 Discovery Aggregation Controller
	// 这个控制器负责将所有可用的 APIService 的 discovery 信息（即 `/apis/group/version` 的内容）
	// 聚合起来，形成一个完整的 discovery 文档。
	s.discoveryAggregationController = NewDiscoveryManager(
		// Use aggregator as the source name to avoid overwriting native/CRD
		// groups
		s.GenericAPIServer.AggregatedDiscoveryGroupManager.WithSource(aggregated.AggregatorSource),
	)

	// Setup discovery endpoint
	s.GenericAPIServer.AddPostStartHookOrDie("apiservice-discovery-controller", func(context genericapiserver.PostStartHookContext) error {
		// Discovery aggregation depends on the apiservice registration controller
		// having the full list of APIServices already synced
		// 必须等待 APIService 注册控制器完成同步，确保所有 APIService 都已就位。
		select {
		case <-context.Done():
			return nil
		// Context cancelled, should abort/clean goroutines
		case <-apiServiceRegistrationControllerInitiated:
		}

		// Run discovery manager's worker to watch for new/removed/updated
		// APIServices to the discovery document can be updated at runtime
		// When discovery is ready, all APIServices will be present, with APIServices
		// that have not successfully synced discovery to be present but marked as Stale.
		// 启动 discovery manager 的 worker 来监听 APIService 的变化，并实时更新 discovery 文档。
		discoverySyncedCh := make(chan struct{})
		go s.discoveryAggregationController.Run(context.Done(), discoverySyncedCh)

		// 阻塞，直到 discovery controller 完成第一次的同步。
		select {
		case <-context.Done():
			return nil
		// Context cancelled, should abort/clean goroutines
		case <-discoverySyncedCh:
			// API services successfully sync
		}
		return nil
	})
	// 9e. (可选) 启动 StorageVersion 更新器
	// 如果启用了相关特性，会启动一个后台任务来定期更新内置资源的存储版本，用于支持平滑的存储格式升级。
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.StorageVersionAPI) &&
		utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerIdentity) {
		// Spawn a goroutine in aggregator apiserver to update storage version for
		// all built-in resources
		s.GenericAPIServer.AddPostStartHookOrDie(StorageVersionPostStartHookName, func(hookContext genericapiserver.PostStartHookContext) error {
			// Wait for apiserver-identity to exist first before updating storage
			// versions, to avoid storage version GC accidentally garbage-collecting
			// storage versions.
			kubeClient, err := kubernetes.NewForConfig(hookContext.LoopbackClientConfig)
			if err != nil {
				return err
			}
			if err := wait.PollImmediateUntil(100*time.Millisecond, func() (bool, error) {
				_, err := kubeClient.CoordinationV1().Leases(metav1.NamespaceSystem).Get(
					context.TODO(), s.GenericAPIServer.APIServerID, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				return true, nil
			}, hookContext.Done()); err != nil {
				return fmt.Errorf("failed to wait for apiserver-identity lease %s to be created: %v",
					s.GenericAPIServer.APIServerID, err)
			}
			// Technically an apiserver only needs to update storage version once during bootstrap.
			// Reconcile StorageVersion objects every 10 minutes will help in the case that the
			// StorageVersion objects get accidentally modified/deleted by a different agent. In that
			// case, the reconciliation ensures future storage migration still works. If nothing gets
			// changed, the reconciliation update is a noop and gets short-circuited by the apiserver,
			// therefore won't change the resource version and trigger storage migration.
			go wait.PollImmediateUntil(10*time.Minute, func() (bool, error) {
				// All apiservers (aggregator-apiserver, kube-apiserver, apiextensions-apiserver)
				// share the same generic apiserver config. The same StorageVersion manager is used
				// to register all built-in resources when the generic apiservers install APIs.
				s.GenericAPIServer.StorageVersionManager.UpdateStorageVersions(hookContext.LoopbackClientConfig, s.GenericAPIServer.APIServerID)
				return false, nil
			}, hookContext.Done())
			// Once the storage version updater finishes the first round of update,
			// the PostStartHook will return to unblock /healthz. The handler chain
			// won't block write requests anymore. Check every second since it's not
			// expensive.
			wait.PollImmediateUntil(1*time.Second, func() (bool, error) {
				return s.GenericAPIServer.StorageVersionManager.Completed(), nil
			}, hookContext.Done())
			return nil
		})
	}

	return s, nil
}

// PrepareRun prepares the aggregator to run, by setting up the OpenAPI spec &
// aggregated discovery document and calling the generic PrepareRun.
func (s *APIAggregator) PrepareRun() (preparedAPIAggregator, error) {
	// add post start hook before generic PrepareRun in order to be before /healthz installation
	print("ningminglong update")
	if s.openAPIConfig != nil {
		s.GenericAPIServer.AddPostStartHookOrDie("apiservice-openapi-controller", func(context genericapiserver.PostStartHookContext) error {
			go s.openAPIAggregationController.Run(context.Done())
			return nil
		})
	}

	if s.openAPIV3Config != nil {
		s.GenericAPIServer.AddPostStartHookOrDie("apiservice-openapiv3-controller", func(context genericapiserver.PostStartHookContext) error {
			go s.openAPIV3AggregationController.Run(context.Done())
			return nil
		})
	}

	prepared := s.GenericAPIServer.PrepareRun()

	// delay OpenAPI setup until the delegate had a chance to setup their OpenAPI handlers
	if s.openAPIConfig != nil {
		specDownloader := openapiaggregator.NewDownloader()
		openAPIAggregator, err := openapiaggregator.BuildAndRegisterAggregator(
			&specDownloader,
			s.GenericAPIServer.NextDelegate(),
			s.GenericAPIServer.Handler.GoRestfulContainer.RegisteredWebServices(),
			s.openAPIConfig,
			s.GenericAPIServer.Handler.NonGoRestfulMux)
		if err != nil {
			return preparedAPIAggregator{}, err
		}
		s.openAPIAggregationController = openapicontroller.NewAggregationController(&specDownloader, openAPIAggregator)
	}

	if s.openAPIV3Config != nil {
		specDownloaderV3 := openapiv3aggregator.NewDownloader()
		openAPIV3Aggregator, err := openapiv3aggregator.BuildAndRegisterAggregator(
			specDownloaderV3,
			s.GenericAPIServer.NextDelegate(),
			s.GenericAPIServer.Handler.GoRestfulContainer,
			s.openAPIV3Config,
			s.GenericAPIServer.Handler.NonGoRestfulMux)
		if err != nil {
			return preparedAPIAggregator{}, err
		}
		s.openAPIV3AggregationController = openapiv3controller.NewAggregationController(openAPIV3Aggregator)
	}

	return preparedAPIAggregator{APIAggregator: s, runnable: prepared}, nil
}

func (s preparedAPIAggregator) Run(ctx context.Context) error {
	return s.runnable.RunWithContext(ctx)
}

// AddAPIService adds an API service.  It is not thread-safe, so only call it on one thread at a time please.
// It's a slow moving API, so its ok to run the controller on a single thread
func (s *APIAggregator) AddAPIService(apiService *v1.APIService) error {
	// if the proxyHandler already exists, it needs to be updated. The aggregation bits do not
	// since they are wired against listers because they require multiple resources to respond
	if proxyHandler, exists := s.proxyHandlers[apiService.Name]; exists {
		proxyHandler.updateAPIService(apiService)
		if s.openAPIAggregationController != nil {
			s.openAPIAggregationController.UpdateAPIService(proxyHandler, apiService)
		}
		if s.openAPIV3AggregationController != nil {
			s.openAPIV3AggregationController.UpdateAPIService(proxyHandler, apiService)
		}
		// Forward calls to discovery manager to update discovery document
		if s.discoveryAggregationController != nil {
			handlerCopy := *proxyHandler
			handlerCopy.setServiceAvailable()
			s.discoveryAggregationController.AddAPIService(apiService, &handlerCopy)
		}
		return nil
	}

	proxyPath := "/apis/" + apiService.Spec.Group + "/" + apiService.Spec.Version
	// v1. is a special case for the legacy API.  It proxies to a wider set of endpoints.
	if apiService.Name == legacyAPIServiceName {
		proxyPath = "/api"
	}

	// register the proxy handler
	proxyHandler := &proxyHandler{
		localDelegate:              s.delegateHandler,
		proxyCurrentCertKeyContent: s.proxyCurrentCertKeyContent,
		proxyTransportDial:         s.proxyTransportDial,
		serviceResolver:            s.serviceResolver,
		rejectForwardingRedirects:  s.rejectForwardingRedirects,
		tracerProvider:             s.tracerProvider,
	}
	proxyHandler.updateAPIService(apiService)
	if s.openAPIAggregationController != nil {
		s.openAPIAggregationController.AddAPIService(proxyHandler, apiService)
	}
	if s.openAPIV3AggregationController != nil {
		s.openAPIV3AggregationController.AddAPIService(proxyHandler, apiService)
	}
	if s.discoveryAggregationController != nil {
		s.discoveryAggregationController.AddAPIService(apiService, proxyHandler)
	}

	s.proxyHandlers[apiService.Name] = proxyHandler
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle(proxyPath, proxyHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandlePrefix(proxyPath+"/", proxyHandler)

	// if we're dealing with the legacy group, we're done here
	if apiService.Name == legacyAPIServiceName {
		return nil
	}

	// if we've already registered the path with the handler, we don't want to do it again.
	versions, exist := s.handledGroupVersions[apiService.Spec.Group]
	if exist {
		versions.Insert(apiService.Spec.Version)
		return nil
	}

	// it's time to register the group aggregation endpoint
	groupPath := "/apis/" + apiService.Spec.Group
	groupDiscoveryHandler := &apiGroupHandler{
		codecs:    aggregatorscheme.Codecs,
		groupName: apiService.Spec.Group,
		lister:    s.lister,
		delegate:  s.delegateHandler,
	}
	// aggregation is protected
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle(groupPath, groupDiscoveryHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandle(groupPath+"/", groupDiscoveryHandler)
	s.handledGroupVersions[apiService.Spec.Group] = sets.New[string](apiService.Spec.Version)
	return nil
}

// RemoveAPIService removes the APIService from being handled.  It is not thread-safe, so only call it on one thread at a time please.
// It's a slow moving API, so it's ok to run the controller on a single thread.
func (s *APIAggregator) RemoveAPIService(apiServiceName string) {
	// Forward calls to discovery manager to update discovery document
	if s.discoveryAggregationController != nil {
		s.discoveryAggregationController.RemoveAPIService(apiServiceName)
	}

	version := v1helper.APIServiceNameToGroupVersion(apiServiceName)

	proxyPath := "/apis/" + version.Group + "/" + version.Version
	// v1. is a special case for the legacy API.  It proxies to a wider set of endpoints.
	if apiServiceName == legacyAPIServiceName {
		proxyPath = "/api"
	}
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(proxyPath)
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(proxyPath + "/")
	if s.openAPIAggregationController != nil {
		s.openAPIAggregationController.RemoveAPIService(apiServiceName)
	}
	if s.openAPIV3AggregationController != nil {
		s.openAPIV3AggregationController.RemoveAPIService(apiServiceName)
	}
	delete(s.proxyHandlers, apiServiceName)

	versions, exist := s.handledGroupVersions[version.Group]
	if !exist {
		return
	}
	versions.Delete(version.Version)
	if versions.Len() > 0 {
		return
	}
	delete(s.handledGroupVersions, version.Group)
	groupPath := "/apis/" + version.Group
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(groupPath)
	s.GenericAPIServer.Handler.NonGoRestfulMux.Unregister(groupPath + "/")
}

// DefaultAPIResourceConfigSource returns default configuration for an APIResource.
func DefaultAPIResourceConfigSource() *serverstorage.ResourceConfig {
	ret := serverstorage.NewResourceConfig()
	// NOTE: GroupVersions listed here will be enabled by default. Don't put alpha versions in the list.
	ret.EnableVersions(
		v1.SchemeGroupVersion,
		v1beta1.SchemeGroupVersion,
	)

	return ret
}
