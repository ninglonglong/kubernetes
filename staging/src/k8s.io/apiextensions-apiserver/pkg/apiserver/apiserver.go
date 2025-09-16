/*
Copyright 2017 The Kubernetes Authors.

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
	"net/http"
	"time"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	externalinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	"k8s.io/apiextensions-apiserver/pkg/controller/apiapproval"
	"k8s.io/apiextensions-apiserver/pkg/controller/establish"
	"k8s.io/apiextensions-apiserver/pkg/controller/finalizer"
	"k8s.io/apiextensions-apiserver/pkg/controller/nonstructuralschema"
	openapicontroller "k8s.io/apiextensions-apiserver/pkg/controller/openapi"
	openapiv3controller "k8s.io/apiextensions-apiserver/pkg/controller/openapiv3"
	"k8s.io/apiextensions-apiserver/pkg/controller/status"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresourcedefinition"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/klog/v2"
)

var (
	Scheme = runtime.NewScheme()
	Codecs = serializer.NewCodecFactory(Scheme)

	// if you modify this, make sure you update the crEncoder
	unversionedVersion = schema.GroupVersion{Group: "", Version: "v1"}
	unversionedTypes   = []runtime.Object{
		&metav1.Status{},
		&metav1.WatchEvent{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	}
)

func init() {
	install.Install(Scheme)

	// we need to add the options to empty v1
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Group: "", Version: "v1"})

	Scheme.AddUnversionedTypes(unversionedVersion, unversionedTypes...)
}

type ExtraConfig struct {
	CRDRESTOptionsGetter genericregistry.RESTOptionsGetter

	// MasterCount is used to detect whether cluster is HA, and if it is
	// the CRD Establishing will be hold by 5 seconds.
	MasterCount int

	// ServiceResolver is used in CR webhook converters to resolve webhook's service names
	ServiceResolver webhook.ServiceResolver
	// AuthResolverWrapper is used in CR webhook converters
	AuthResolverWrapper webhook.AuthenticationInfoResolverWrapper
}

type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

type CustomResourceDefinitions struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	// provided for easier embedding
	Informers externalinformers.SharedInformerFactory
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	c.GenericConfig.EnableDiscovery = false

	return CompletedConfig{&c}
}

// New returns a new instance of CustomResourceDefinitions from the given config.
// New 返回一个根据给定配置实例化的 CustomResourceDefinitions 服务器。
// `delegationTarget` 是它的下一环委托目标，当它无法处理请求时会将请求传递过去。
func (c completedConfig) New(delegationTarget genericapiserver.DelegationTarget) (*CustomResourceDefinitions, error) {
	// --- 1. 创建通用的 APIServer 实例 ---
	// 这是所有 APIServer 的基础。它包含了通用的 HTTP 处理、认证、授权等框架。
	// "apiextensions-apiserver" 是这个服务器的名字，用于日志和度量。
	genericServer, err := c.GenericConfig.New("apiextensions-apiserver", delegationTarget)
	if err != nil {
		return nil, err
	}

	// hasCRDInformerSyncedSignal is closed when the CRD informer this server uses has been fully synchronized.
	// It ensures that requests to potential custom resource endpoints while the server hasn't installed all known HTTP paths get a 503 error instead of a 404
	// --- 2. 创建 CRD Informer 同步信号 ---
	// `hasCRDInformerSyncedSignal` 是一个 channel，当 CRD 的 informer 完全同步后，它会被关闭。
	// `RegisterMuxAndDiscoveryCompleteSignal` 的作用是：在这个 channel 关闭之前，
	// 如果服务器收到一个它不认识的请求，它会返回 503 (Service Unavailable) 而不是 404 (Not Found)。
	// 这样做可以防止在服务器启动过程中，因为 CRD 对应的路由还没完全建立，而错误地给客户端返回 404。
	hasCRDInformerSyncedSignal := make(chan struct{})
	if err := genericServer.RegisterMuxAndDiscoveryCompleteSignal("CRDInformerHasNotSynced", hasCRDInformerSyncedSignal); err != nil {
		return nil, err
	}
	// 创建 CustomResourceDefinitions 服务器的主结构体。
	s := &CustomResourceDefinitions{
		GenericAPIServer: genericServer,
	}
	// --- 3. 安装 CRD 自身的 API ---
	// 这一步是将 `/apis/apiextensions.k8s.io/v1/customresourcedefinitions` 这个 API 注册到服务器中。
	apiResourceConfig := c.GenericConfig.MergedResourceConfig
	// 创建一个描述 `apiextensions.k8s.io` API 组的元信息对象。
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(apiextensions.GroupName, Scheme, metav1.ParameterCodec, Codecs)
	storage := map[string]rest.Storage{}
	// customresourcedefinitions
	// 检查 CRD 资源是否在配置中被启用了。
	if resource := "customresourcedefinitions"; apiResourceConfig.ResourceEnabled(v1.SchemeGroupVersion.WithResource(resource)) {
		// 创建 CRD 的 REST 存储后端。这个后端封装了对 etcd 中 CRD 对象的增删改查逻辑。
		customResourceDefinitionStorage, err := customresourcedefinition.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
		if err != nil {
			return nil, err
		}
		// 将 CRD 的主资源和 status 子资源的存储后端注册到 storage map 中。
		storage[resource] = customResourceDefinitionStorage
		storage[resource+"/status"] = customresourcedefinition.NewStatusREST(Scheme, customResourceDefinitionStorage)
	}
	if len(storage) > 0 {
		// 将 storage map 关联到 v1 版本。
		apiGroupInfo.VersionedResourcesStorageMap[v1.SchemeGroupVersion.Version] = storage
	}
	// `InstallAPIGroup` 是关键一步，它将 `apiGroupInfo` 中定义的所有 API 路径（如 GET, POST 等）
	// 安装到 `genericServer` 的 HTTP 处理器中。
	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}
	// --- 4. 创建 Informer 和 核心的 CRD 处理器 ---
	// Informer 是 Kubernetes 的标准组件，用于高效地监听和缓存资源对象。
	// 这里创建一个 CRD 客户端，然后用它来初始化一个 Informer 工厂。
	crdClient, err := clientset.NewForConfig(s.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		// it's really bad that this is leaking here, but until we can fix the test (which I'm pretty sure isn't even testing what it wants to test),
		// we need to be able to move forward
		return nil, fmt.Errorf("failed to create clientset: %v", err)
	}
	s.Informers = externalinformers.NewSharedInformerFactory(crdClient, 5*time.Minute)
	// 获取委托目标的处理器，如果委托目标不存在，则默认为 404 处理器。
	delegateHandler := delegationTarget.UnprotectedHandler()
	if delegateHandler == nil {
		delegateHandler = http.NotFoundHandler()
	}
	// `versionDiscoveryHandler` 和 `groupDiscoveryHandler` 是用于动态生成服务发现信息的处理器。
	// 当 CRD 被创建或删除时，它们会动态地更新 `/apis` 和 `/apis/custom.example.com` 的内容。
	versionDiscoveryHandler := &versionDiscoveryHandler{
		discovery: map[schema.GroupVersion]*discovery.APIVersionHandler{},
		delegate:  delegateHandler,
	}
	groupDiscoveryHandler := &groupDiscoveryHandler{
		discovery: map[string]*discovery.APIGroupHandler{},
		delegate:  delegateHandler,
	}
	// `establishingController` 负责处理 CRD 的 "Established" 状态，
	// 确保在 CRD 被标记为 Established 之前，其对应的 API 是不可用的。
	establishingController := establish.NewEstablishingController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	// `NewCustomResourceDefinitionHandler` 是整个 apiextensions-apiserver 的【核心】。
	// 它创建了一个巨大的 HTTP 处理器 `crdHandler`。
	// 这个处理器负责拦截所有对自定义资源（例如 `/apis/custom.example.com/v1/myresources`）的请求，
	// 然后动态地创建对应的 REST 存储后端，并处理这些请求。
	crdHandler, err := NewCustomResourceDefinitionHandler(
		versionDiscoveryHandler,
		groupDiscoveryHandler,
		s.Informers.Apiextensions().V1().CustomResourceDefinitions(),
		delegateHandler,
		c.ExtraConfig.CRDRESTOptionsGetter,
		c.GenericConfig.AdmissionControl,
		establishingController,
		c.ExtraConfig.ServiceResolver,
		c.ExtraConfig.AuthResolverWrapper,
		c.ExtraConfig.MasterCount,
		s.GenericAPIServer.Authorizer,
		c.GenericConfig.RequestTimeout,
		time.Duration(c.GenericConfig.MinRequestTimeout)*time.Second,
		apiGroupInfo.StaticOpenAPISpec,
		c.GenericConfig.MaxRequestBodyBytes,
	)
	if err != nil {
		return nil, err
	}
	// 将这个核心的 `crdHandler` 注册到服务器的 Mux 中，让它处理所有 `/apis` 和 `/apis/` 前缀的请求。
	// 注意：它的优先级低于之前通过 `InstallAPIGroup` 安装的具体 API 路径。
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/apis", crdHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.HandlePrefix("/apis/", crdHandler)
	s.GenericAPIServer.RegisterDestroyFunc(crdHandler.destroy)

	aggregatedDiscoveryManager := genericServer.AggregatedDiscoveryGroupManager
	if aggregatedDiscoveryManager != nil {
		aggregatedDiscoveryManager = aggregatedDiscoveryManager.WithSource(aggregated.CRDSource)
	}

	// --- 5. 创建并准备启动所有后台控制器 ---
	// 这些控制器 watch CRD 对象的变化，并执行各种维护任务。

	// `DiscoveryController`: 更新服务发现信息。
	discoveryController := NewDiscoveryController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), versionDiscoveryHandler, groupDiscoveryHandler, aggregatedDiscoveryManager)
	// `NamingConditionController`: 检查 CRD 的命名是否冲突，并更新其状态。
	namingController := status.NewNamingConditionController(klog.TODO() /* for contextual logging */, s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	// `NonStructuralSchemaController`: 检查 CRD 的 schema 是否是“结构化”的。
	nonStructuralSchemaController := nonstructuralschema.NewConditionController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	// `APIApprovalController`: 检查 CRD 是否符合 API 批准策略。
	apiApprovalController := apiapproval.NewKubernetesAPIApprovalPolicyConformantConditionController(s.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdClient.ApiextensionsV1())
	// `FinalizingController`: 处理 CRD 的删除逻辑，确保在删除 CRD 前，其定义的所有自定义资源实例都已被清理。
	finalizingController := finalizer.NewCRDFinalizer(
		s.Informers.Apiextensions().V1().CustomResourceDefinitions(),
		crdClient.ApiextensionsV1(),
		crdHandler,
	)
	// --- 6. 注册 PostStartHooks 来启动 Informer 和控制器 ---
	// `PostStartHook` 是在服务器开始监听端口之后，但被标记为“就绪”之前执行的钩子。
	// 启动 Informer 工厂。
	s.GenericAPIServer.AddPostStartHookOrDie("start-apiextensions-informers", func(hookContext genericapiserver.PostStartHookContext) error {
		s.Informers.Start(hookContext.Done())
		return nil
	})
	// 启动所有控制器。
	s.GenericAPIServer.AddPostStartHookOrDie("start-apiextensions-controllers", func(hookContext genericapiserver.PostStartHookContext) error {
		// OpenAPIVersionedService and StaticOpenAPISpec are populated in generic apiserver PrepareRun().
		// Together they serve the /openapi/v2 endpoint on a generic apiserver. A generic apiserver may
		// choose to not enable OpenAPI by having null openAPIConfig, and thus OpenAPIVersionedService
		// and StaticOpenAPISpec are both null. In that case we don't run the CRD OpenAPI controller.
		// 启动 OpenAPI 控制器，它们负责根据 CRD 动态生成 OpenAPI 规范。

		if s.GenericAPIServer.StaticOpenAPISpec != nil {
			if s.GenericAPIServer.OpenAPIVersionedService != nil {
				openapiController := openapicontroller.NewController(s.Informers.Apiextensions().V1().CustomResourceDefinitions())
				go openapiController.Run(s.GenericAPIServer.StaticOpenAPISpec, s.GenericAPIServer.OpenAPIVersionedService, hookContext.Done())
			}

			if s.GenericAPIServer.OpenAPIV3VersionedService != nil {
				openapiv3Controller := openapiv3controller.NewController(s.Informers.Apiextensions().V1().CustomResourceDefinitions())
				go openapiv3Controller.Run(s.GenericAPIServer.OpenAPIV3VersionedService, hookContext.Done())
			}
		}
		// 在后台 goroutine 中启动所有 CRD 状态维护控制器。
		go namingController.Run(hookContext.Done())
		go establishingController.Run(hookContext.Done())
		go nonStructuralSchemaController.Run(5, hookContext.Done())
		go apiApprovalController.Run(5, hookContext.Done())
		go finalizingController.Run(5, hookContext.Done())
		// 启动 discovery 控制器并等待它同步完成。
		discoverySyncedCh := make(chan struct{})
		go discoveryController.Run(hookContext.Done(), discoverySyncedCh)
		select {
		case <-hookContext.Done():
		case <-discoverySyncedCh:
		}

		return nil
	})
	// we don't want to report healthy until we can handle all CRDs that have already been registered.  Waiting for the informer
	// to sync makes sure that the lister will be valid before we begin.  There may still be races for CRDs added after startup,
	// but we won't go healthy until we can handle the ones already present.
	// 注册一个钩子，它会一直等待，直到 CRD informer 完全同步。
	// 同步完成后，它会关闭 `hasCRDInformerSyncedSignal` channel，
	// 这会告诉服务器的 HTTP 处理器可以停止返回 503，并开始正常处理请求。
	// 这确保了服务器在报告“健康”之前，已经加载了所有已存在的 CRD。
	s.GenericAPIServer.AddPostStartHookOrDie("crd-informer-synced", func(ctx genericapiserver.PostStartHookContext) error {
		return wait.PollUntilContextCancel(ctx.Context, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
			if s.Informers.Apiextensions().V1().CustomResourceDefinitions().Informer().HasSynced() {
				close(hasCRDInformerSyncedSignal)
				return true, nil
			}
			return false, nil
		})
	})

	return s, nil
}

func DefaultAPIResourceConfigSource() *serverstorage.ResourceConfig {
	ret := serverstorage.NewResourceConfig()
	// NOTE: GroupVersions listed here will be enabled by default. Don't put alpha versions in the list.
	ret.EnableVersions(
		v1beta1.SchemeGroupVersion,
		v1.SchemeGroupVersion,
	)

	return ret
}
