/*
Copyright 2023 The Kubernetes Authors.

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
	"crypto/tls"
	"fmt"
	"k8s.io/klog/v2"
	"net/http"
	"time"

	noopoteltrace "go.opentelemetry.io/otel/trace/noop"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericfeatures "k8s.io/apiserver/pkg/features"
	peerreconcilers "k8s.io/apiserver/pkg/reconcilers"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/server/filters"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/openapi"
	utilpeerproxy "k8s.io/apiserver/pkg/util/peerproxy"
	"k8s.io/client-go/dynamic"
	clientgoinformers "k8s.io/client-go/informers"
	clientgoclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/keyutil"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	openapicommon "k8s.io/kube-openapi/pkg/common"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	controlplaneadmission "k8s.io/kubernetes/pkg/controlplane/apiserver/admission"
	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/controlplane/controller/clusterauthenticationtrust"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubeapiserver"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
	rbacrest "k8s.io/kubernetes/pkg/registry/rbac/rest"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

// Config defines configuration for the master
type Config struct {
	Generic *genericapiserver.Config
	Extra
}

type Extra struct {
	ClusterAuthenticationInfo clusterauthenticationtrust.ClusterAuthenticationInfo

	APIResourceConfigSource serverstorage.APIResourceConfigSource
	StorageFactory          serverstorage.StorageFactory
	EventTTL                time.Duration

	EnableLogsSupport bool
	ProxyTransport    *http.Transport

	// PeerProxy, if not nil, sets proxy transport between kube-apiserver peers for requests
	// that can not be served locally
	PeerProxy utilpeerproxy.Interface
	// PeerEndpointReconcileInterval defines how often the endpoint leases are reconciled in etcd.
	PeerEndpointReconcileInterval time.Duration
	// PeerEndpointLeaseReconciler updates the peer endpoint leases
	PeerEndpointLeaseReconciler peerreconcilers.PeerEndpointLeaseReconciler
	// PeerAdvertiseAddress is the IP for this kube-apiserver which is used by peer apiservers to route a request
	// to this apiserver. This happens in cases where the peer is not able to serve the request due to
	// version skew. If unset, AdvertiseAddress/BindAddress will be used.
	PeerAdvertiseAddress peerreconcilers.PeerAdvertiseAddress

	ServiceAccountIssuer                serviceaccount.TokenGenerator
	ServiceAccountMaxExpiration         time.Duration
	ServiceAccountExtendedMaxExpiration time.Duration
	ExtendExpiration                    bool

	// ServiceAccountIssuerDiscovery
	ServiceAccountIssuerURL        string
	ServiceAccountJWKSURI          string
	ServiceAccountPublicKeysGetter serviceaccount.PublicKeysGetter

	SystemNamespaces []string

	VersionedInformers clientgoinformers.SharedInformerFactory

	// Coordinated Leader Election timers
	CoordinatedLeadershipLeaseDuration time.Duration
	CoordinatedLeadershipRenewDeadline time.Duration
	CoordinatedLeadershipRetryPeriod   time.Duration
}

// BuildGenericConfig takes the generic controlplane apiserver options and produces
// the genericapiserver.Config associated with it. The genericapiserver.Config is
// often shared between multiple delegated apiservers.
// BuildGenericConfig 函数的作用是: 获取已经“完成”的通用控制平面选项(s)，并生成一个
// 所有 apiserver 都能共享的 `genericapiserver.Config`。这个函数是构建 apiserver
// 所有基础功能的核心，包括但不限于：安全、认证、授权、etcd 存储、informer、OpenAPI 等。
func BuildGenericConfig(
	s options.CompletedOptions,
	schemes []*runtime.Scheme,
	resourceConfig *serverstorage.ResourceConfig,
	getOpenAPIDefinitions func(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition,
) (
	genericConfig *genericapiserver.Config,
	versionedInformers clientgoinformers.SharedInformerFactory,
	storageFactory *serverstorage.DefaultStorageFactory,
	lastErr error,
) {
	// 步骤 1: 创建一个全新的、默认的 genericapiserver.Config
	// - `legacyscheme.Codecs`: 提供了用于序列化/反序列化 API 对象的编解码器。
	klog.V(2).InfoS("Creating new generic apiserver config")
	genericConfig = genericapiserver.NewConfig(legacyscheme.Codecs)
	// 将 `feature-gates` 配置赋值给 `genericConfig`
	genericConfig.Flagz = s.Flagz
	// `MergedResourceConfig` 定义了哪些 API 资源组/版本是启用的。
	genericConfig.MergedResourceConfig = resourceConfig
	// 步骤 2: 应用最基础的服务运行选项
	// s.GenericServerRunOptions 包含了像 MaxRequestsInFlight, MinRequestTimeout 等服务器级别的参数。
	// .ApplyTo 方法将这些参数值应用到 `genericConfig` 中。
	klog.V(4).InfoS("Applying generic server run options")
	if lastErr = s.GenericServerRunOptions.ApplyTo(genericConfig); lastErr != nil {
		return
	}
	// 步骤 3: 应用安全服务端口配置 (HTTPS)
	// s.SecureServing 包含了 TLS 证书、端口号等配置。
	// .ApplyTo 方法会配置 `genericConfig.SecureServing` 和 `genericConfig.LoopbackClientConfig`。
	// `LoopbackClientConfig` 是 apiserver 用来“自己调用自己”的客户端配置，非常重要。
	klog.V(4).InfoS("Applying secure serving options")
	if lastErr = s.SecureServing.ApplyTo(&genericConfig.SecureServing, &genericConfig.LoopbackClientConfig); lastErr != nil {
		return
	}

	// Use protobufs for self-communication.
	// Since not every generic apiserver has to support protobufs, we
	// cannot default to it in generic apiserver and need to explicitly
	// set it in kube-apiserver.
	// 步骤 4: 优化内部通信 (Loopback Client)
	// 为了提高效率，apiserver 内部组件之间通信时，强制使用 Protobuf 格式，因为它比 JSON 更高效。
	// 同时禁用压缩，因为内部通信在高速局域网内，压缩的 CPU 开销得不偿失。
	klog.V(4).InfoS("Configuring loopback client for internal communication to use protobuf")
	genericConfig.LoopbackClientConfig.ContentConfig.ContentType = "application/vnd.kubernetes.protobuf"
	// Disable compression for self-communication, since we are going to be
	// on a fast local network
	genericConfig.LoopbackClientConfig.DisableCompression = true
	// 步骤 5: 创建 Informer Factory
	// Informer 是 Kubernetes 控制器模型的核心，它提供了对 API 对象的高效缓存和事件通知机制。
	// 这里创建一个 "versioned" informer factory，用于监听所有内置的 API 资源。
	klog.V(2).InfoS("Creating client and shared informer factory")
	kubeClientConfig := genericConfig.LoopbackClientConfig
	clientgoExternalClient, err := clientgoclientset.NewForConfig(kubeClientConfig)
	if err != nil {
		lastErr = fmt.Errorf("failed to create real external clientset: %w", err)
		return
	}
	// `trim` 函数是一个转换器，用于在对象存入 Informer 缓存之前，去掉 `managedFields` 字段，
	// 这样可以显著减小内存占用
	trim := func(obj interface{}) (interface{}, error) {
		if accessor, err := meta.Accessor(obj); err == nil && accessor.GetManagedFields() != nil {
			accessor.SetManagedFields(nil)
		}
		return obj, nil
	}
	versionedInformers = clientgoinformers.NewSharedInformerFactoryWithOptions(clientgoExternalClient, 10*time.Minute, clientgoinformers.WithTransform(trim))
	// 步骤 6: 应用各种特性门控和功能开关
	klog.V(4).InfoS("Applying feature gates and API enablement options")
	// Features: 可能是关于 Priority and Fairness (APF) 等高级特性的配置。
	if lastErr = s.Features.ApplyTo(genericConfig, clientgoExternalClient, versionedInformers); lastErr != nil {
		return
	}
	if lastErr = s.APIEnablement.ApplyTo(genericConfig, resourceConfig, legacyscheme.Scheme); lastErr != nil {
		return
	}
	if lastErr = s.EgressSelector.ApplyTo(genericConfig); lastErr != nil {
		return
	}
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerTracing) {
		if lastErr = s.Traces.ApplyTo(genericConfig.EgressSelector, genericConfig); lastErr != nil {
			return
		}
	}
	// wrap the definitions to revert any changes from disabled features
	// 步骤 7: 配置 OpenAPI (Swagger) 文档生成
	klog.V(2).InfoS("Configuring OpenAPI v2 and v3 definitions")
	// 包装 `getOpenAPIDefinitions` 函数，以确保禁用的特性不会出现在 OpenAPI 文档中。
	getOpenAPIDefinitions = openapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(getOpenAPIDefinitions)
	// `namer` 负责为 OpenAPI schema 中的定义生成唯一的名称。
	namer := openapinamer.NewDefinitionNamer(schemes...)
	// 配置 OpenAPI V2 (旧版 Swagger)
	genericConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(getOpenAPIDefinitions, namer)
	genericConfig.OpenAPIConfig.Info.Title = "Kubernetes"
	// 配置 OpenAPI V3
	genericConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(getOpenAPIDefinitions, namer)
	genericConfig.OpenAPIV3Config.Info.Title = "Kubernetes"
	// 步骤 8: 定义长连接请求
	// 告诉 apiserver 哪些请求是长连接（如 `watch`）或可能长时间运行（如 `exec`, `portforward`），
	// 以便在请求超时和优雅关闭等处理上对它们进行特殊照顾。
	genericConfig.LongRunningFunc = filters.BasicLongRunningRequestCheck(
		sets.NewString("watch", "proxy"),
		sets.NewString("attach", "exec", "proxy", "log", "portforward"),
	)

	// 步骤 9: 创建并配置存储工厂 (StorageFactory)
	// 这是连接到 etcd 的核心。StorageFactory 知道如何为每种资源创建正确的 etcd 存储后端。
	klog.V(2).InfoS("Creating and configuring storage factory for etcd")
	// 如果配置了 EgressSelector，将其注入到 etcd 的 transport 中，使得 etcd 客户端的连接也受网络策略控制。
	if genericConfig.EgressSelector != nil {
		s.Etcd.StorageConfig.Transport.EgressLookup = genericConfig.EgressSelector.Lookup
	}
	// 注入追踪器
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerTracing) {
		s.Etcd.StorageConfig.Transport.TracerProvider = genericConfig.TracerProvider
	} else {
		s.Etcd.StorageConfig.Transport.TracerProvider = noopoteltrace.NewTracerProvider()
	}

	storageFactoryConfig := kubeapiserver.NewStorageFactoryConfigEffectiveVersion(genericConfig.EffectiveVersion)
	storageFactoryConfig.APIResourceConfig = genericConfig.MergedResourceConfig
	storageFactoryConfig.DefaultResourceEncoding.SetEffectiveVersion(genericConfig.EffectiveVersion)
	// `.Complete(s.Etcd)` 将 etcd 的地址、证书等信息填入，`.New()` 创建最终的 factory 实例。
	storageFactory, lastErr = storageFactoryConfig.Complete(s.Etcd).New()
	if lastErr != nil {
		return
	}
	// storageFactory.StorageConfig is copied from etcdOptions.StorageConfig,
	// the StorageObjectCountTracker is still nil. Here we copy from genericConfig.
	// 将 `StorageObjectCountTracker` 从 genericConfig 拷贝到 storageFactory，用于监控 etcd 中的对象数量。
	storageFactory.StorageConfig.StorageObjectCountTracker = genericConfig.StorageObjectCountTracker
	// 将 etcd 的其他配置（如健康检查）应用到 genericConfig。
	if lastErr = s.Etcd.ApplyWithStorageFactoryTo(storageFactory, genericConfig); lastErr != nil {
		return
	}
	// 步骤 10: 配置认证 (Authentication)
	// s.Authentication 包含了所有认证方式的配置（如 OIDC, ServiceAccount Tokens, Webhook）。
	// .ApplyTo 会创建并组装一个认证器链。
	klog.V(2).InfoS("Applying authentication options")
	ctx := wait.ContextForChannel(genericConfig.DrainedNotify())

	// Authentication.ApplyTo requires already applied OpenAPIConfig and EgressSelector if present
	if lastErr = s.Authentication.ApplyTo(ctx, &genericConfig.Authentication, genericConfig.SecureServing, genericConfig.EgressSelector, genericConfig.OpenAPIConfig, genericConfig.OpenAPIV3Config, clientgoExternalClient, versionedInformers, genericConfig.APIServerID); lastErr != nil {
		return
	}
	// 步骤 11: 配置授权 (Authorization)
	// `BuildAuthorizer` 会根据用户的配置（如 `--authorization-mode=RBAC,Node`）创建并组合一个授权器链。
	klog.V(2).InfoS("Building authorizer")
	var enablesRBAC bool
	genericConfig.Authorization.Authorizer, genericConfig.RuleResolver, enablesRBAC, err = BuildAuthorizer(
		ctx,
		s,
		genericConfig.EgressSelector,
		genericConfig.APIServerID,
		versionedInformers,
	)
	if err != nil {
		lastErr = fmt.Errorf("invalid authorization config: %w", err)
		return
	}
	// 如果用户没有启用 RBAC，则禁用 RBAC 相关的 PostStartHook (用于创建默认的集群角色)。
	if s.Authorization != nil && !enablesRBAC {
		genericConfig.DisabledPostStartHooks.Insert(rbacrest.PostStartHookName)
	}
	// 步骤 12: 配置审计 (Audit)
	// s.Audit 包含了审计日志的策略、输出位置（文件、webhook）等配置。
	lastErr = s.Audit.ApplyTo(genericConfig)
	if lastErr != nil {
		return
	}
	// 步骤 13: 配置聚合发现管理器
	// 用于管理 `/apis` 路径下的 API 组发现。
	genericConfig.AggregatedDiscoveryGroupManager = aggregated.NewResourceManager("apis")
	// 步骤 14: 返回所有创建好的核心组件
	klog.InfoS("Generic configuration build completed successfully")
	return
}

// BuildAuthorizer constructs the authorizer. If authorization is not set in s, it returns nil, nil, false, nil
func BuildAuthorizer(ctx context.Context, s options.CompletedOptions, egressSelector *egressselector.EgressSelector, apiserverID string, versionedInformers clientgoinformers.SharedInformerFactory) (authorizer.Authorizer, authorizer.RuleResolver, bool, error) {
	authorizationConfig, err := s.Authorization.ToAuthorizationConfig(versionedInformers)
	if err != nil {
		return nil, nil, false, err
	}
	if authorizationConfig == nil {
		return nil, nil, false, nil
	}

	if egressSelector != nil {
		egressDialer, err := egressSelector.Lookup(egressselector.ControlPlane.AsNetworkContext())
		if err != nil {
			return nil, nil, false, err
		}
		authorizationConfig.CustomDial = egressDialer
	}

	enablesRBAC := false
	for _, a := range authorizationConfig.AuthorizationConfiguration.Authorizers {
		if string(a.Type) == modes.ModeRBAC {
			enablesRBAC = true
			break
		}
	}

	authorizer, ruleResolver, err := authorizationConfig.New(ctx, apiserverID)

	return authorizer, ruleResolver, enablesRBAC, err
}

// CreateConfig takes the generic controlplane apiserver options and
// creates a config for the generic Kube APIs out of it.
func CreateConfig(
	opts options.CompletedOptions,
	genericConfig *genericapiserver.Config,
	versionedInformers clientgoinformers.SharedInformerFactory,
	storageFactory *serverstorage.DefaultStorageFactory,
	serviceResolver aggregatorapiserver.ServiceResolver,
	additionalInitializers []admission.PluginInitializer,
) (
	*Config,
	[]admission.PluginInitializer,
	error,
) {
	proxyTransport := CreateProxyTransport()

	opts.Metrics.Apply()
	serviceaccount.RegisterMetrics()

	config := &Config{
		Generic: genericConfig,
		Extra: Extra{
			APIResourceConfigSource: storageFactory.APIResourceConfigSource,
			StorageFactory:          storageFactory,
			EventTTL:                opts.EventTTL,
			EnableLogsSupport:       opts.EnableLogsHandler,
			ProxyTransport:          proxyTransport,
			SystemNamespaces:        opts.SystemNamespaces,

			ServiceAccountIssuer:                opts.ServiceAccountIssuer,
			ServiceAccountMaxExpiration:         opts.ServiceAccountTokenMaxExpiration,
			ServiceAccountExtendedMaxExpiration: opts.Authentication.ServiceAccounts.MaxExtendedExpiration,
			ExtendExpiration:                    opts.Authentication.ServiceAccounts.ExtendExpiration,

			VersionedInformers: versionedInformers,

			CoordinatedLeadershipLeaseDuration: opts.CoordinatedLeadershipLeaseDuration,
			CoordinatedLeadershipRenewDeadline: opts.CoordinatedLeadershipRenewDeadline,
			CoordinatedLeadershipRetryPeriod:   opts.CoordinatedLeadershipRetryPeriod,
		},
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.UnknownVersionInteroperabilityProxy) {
		var err error
		config.PeerEndpointLeaseReconciler, err = CreatePeerEndpointLeaseReconciler(*genericConfig, storageFactory)
		if err != nil {
			return nil, nil, err
		}
		if opts.PeerCAFile != "" {
			leaseInformer := versionedInformers.Coordination().V1().Leases()
			config.PeerProxy, err = BuildPeerProxy(
				leaseInformer,
				genericConfig.LoopbackClientConfig,
				opts.ProxyClientCertFile,
				opts.ProxyClientKeyFile, opts.PeerCAFile,
				opts.PeerAdvertiseAddress,
				genericConfig.APIServerID,
				config.Extra.PeerEndpointLeaseReconciler,
				config.Generic.Serializer)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	clientCAProvider, err := opts.Authentication.ClientCert.GetClientCAContentProvider()
	if err != nil {
		return nil, nil, err
	}
	config.ClusterAuthenticationInfo.ClientCA = clientCAProvider

	requestHeaderConfig, err := opts.Authentication.RequestHeader.ToAuthenticationRequestHeaderConfig()
	if err != nil {
		return nil, nil, err
	}
	if requestHeaderConfig != nil {
		config.ClusterAuthenticationInfo.RequestHeaderCA = requestHeaderConfig.CAContentProvider
		config.ClusterAuthenticationInfo.RequestHeaderAllowedNames = requestHeaderConfig.AllowedClientNames
		config.ClusterAuthenticationInfo.RequestHeaderExtraHeaderPrefixes = requestHeaderConfig.ExtraHeaderPrefixes
		config.ClusterAuthenticationInfo.RequestHeaderGroupHeaders = requestHeaderConfig.GroupHeaders
		config.ClusterAuthenticationInfo.RequestHeaderUsernameHeaders = requestHeaderConfig.UsernameHeaders
		config.ClusterAuthenticationInfo.RequestHeaderUIDHeaders = requestHeaderConfig.UIDHeaders
	}

	// setup admission
	genericAdmissionConfig := controlplaneadmission.Config{
		ExternalInformers:    versionedInformers,
		LoopbackClientConfig: genericConfig.LoopbackClientConfig,
	}
	genericInitializers, err := genericAdmissionConfig.New(proxyTransport, genericConfig.EgressSelector, serviceResolver, genericConfig.TracerProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create admission plugin initializer: %w", err)
	}
	clientgoExternalClient, err := clientgoclientset.NewForConfig(genericConfig.LoopbackClientConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create real client-go external client: %w", err)
	}
	dynamicExternalClient, err := dynamic.NewForConfig(genericConfig.LoopbackClientConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create real dynamic external client: %w", err)
	}
	err = opts.Admission.ApplyTo(
		genericConfig,
		versionedInformers,
		clientgoExternalClient,
		dynamicExternalClient,
		utilfeature.DefaultFeatureGate,
		append(genericInitializers, additionalInitializers...)...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to apply admission: %w", err)
	}

	if len(opts.Authentication.ServiceAccounts.KeyFiles) > 0 {
		// Load and set the public keys.
		var pubKeys []any
		for _, f := range opts.Authentication.ServiceAccounts.KeyFiles {
			keys, err := keyutil.PublicKeysFromFile(f)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse key file %q: %w", f, err)
			}
			pubKeys = append(pubKeys, keys...)
		}
		keysGetter, err := serviceaccount.StaticPublicKeysGetter(pubKeys)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to set up public service account keys: %w", err)
		}
		config.ServiceAccountPublicKeysGetter = keysGetter
	} else if opts.Authentication.ServiceAccounts.ExternalPublicKeysGetter != nil {
		config.ServiceAccountPublicKeysGetter = opts.Authentication.ServiceAccounts.ExternalPublicKeysGetter
	}

	config.ServiceAccountIssuerURL = opts.Authentication.ServiceAccounts.Issuers[0]
	config.ServiceAccountJWKSURI = opts.Authentication.ServiceAccounts.JWKSURI

	return config, genericInitializers, nil
}

// CreateProxyTransport creates the dialer infrastructure to connect to the nodes.
func CreateProxyTransport() *http.Transport {
	var proxyDialerFn utilnet.DialFunc
	// Proxying to pods and services is IP-based... don't expect to be able to verify the hostname
	proxyTLSClientConfig := &tls.Config{InsecureSkipVerify: true}
	proxyTransport := utilnet.SetTransportDefaults(&http.Transport{
		DialContext:     proxyDialerFn,
		TLSClientConfig: proxyTLSClientConfig,
	})
	return proxyTransport
}
