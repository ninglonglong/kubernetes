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

package server

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"net"
	"net/http"
	"os"
	goruntime "runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/cryptobyte"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilwaitgroup "k8s.io/apimachinery/pkg/util/waitgroup"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	authenticatorunion "k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/apiserver/pkg/endpoints/filterlatency"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apiopenapi "k8s.io/apiserver/pkg/endpoints/openapi"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/egressselector"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/apiserver/pkg/server/routine"
	serverstore "k8s.io/apiserver/pkg/server/storage"
	storagevalue "k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storageversion"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/informers"
	restclient "k8s.io/client-go/rest"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/features"
	"k8s.io/component-base/metrics/prometheus/slis"
	"k8s.io/component-base/tracing"
	"k8s.io/component-base/zpages/flagz"
	"k8s.io/klog/v2"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/utils/clock"
	utilsnet "k8s.io/utils/net"

	// install apis
	_ "k8s.io/apiserver/pkg/apis/apiserver/install"
)

// hostnameFunc is a function to set the hostnameFunc of this apiserver.
// To be used for testing purpose only, to simulate scenarios where multiple apiservers
// exist. In such cases we want to ensure unique apiserver IDs which are a hash of hostnameFunc.
var (
	hostnameFunc = os.Hostname
)

const (
	// DefaultLegacyAPIPrefix is where the legacy APIs will be located.
	DefaultLegacyAPIPrefix = "/api"

	// APIGroupPrefix is where non-legacy API group will be located.
	APIGroupPrefix = "/apis"
)

// Config is a structure used to configure a GenericAPIServer.
// Its members are sorted roughly in order of importance for composers.
type Config struct {
	// SecureServing is required to serve https
	SecureServing *SecureServingInfo

	// Authentication is the configuration for authentication
	Authentication AuthenticationInfo

	// Authorization is the configuration for authorization
	Authorization AuthorizationInfo

	// LoopbackClientConfig is a config for a privileged loopback connection to the API server
	// This is required for proper functioning of the PostStartHooks on a GenericAPIServer
	// TODO: move into SecureServing(WithLoopback) as soon as insecure serving is gone
	LoopbackClientConfig *restclient.Config

	// EgressSelector provides a lookup mechanism for dialing outbound connections.
	// It does so based on a EgressSelectorConfiguration which was read at startup.
	EgressSelector *egressselector.EgressSelector

	// RuleResolver is required to get the list of rules that apply to a given user
	// in a given namespace
	RuleResolver authorizer.RuleResolver
	// AdmissionControl performs deep inspection of a given request (including content)
	// to set values and determine whether its allowed
	AdmissionControl      admission.Interface
	CorsAllowedOriginList []string
	HSTSDirectives        []string
	// FlowControl, if not nil, gives priority and fairness to request handling
	FlowControl utilflowcontrol.Interface

	EnableIndex     bool
	EnableProfiling bool
	DebugSocketPath string
	EnableDiscovery bool

	// Requires generic profiling enabled
	EnableContentionProfiling bool
	EnableMetrics             bool

	DisabledPostStartHooks sets.String
	// done values in this values for this map are ignored.
	PostStartHooks map[string]PostStartHookConfigEntry

	// EffectiveVersion determines which apis and features are available
	// based on when the api/feature lifecyle.
	EffectiveVersion basecompatibility.EffectiveVersion
	// EmulationForwardCompatible is an option to implicitly enable all APIs which are introduced after the emulation version and
	// have higher priority than APIs of the same group resource enabled at the emulation version.
	// If true, all APIs that have higher priority than the APIs(beta+) of the same group resource enabled at the emulation version will be installed.
	// This is needed when a controller implementation migrates to newer API versions, for the binary version, and also uses the newer API versions even when emulation version is set.
	// Not applicable to alpha APIs.
	EmulationForwardCompatible bool
	// RuntimeConfigEmulationForwardCompatible is an option to explicitly enable specific APIs introduced after the emulation version through the runtime-config.
	// If true, APIs identified by group/version that are enabled in the --runtime-config flag will be installed even if it is introduced after the emulation version. --runtime-config flag values that identify multiple APIs, such as api/all,api/ga,api/beta, are not influenced by this flag and will only enable APIs available at the current emulation version.
	// If false, error would be thrown if any GroupVersion or GroupVersionResource explicitly enabled in the --runtime-config flag is introduced after the emulation version.
	RuntimeConfigEmulationForwardCompatible bool
	// FeatureGate is a way to plumb feature gate through if you have them.
	FeatureGate featuregate.FeatureGate
	// AuditBackend is where audit events are sent to.
	AuditBackend audit.Backend
	// AuditPolicyRuleEvaluator makes the decision of whether and how to audit log a request.
	AuditPolicyRuleEvaluator audit.PolicyRuleEvaluator
	// ExternalAddress is the host name to use for external (public internet) facing URLs (e.g. Swagger)
	// Will default to a value based on secure serving info and available ipv4 IPs.
	ExternalAddress string

	// TracerProvider can provide a tracer, which records spans for distributed tracing.
	TracerProvider tracing.TracerProvider

	//===========================================================================
	// Fields you probably don't care about changing
	//===========================================================================

	// BuildHandlerChainFunc allows you to build custom handler chains by decorating the apiHandler.
	BuildHandlerChainFunc func(apiHandler http.Handler, c *Config) (secure http.Handler)
	// NonLongRunningRequestWaitGroup allows you to wait for all chain
	// handlers associated with non long-running requests
	// to complete while the server is shuting down.
	NonLongRunningRequestWaitGroup *utilwaitgroup.SafeWaitGroup
	// WatchRequestWaitGroup allows us to wait for all chain
	// handlers associated with active watch requests to
	// complete while the server is shuting down.
	WatchRequestWaitGroup *utilwaitgroup.RateLimitedSafeWaitGroup
	// DiscoveryAddresses is used to build the IPs pass to discovery. If nil, the ExternalAddress is
	// always reported
	DiscoveryAddresses discovery.Addresses
	// The default set of healthz checks. There might be more added via AddHealthChecks dynamically.
	HealthzChecks []healthz.HealthChecker
	// The default set of livez checks. There might be more added via AddHealthChecks dynamically.
	LivezChecks []healthz.HealthChecker
	// The default set of readyz-only checks. There might be more added via AddReadyzChecks dynamically.
	ReadyzChecks []healthz.HealthChecker
	Flagz        flagz.Reader
	// LegacyAPIGroupPrefixes is used to set up URL parsing for authorization and for validating requests
	// to InstallLegacyAPIGroup. New API servers don't generally have legacy groups at all.
	LegacyAPIGroupPrefixes sets.String
	// RequestInfoResolver is used to assign attributes (used by admission and authorization) based on a request URL.
	// Use-cases that are like kubelets may need to customize this.
	RequestInfoResolver apirequest.RequestInfoResolver
	// Serializer is required and provides the interface for serializing and converting objects to and from the wire
	// The default (api.Codecs) usually works fine.
	Serializer runtime.NegotiatedSerializer
	// OpenAPIConfig will be used in generating OpenAPI spec. This is nil by default. Use DefaultOpenAPIConfig for "working" defaults.
	OpenAPIConfig *openapicommon.Config
	// OpenAPIV3Config will be used in generating OpenAPI V3 spec. This is nil by default. Use DefaultOpenAPIV3Config for "working" defaults.
	OpenAPIV3Config *openapicommon.OpenAPIV3Config
	// SkipOpenAPIInstallation avoids installing the OpenAPI handler if set to true.
	SkipOpenAPIInstallation bool

	// ResourceTransformers are used to transform resources from and to etcd, e.g. encryption.
	ResourceTransformers storagevalue.ResourceTransformers
	// RESTOptionsGetter is used to construct RESTStorage types via the generic registry.
	RESTOptionsGetter genericregistry.RESTOptionsGetter

	// If specified, all requests except those which match the LongRunningFunc predicate will timeout
	// after this duration.
	RequestTimeout time.Duration
	// If specified, long running requests such as watch will be allocated a random timeout between this value, and
	// twice this value.  Note that it is up to the request handlers to ignore or honor this timeout. In seconds.
	MinRequestTimeout int

	// StorageInitializationTimeout defines the maximum amount of time to wait for storage initialization
	// before declaring apiserver ready.
	StorageInitializationTimeout time.Duration

	// This represents the maximum amount of time it should take for apiserver to complete its startup
	// sequence and become healthy. From apiserver's start time to when this amount of time has
	// elapsed, /livez will assume that unfinished post-start hooks will complete successfully and
	// therefore return true.
	LivezGracePeriod time.Duration
	// ShutdownDelayDuration allows to block shutdown for some time, e.g. until endpoints pointing to this API server
	// have converged on all node. During this time, the API server keeps serving, /healthz will return 200,
	// but /readyz will return failure.
	ShutdownDelayDuration time.Duration

	// The limit on the total size increase all "copy" operations in a json
	// patch may cause.
	// This affects all places that applies json patch in the binary.
	JSONPatchMaxCopyBytes int64
	// The limit on the request size that would be accepted and decoded in a write request
	// 0 means no limit.
	MaxRequestBodyBytes int64
	// MaxRequestsInFlight is the maximum number of parallel non-long-running requests. Every further
	// request has to wait. Applies only to non-mutating requests.
	MaxRequestsInFlight int
	// MaxMutatingRequestsInFlight is the maximum number of parallel mutating requests. Every further
	// request has to wait.
	MaxMutatingRequestsInFlight int
	// Predicate which is true for paths of long-running http requests
	LongRunningFunc apirequest.LongRunningRequestCheck

	// GoawayChance is the probability that send a GOAWAY to HTTP/2 clients. When client received
	// GOAWAY, the in-flight requests will not be affected and new requests will use
	// a new TCP connection to triggering re-balancing to another server behind the load balance.
	// Default to 0, means never send GOAWAY. Max is 0.02 to prevent break the apiserver.
	GoawayChance float64

	// MergedResourceConfig indicates which groupVersion enabled and its resources enabled/disabled.
	// This is composed of genericapiserver defaultAPIResourceConfig and those parsed from flags.
	// If not specify any in flags, then genericapiserver will only enable defaultAPIResourceConfig.
	MergedResourceConfig *serverstore.ResourceConfig

	// lifecycleSignals provides access to the various signals
	// that happen during lifecycle of the apiserver.
	// it's intentionally marked private as it should never be overridden.
	lifecycleSignals lifecycleSignals

	// StorageObjectCountTracker is used to keep track of the total number of objects
	// in the storage per resource, so we can estimate width of incoming requests.
	StorageObjectCountTracker flowcontrolrequest.StorageObjectCountTracker

	// ShutdownSendRetryAfter dictates when to initiate shutdown of the HTTP
	// Server during the graceful termination of the apiserver. If true, we wait
	// for non longrunning requests in flight to be drained and then initiate a
	// shutdown of the HTTP Server. If false, we initiate a shutdown of the HTTP
	// Server as soon as ShutdownDelayDuration has elapsed.
	// If enabled, after ShutdownDelayDuration elapses, any incoming request is
	// rejected with a 429 status code and a 'Retry-After' response.
	ShutdownSendRetryAfter bool

	//===========================================================================
	// values below here are targets for removal
	//===========================================================================

	// PublicAddress is the IP address where members of the cluster (kubelet,
	// kube-proxy, services, etc.) can reach the GenericAPIServer.
	// If nil or 0.0.0.0, the host's default interface will be used.
	PublicAddress net.IP

	// EquivalentResourceRegistry provides information about resources equivalent to a given resource,
	// and the kind associated with a given resource. As resources are installed, they are registered here.
	EquivalentResourceRegistry runtime.EquivalentResourceRegistry

	// APIServerID is the ID of this API server
	APIServerID string

	// StorageVersionManager holds the storage versions of the API resources installed by this server.
	StorageVersionManager storageversion.Manager

	// AggregatedDiscoveryGroupManager serves /apis in an aggregated form.
	AggregatedDiscoveryGroupManager discoveryendpoint.ResourceManager

	// ShutdownWatchTerminationGracePeriod, if set to a positive value,
	// is the maximum duration the apiserver will wait for all active
	// watch request(s) to drain.
	// Once this grace period elapses, the apiserver will no longer
	// wait for any active watch request(s) in flight to drain, it will
	// proceed to the next step in the graceful server shutdown process.
	// If set to a positive value, the apiserver will keep track of the
	// number of active watch request(s) in flight and during shutdown
	// it will wait, at most, for the specified duration and allow these
	// active watch requests to drain with some rate limiting in effect.
	// The default is zero, which implies the apiserver will not keep
	// track of active watch request(s) in flight and will not wait
	// for them to drain, this maintains backward compatibility.
	// This grace period is orthogonal to other grace periods, and
	// it is not overridden by any other grace period.
	ShutdownWatchTerminationGracePeriod time.Duration
}

type RecommendedConfig struct {
	Config

	// SharedInformerFactory provides shared informers for Kubernetes resources. This value is set by
	// RecommendedOptions.CoreAPI.ApplyTo called by RecommendedOptions.ApplyTo. It uses an in-cluster client config
	// by default, or the kubeconfig given with kubeconfig command line flag.
	SharedInformerFactory informers.SharedInformerFactory

	// ClientConfig holds the kubernetes client configuration.
	// This value is set by RecommendedOptions.CoreAPI.ApplyTo called by RecommendedOptions.ApplyTo.
	// By default in-cluster client config is used.
	ClientConfig *restclient.Config
}

type SecureServingInfo struct {
	// Listener is the secure server network listener.
	Listener net.Listener

	// Cert is the main server cert which is used if SNI does not match. Cert must be non-nil and is
	// allowed to be in SNICerts.
	Cert dynamiccertificates.CertKeyContentProvider

	// SNICerts are the TLS certificates used for SNI.
	SNICerts []dynamiccertificates.SNICertKeyContentProvider

	// ClientCA is the certificate bundle for all the signers that you'll recognize for incoming client certificates
	ClientCA dynamiccertificates.CAContentProvider

	// MinTLSVersion optionally overrides the minimum TLS version supported.
	// Values are from tls package constants (https://golang.org/pkg/crypto/tls/#pkg-constants).
	MinTLSVersion uint16

	// CipherSuites optionally overrides the list of allowed cipher suites for the server.
	// Values are from tls package constants (https://golang.org/pkg/crypto/tls/#pkg-constants).
	CipherSuites []uint16

	// HTTP2MaxStreamsPerConnection is the limit that the api server imposes on each client.
	// A value of zero means to use the default provided by golang's HTTP/2 support.
	HTTP2MaxStreamsPerConnection int

	// DisableHTTP2 indicates that http2 should not be enabled.
	DisableHTTP2 bool
}

type AuthenticationInfo struct {
	// APIAudiences is a list of identifier that the API identifies as. This is
	// used by some authenticators to validate audience bound credentials.
	APIAudiences authenticator.Audiences
	// Authenticator determines which subject is making the request
	Authenticator authenticator.Request

	RequestHeaderConfig *authenticatorfactory.RequestHeaderConfig
}

type AuthorizationInfo struct {
	// Authorizer determines whether the subject is allowed to make the request based only
	// on the RequestURI
	Authorizer authorizer.Authorizer
}

func init() {
	utilruntime.Must(features.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// NewConfig returns a Config struct with the default values
// NewConfig 创建一个通用的 API 服务器配置对象（Config），并为其填充一系列社区推荐的默认值。
// 这个 Config 对象是构建一个 API 服务器实例的蓝图。
func NewConfig(codecs serializer.CodecFactory) *Config {
	// --- 1. 健康检查默认配置 ---
	// 定义一组默认的健康检查器。PingHealthz 检查服务器是否能响应，LogHealthz 检查日志系统是否正常。
	defaultHealthChecks := []healthz.HealthChecker{healthz.PingHealthz, healthz.LogHealthz}
	// --- 2. APIServer Identity (身份标识) ---
	var id string
	// 检查 APIServerIdentity 这个特性门控是否被启用。
	// 这个特性为每个 apiserver 实例提供一个唯一的、稳定的 ID，用于 Lease 对象的争抢等场景。
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerIdentity) {
		hostname, err := hostnameFunc() // 获取当前机器的主机名
		if err != nil {
			klog.Fatalf("error getting hostname for apiserver identity: %v", err)
		}

		// Since the hash needs to be unique across each kube-apiserver and aggregated apiservers,
		// the hash used for the identity should include both the hostname and the identity value.
		// TODO: receive the identity value as a parameter once the apiserver identity lease controller
		// post start hook is moved to generic apiserver.
		// 为了确保在所有 kube-apiserver 和聚合 apiserver 之间哈希值是唯一的，
		// 哈希的原始数据应该包含主机名和一个固定的身份值（这里是 "kube-apiserver"）。
		b := cryptobyte.NewBuilder(nil)
		// 使用 TLV (Type-Length-Value) 格式来构建数据，确保字段之间不会混淆。
		b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes([]byte(hostname))
		})
		b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
			b.AddBytes([]byte("kube-apiserver"))
		})
		hashData, err := b.Bytes()
		if err != nil {
			klog.Fatalf("error building hash data for apiserver identity: %v", err)
		}

		hash := sha256.Sum256(hashData)
		id = "apiserver-" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hash[:16]))
	}
	// --- 3. 生命周期信号 ---
	// 创建用于协调服务器启动、关闭和健康状态的信号通道。
	lifecycleSignals := newLifecycleSignals()

	return &Config{
		// 序列化与反序列化相关：编解码器工厂，用于处理不同版本的 API 对象。
		Serializer: codecs,
		// HTTP 请求处理链：用于构建包含认证、授权、准入控制等一系列过滤器的 HTTP Handler 链。
		BuildHandlerChainFunc: DefaultBuildHandlerChain,
		// 等待组：用于在优雅关闭时，等待所有正在处理的“非长连接”请求完成。
		NonLongRunningRequestWaitGroup: new(utilwaitgroup.SafeWaitGroup),
		// 等待组：专门用于等待 watch 请求的完成，并带有速率限制。
		WatchRequestWaitGroup: &utilwaitgroup.RateLimitedSafeWaitGroup{},
		// 旧版 API 组前缀：如 "/api"，用于兼容旧的 API 访问路径。
		LegacyAPIGroupPrefixes: sets.NewString(DefaultLegacyAPIPrefix),
		// 禁用的启动后钩子：一个集合，用于禁用某些默认的 PostStartHook。
		DisabledPostStartHooks: sets.NewString(),
		// 启动后钩子：一个 map，用于注册在服务器主循环启动后需要执行的任务（如启动各种控制器）。
		PostStartHooks: map[string]PostStartHookConfigEntry{},
		// 健康检查：/healthz 端点的检查器列表，默认包含 ping 和 log 检查。
		HealthzChecks: append([]healthz.HealthChecker{}, defaultHealthChecks...),
		// 就绪检查：/readyz 端点的检查器列表，默认与 healthz 相同。
		ReadyzChecks: append([]healthz.HealthChecker{}, defaultHealthChecks...),
		// 存活检查：/livez 端点的检查器列表，默认与 healthz 相同。
		LivezChecks: append([]healthz.HealthChecker{}, defaultHealthChecks...),
		// 是否启用 API 索引页 ("/apis")。
		EnableIndex: true,
		// 是否启用 API 发现页（如 "/apis/apps/v1"）。
		EnableDiscovery: true,
		// 是否启用性能分析端点 ("/debug/pprof")。
		EnableProfiling: true,
		// 调试用的 Unix Socket 路径，如果设置了，可以通过它进行调试。
		DebugSocketPath: "",
		// 是否启用指标暴露端点 ("/metrics")。
		EnableMetrics:                true,
		MaxRequestsInFlight:          400,
		MaxMutatingRequestsInFlight:  200,
		RequestTimeout:               time.Duration(60) * time.Second,
		MinRequestTimeout:            1800,
		StorageInitializationTimeout: time.Minute,
		LivezGracePeriod:             time.Duration(0),
		ShutdownDelayDuration:        time.Duration(0),
		// 1.5MB is the default client request size in bytes
		// the etcd server should accept. See
		// https://github.com/etcd-io/etcd/blob/release-3.4/embed/config.go#L56.
		// A request body might be encoded in json, and is converted to
		// proto when persisted in etcd, so we allow 2x as the largest size
		// increase the "copy" operations in a json patch may cause.
		JSONPatchMaxCopyBytes: int64(3 * 1024 * 1024),
		// 1.5MB is the recommended client request size in byte
		// the etcd server should accept. See
		// https://github.com/etcd-io/etcd/blob/release-3.4/embed/config.go#L56.
		// A request body might be encoded in json, and is converted to
		// proto when persisted in etcd, so we allow 2x as the largest request
		// body size to be accepted and decoded in a write request.
		// If this constant is changed, DefaultMaxRequestSizeBytes in k8s.io/apiserver/pkg/cel/limits.go
		// should be changed to reflect the new value, if the two haven't
		// been wired together already somehow.
		MaxRequestBodyBytes: int64(3 * 1024 * 1024),

		// Default to treating watch as a long-running operation
		// Generic API servers have no inherent long-running subresources
		// 长连接请求判断函数：定义哪些请求被认为是“长连接”（如 "watch"），从而走不同的处理逻辑。
		LongRunningFunc: genericfilters.BasicLongRunningRequestCheck(sets.NewString("watch"), sets.NewString()),
		// 内部生命周期信号，用于协调关闭流程。
		lifecycleSignals: lifecycleSignals,
		// 存储对象计数器：用于 APF (API Priority and Fairness) 流控，追踪存储层中的对象数量。
		StorageObjectCountTracker: flowcontrolrequest.NewStorageObjectCountTracker(),
		// 在优雅关闭期间，允许 watch 请求在被终止前持续的最长时间。
		ShutdownWatchTerminationGracePeriod: time.Duration(0),
		// API 服务器的唯一标识符，如果特性门控开启，这里会被赋值。
		APIServerID: id,
		// 存储版本管理器：用于处理 API 对象的存储版本迁移。
		StorageVersionManager: storageversion.NewDefaultManager(),
		// 分布式追踪提供者：默认是一个空操作的 NoopTracerProvider，即禁用追踪。
		TracerProvider: tracing.NewNoopTracerProvider(),
	}
}

// NewRecommendedConfig returns a RecommendedConfig struct with the default values
func NewRecommendedConfig(codecs serializer.CodecFactory) *RecommendedConfig {
	return &RecommendedConfig{
		Config: *NewConfig(codecs),
	}
}

// DefaultOpenAPIConfig provides the default OpenAPIConfig used to build the OpenAPI V2 spec
func DefaultOpenAPIConfig(getDefinitions openapicommon.GetOpenAPIDefinitions, defNamer *apiopenapi.DefinitionNamer) *openapicommon.Config {
	return &openapicommon.Config{
		ProtocolList:   []string{"https"},
		IgnorePrefixes: []string{},
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title: "Generic API Server",
			},
		},
		DefaultResponse: &spec.Response{
			ResponseProps: spec.ResponseProps{
				Description: "Default Response.",
			},
		},
		GetOperationIDAndTags: apiopenapi.GetOperationIDAndTags,
		GetDefinitionName:     defNamer.GetDefinitionName,
		GetDefinitions:        getDefinitions,
	}
}

// DefaultOpenAPIV3Config provides the default OpenAPIV3Config used to build the OpenAPI V3 spec
func DefaultOpenAPIV3Config(getDefinitions openapicommon.GetOpenAPIDefinitions, defNamer *apiopenapi.DefinitionNamer) *openapicommon.OpenAPIV3Config {
	defaultConfig := &openapicommon.OpenAPIV3Config{
		IgnorePrefixes: []string{},
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title: "Generic API Server",
			},
		},
		DefaultResponse: &spec3.Response{
			ResponseProps: spec3.ResponseProps{
				Description: "Default Response.",
			},
		},
		GetOperationIDAndTags: apiopenapi.GetOperationIDAndTags,
		GetDefinitionName:     defNamer.GetDefinitionName,
		GetDefinitions:        getDefinitions,
	}
	defaultConfig.Definitions = getDefinitions(func(name string) spec.Ref {
		defName, _ := defaultConfig.GetDefinitionName(name)
		return spec.MustCreateRef("#/components/schemas/" + openapicommon.EscapeJsonPointer(defName))
	})

	return defaultConfig
}

func (c *AuthenticationInfo) ApplyClientCert(clientCA dynamiccertificates.CAContentProvider, servingInfo *SecureServingInfo) error {
	if servingInfo == nil {
		return nil
	}
	if clientCA == nil {
		return nil
	}
	if servingInfo.ClientCA == nil {
		servingInfo.ClientCA = clientCA
		return nil
	}

	servingInfo.ClientCA = dynamiccertificates.NewUnionCAContentProvider(servingInfo.ClientCA, clientCA)
	return nil
}

type completedConfig struct {
	*Config

	//===========================================================================
	// values below here are filled in during completion
	//===========================================================================

	// SharedInformerFactory provides shared informers for resources
	SharedInformerFactory informers.SharedInformerFactory
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// AddHealthChecks adds a health check to our config to be exposed by the health endpoints
// of our configured apiserver. We should prefer this to adding healthChecks directly to
// the config unless we explicitly want to add a healthcheck only to a specific health endpoint.
func (c *Config) AddHealthChecks(healthChecks ...healthz.HealthChecker) {
	c.HealthzChecks = append(c.HealthzChecks, healthChecks...)
	c.LivezChecks = append(c.LivezChecks, healthChecks...)
	c.ReadyzChecks = append(c.ReadyzChecks, healthChecks...)
}

// AddReadyzChecks adds a health check to our config to be exposed by the readyz endpoint
// of our configured apiserver.
func (c *Config) AddReadyzChecks(healthChecks ...healthz.HealthChecker) {
	c.ReadyzChecks = append(c.ReadyzChecks, healthChecks...)
}

// AddPostStartHook allows you to add a PostStartHook that will later be added to the server itself in a New call.
// Name conflicts will cause an error.
func (c *Config) AddPostStartHook(name string, hook PostStartHookFunc) error {
	if len(name) == 0 {
		return fmt.Errorf("missing name")
	}
	if hook == nil {
		return fmt.Errorf("hook func may not be nil: %q", name)
	}
	if c.DisabledPostStartHooks.Has(name) {
		klog.V(1).Infof("skipping %q because it was explicitly disabled", name)
		return nil
	}

	if postStartHook, exists := c.PostStartHooks[name]; exists {
		// this is programmer error, but it can be hard to debug
		return fmt.Errorf("unable to add %q because it was already registered by: %s", name, postStartHook.originatingStack)
	}
	c.PostStartHooks[name] = PostStartHookConfigEntry{hook: hook, originatingStack: string(debug.Stack())}

	return nil
}

// AddPostStartHookOrDie allows you to add a PostStartHook, but dies on failure.
func (c *Config) AddPostStartHookOrDie(name string, hook PostStartHookFunc) {
	if err := c.AddPostStartHook(name, hook); err != nil {
		klog.Fatalf("Error registering PostStartHook %q: %v", name, err)
	}
}

func completeOpenAPI(config *openapicommon.Config, version *version.Version) {
	if config == nil {
		return
	}
	if config.SecurityDefinitions != nil {
		// Setup OpenAPI security: all APIs will have the same authentication for now.
		config.DefaultSecurity = []map[string][]string{}
		keys := []string{}
		for k := range *config.SecurityDefinitions {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			config.DefaultSecurity = append(config.DefaultSecurity, map[string][]string{k: {}})
		}
		if config.CommonResponses == nil {
			config.CommonResponses = map[int]spec.Response{}
		}
		if _, exists := config.CommonResponses[http.StatusUnauthorized]; !exists {
			config.CommonResponses[http.StatusUnauthorized] = spec.Response{
				ResponseProps: spec.ResponseProps{
					Description: "Unauthorized",
				},
			}
		}
	}
	// make sure we populate info, and info.version, if not manually set
	if config.Info == nil {
		config.Info = &spec.Info{}
	}
	if config.Info.Version == "" {
		if version != nil {
			config.Info.Version = strings.Split(version.String(), "-")[0]
		} else {
			config.Info.Version = "unversioned"
		}
	}
}

func completeOpenAPIV3(config *openapicommon.OpenAPIV3Config, version *version.Version) {
	if config == nil {
		return
	}
	if config.SecuritySchemes != nil {
		// Setup OpenAPI security: all APIs will have the same authentication for now.
		config.DefaultSecurity = []map[string][]string{}
		keys := []string{}
		for k := range config.SecuritySchemes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			config.DefaultSecurity = append(config.DefaultSecurity, map[string][]string{k: {}})
		}
		if config.CommonResponses == nil {
			config.CommonResponses = map[int]*spec3.Response{}
		}
		if _, exists := config.CommonResponses[http.StatusUnauthorized]; !exists {
			config.CommonResponses[http.StatusUnauthorized] = &spec3.Response{
				ResponseProps: spec3.ResponseProps{
					Description: "Unauthorized",
				},
			}
		}
	}
	// make sure we populate info, and info.version, if not manually set
	if config.Info == nil {
		config.Info = &spec.Info{}
	}
	if config.Info.Version == "" {
		if version != nil {
			config.Info.Version = strings.Split(version.String(), "-")[0]
		} else {
			config.Info.Version = "unversioned"
		}
	}
}

// DrainedNotify returns a lifecycle signal of genericapiserver already drained while shutting down.
func (c *Config) DrainedNotify() <-chan struct{} {
	return c.lifecycleSignals.InFlightRequestsDrained.Signaled()
}

// ShutdownInitiated returns a lifecycle signal of apiserver shutdown having been initiated.
func (c *Config) ShutdownInitiatedNotify() <-chan struct{} {
	return c.lifecycleSignals.ShutdownInitiated.Signaled()
}

// Complete fills in any fields not set that are required to have valid data and can be derived
// from other fields. If you're going to `ApplyOptions`, do that first. It's mutating the receiver.
// Complete 接收一个 SharedInformerFactory，并为通用 APIServer 配置填充所有缺失的默认值。
func (c *Config) Complete(informers informers.SharedInformerFactory) CompletedConfig {
	// --- 1. 基础配置和默认值 ---

	// 如果没有为这个 APIServer 实例单独指定 FeatureGate，就使用全局的默认 FeatureGate。
	if c.FeatureGate == nil {
		c.FeatureGate = utilfeature.DefaultFeatureGate
	}
	// 如果 ExternalAddress (外部地址) 未设置，但 PublicAddress (公网地址) 已设置，
	// 则使用 PublicAddress 作为 ExternalAddress。这是一种向后兼容或配置合并的逻辑。
	if len(c.ExternalAddress) == 0 && c.PublicAddress != nil {
		c.ExternalAddress = c.PublicAddress.String()
	}

	// if there is no port, and we listen on one securely, use that one
	// 确保 ExternalAddress 包含端口号。这对于生成可访问的 URL至关重要。
	if _, _, err := net.SplitHostPort(c.ExternalAddress); err != nil {
		// 如果 ExternalAddress 本身不带端口（例如只是一个 IP），则尝试从 SecureServing 配置中推断。
		if c.SecureServing == nil {
			// 如果连 SecureServing (HTTPS 服务) 都没有配置，就无法推断端口，这是致命错误。
			klog.Fatalf("cannot derive external address port without listening on a secure port.")
		}
		// 从 SecureServing 配置中获取监听的主机和端口。
		_, port, err := c.SecureServing.HostPort()
		if err != nil {
			klog.Fatalf("cannot derive external address from the secure port: %v", err)
		}
		// 将推断出的端口号附加到 ExternalAddress 上。
		c.ExternalAddress = net.JoinHostPort(c.ExternalAddress, strconv.Itoa(port))
	}
	// --- 2. OpenAPI 和服务发现配置 ---

	// 补全 OpenAPI V2 和 V3 的配置。这会根据服务器的模拟版本（EmulationVersion）
	// 来设置正确的 OpenAPI 规范（spec）模板和行为。
	completeOpenAPI(c.OpenAPIConfig, c.EffectiveVersion.EmulationVersion())
	completeOpenAPIV3(c.OpenAPIV3Config, c.EffectiveVersion.EmulationVersion())

	// 如果服务发现地址（DiscoveryAddresses）未设置，则创建一个默认的，
	// 它会使用我们刚刚精心构造好的 ExternalAddress 作为客户端发现 API 的地址。
	if c.DiscoveryAddresses == nil {
		c.DiscoveryAddresses = discovery.DefaultAddresses{DefaultAddress: c.ExternalAddress}
	}
	// --- 3. 内部组件和依赖注入 ---

	// 为 Loopback 客户端配置认证信息。
	// Loopback 客户端是 APIServer 用来调用自己的客户端（例如，执行 admission webhook 调用）。
	// 这个函数确保了 loopback 客户端拥有一个合法的、能通过本服务器认证和授权的 token。
	AuthorizeClientBearerToken(c.LoopbackClientConfig, &c.Authentication, &c.Authorization)
	// 如果没有提供自定义的 RequestInfoResolver，就创建一个默认的。
	// RequestInfoResolver 的作用是解析一个 HTTP 请求，判断它对应的是哪个 API 资源、命名空间、动词等。
	if c.RequestInfoResolver == nil {
		c.RequestInfoResolver = NewRequestInfoResolver(c)
	}
	// 如果没有提供 EquivalentResourceRegistry，就创建一个默认的。
	// 这个注册表用于声明哪些资源实际上是等价的（例如 `apps/v1.deployments` 和 `apps/v1.deployments/status`）。
	if c.EquivalentResourceRegistry == nil {
		if c.RESTOptionsGetter == nil {
			// 如果没有提供 RESTOptionsGetter，无法获取存储细节，只能创建一个最简单的注册表。
			c.EquivalentResourceRegistry = runtime.NewEquivalentResourceRegistry()
		} else {
			// 如果有 RESTOptionsGetter，就可以创建一个更“智能”的注册表。
			// 这个注册表使用一个函数来为每个资源生成一个唯一标识。
			// 该函数会尝试使用资源的“存储前缀”作为标识，这能更准确地判断资源是否等价。
			c.EquivalentResourceRegistry = runtime.NewEquivalentResourceRegistryWithIdentity(func(groupResource schema.GroupResource) string {
				// use the storage prefix as the key if possible
				if opts, err := c.RESTOptionsGetter.GetRESTOptions(groupResource, nil); err == nil {
					return opts.ResourcePrefix
				}
				// otherwise return "" to use the default key (parent GV name)
				return ""
			})
		}
	}
	// --- 4. 返回最终配置 ---

	// 将已经补全的 `c` (Config) 和传入的 `informers` 一起包装成 CompletedConfig 并返回。
	// `informers` 会被后续的服务器构建过程使用，例如用于准入控制器。
	return CompletedConfig{&completedConfig{c, informers}}
}

// Complete fills in any fields not set that are required to have valid data and can be derived
// from other fields. If you're going to `ApplyOptions`, do that first. It's mutating the receiver.
func (c *RecommendedConfig) Complete() CompletedConfig {
	return c.Config.Complete(c.SharedInformerFactory)
}

var defaultAllowedMediaTypes = []string{
	runtime.ContentTypeJSON,
	runtime.ContentTypeYAML,
	runtime.ContentTypeProtobuf,
}

// New creates a new server which logically combines the handling chain with the passed server.
// name is used to differentiate for logging. The handler chain in particular can be difficult as it starts delegating.
// delegationTarget may not be nil.
func (c completedConfig) New(name string, delegationTarget DelegationTarget) (*GenericAPIServer, error) {
	if c.Serializer == nil {
		return nil, fmt.Errorf("Genericapiserver.New() called with config.Serializer == nil")
	}
	allowedMediaTypes := defaultAllowedMediaTypes
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		allowedMediaTypes = append(allowedMediaTypes, runtime.ContentTypeCBOR)
	}
	for _, info := range c.Serializer.SupportedMediaTypes() {
		var ok bool
		for _, mt := range allowedMediaTypes {
			if info.MediaType == mt {
				ok = true
				break
			}
		}
		if !ok {
			return nil, fmt.Errorf("refusing to create new apiserver %q with support for media type %q (allowed media types are: %s)", name, info.MediaType, strings.Join(allowedMediaTypes, ", "))
		}
	}
	if c.LoopbackClientConfig == nil {
		return nil, fmt.Errorf("Genericapiserver.New() called with config.LoopbackClientConfig == nil")
	}
	if c.EquivalentResourceRegistry == nil {
		return nil, fmt.Errorf("Genericapiserver.New() called with config.EquivalentResourceRegistry == nil")
	}

	handlerChainBuilder := func(handler http.Handler) http.Handler {
		return c.BuildHandlerChainFunc(handler, c.Config)
	}

	var debugSocket *routes.DebugSocket
	if c.DebugSocketPath != "" {
		debugSocket = routes.NewDebugSocket(c.DebugSocketPath)
	}

	apiServerHandler := NewAPIServerHandler(name, c.Serializer, handlerChainBuilder, delegationTarget.UnprotectedHandler())

	s := &GenericAPIServer{
		discoveryAddresses:             c.DiscoveryAddresses,
		LoopbackClientConfig:           c.LoopbackClientConfig,
		legacyAPIGroupPrefixes:         c.LegacyAPIGroupPrefixes,
		admissionControl:               c.AdmissionControl,
		Serializer:                     c.Serializer,
		AuditBackend:                   c.AuditBackend,
		Authorizer:                     c.Authorization.Authorizer,
		delegationTarget:               delegationTarget,
		EquivalentResourceRegistry:     c.EquivalentResourceRegistry,
		NonLongRunningRequestWaitGroup: c.NonLongRunningRequestWaitGroup,
		WatchRequestWaitGroup:          c.WatchRequestWaitGroup,
		Handler:                        apiServerHandler,
		UnprotectedDebugSocket:         debugSocket,

		listedPathProvider: apiServerHandler,

		minRequestTimeout:                   time.Duration(c.MinRequestTimeout) * time.Second,
		ShutdownTimeout:                     c.RequestTimeout,
		ShutdownDelayDuration:               c.ShutdownDelayDuration,
		ShutdownWatchTerminationGracePeriod: c.ShutdownWatchTerminationGracePeriod,
		SecureServingInfo:                   c.SecureServing,
		ExternalAddress:                     c.ExternalAddress,

		openAPIConfig:           c.OpenAPIConfig,
		openAPIV3Config:         c.OpenAPIV3Config,
		skipOpenAPIInstallation: c.SkipOpenAPIInstallation,

		postStartHooks:         map[string]postStartHookEntry{},
		preShutdownHooks:       map[string]preShutdownHookEntry{},
		disabledPostStartHooks: c.DisabledPostStartHooks,

		healthzRegistry:  healthCheckRegistry{path: "/healthz", checks: c.HealthzChecks},
		livezRegistry:    healthCheckRegistry{path: "/livez", checks: c.LivezChecks, clock: clock.RealClock{}},
		readyzRegistry:   healthCheckRegistry{path: "/readyz", checks: c.ReadyzChecks},
		livezGracePeriod: c.LivezGracePeriod,

		DiscoveryGroupManager: discovery.NewRootAPIsHandler(c.DiscoveryAddresses, c.Serializer),

		maxRequestBodyBytes: c.MaxRequestBodyBytes,

		lifecycleSignals:       c.lifecycleSignals,
		ShutdownSendRetryAfter: c.ShutdownSendRetryAfter,

		APIServerID:           c.APIServerID,
		StorageReadinessHook:  NewStorageReadinessHook(c.StorageInitializationTimeout),
		StorageVersionManager: c.StorageVersionManager,

		EffectiveVersion:                        c.EffectiveVersion,
		EmulationForwardCompatible:              c.EmulationForwardCompatible,
		RuntimeConfigEmulationForwardCompatible: c.RuntimeConfigEmulationForwardCompatible,
		FeatureGate:                             c.FeatureGate,

		muxAndDiscoveryCompleteSignals: map[string]<-chan struct{}{},
	}

	manager := c.AggregatedDiscoveryGroupManager
	if manager == nil {
		manager = discoveryendpoint.NewResourceManager("apis")
	}
	s.AggregatedDiscoveryGroupManager = manager
	s.AggregatedLegacyDiscoveryGroupManager = discoveryendpoint.NewResourceManager("api")
	for {
		if c.JSONPatchMaxCopyBytes <= 0 {
			break
		}
		existing := atomic.LoadInt64(&jsonpatch.AccumulatedCopySizeLimit)
		if existing > 0 && existing < c.JSONPatchMaxCopyBytes {
			break
		}
		if atomic.CompareAndSwapInt64(&jsonpatch.AccumulatedCopySizeLimit, existing, c.JSONPatchMaxCopyBytes) {
			break
		}
	}

	// first add poststarthooks from delegated targets
	for k, v := range delegationTarget.PostStartHooks() {
		s.postStartHooks[k] = v
	}

	for k, v := range delegationTarget.PreShutdownHooks() {
		s.preShutdownHooks[k] = v
	}

	// add poststarthooks that were preconfigured.  Using the add method will give us an error if the same name has already been registered.
	for name, preconfiguredPostStartHook := range c.PostStartHooks {
		if err := s.AddPostStartHook(name, preconfiguredPostStartHook.hook); err != nil {
			return nil, err
		}
	}

	// register mux signals from the delegated server
	for k, v := range delegationTarget.MuxAndDiscoveryCompleteSignals() {
		if err := s.RegisterMuxAndDiscoveryCompleteSignal(k, v); err != nil {
			return nil, err
		}
	}

	genericApiServerHookName := "generic-apiserver-start-informers"
	if c.SharedInformerFactory != nil {
		if !s.isPostStartHookRegistered(genericApiServerHookName) {
			err := s.AddPostStartHook(genericApiServerHookName, func(hookContext PostStartHookContext) error {
				c.SharedInformerFactory.Start(hookContext.Done())
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
		// TODO: Once we get rid of /healthz consider changing this to post-start-hook.
		err := s.AddReadyzChecks(healthz.NewInformerSyncHealthz(c.SharedInformerFactory))
		if err != nil {
			return nil, err
		}
	}

	const priorityAndFairnessConfigConsumerHookName = "priority-and-fairness-config-consumer"
	if s.isPostStartHookRegistered(priorityAndFairnessConfigConsumerHookName) {
	} else if c.FlowControl != nil {
		err := s.AddPostStartHook(priorityAndFairnessConfigConsumerHookName, func(hookContext PostStartHookContext) error {
			go c.FlowControl.Run(hookContext.Done())
			return nil
		})
		if err != nil {
			return nil, err
		}
		// TODO(yue9944882): plumb pre-shutdown-hook for request-management system?
	} else {
		klog.V(3).Infof("Not requested to run hook %s", priorityAndFairnessConfigConsumerHookName)
	}

	// Add PostStartHooks for maintaining the watermarks for the Priority-and-Fairness and the Max-in-Flight filters.
	if c.FlowControl != nil {
		const priorityAndFairnessFilterHookName = "priority-and-fairness-filter"
		if !s.isPostStartHookRegistered(priorityAndFairnessFilterHookName) {
			err := s.AddPostStartHook(priorityAndFairnessFilterHookName, func(hookContext PostStartHookContext) error {
				genericfilters.StartPriorityAndFairnessWatermarkMaintenance(hookContext.Done())
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	} else {
		const maxInFlightFilterHookName = "max-in-flight-filter"
		if !s.isPostStartHookRegistered(maxInFlightFilterHookName) {
			err := s.AddPostStartHook(maxInFlightFilterHookName, func(hookContext PostStartHookContext) error {
				genericfilters.StartMaxInFlightWatermarkMaintenance(hookContext.Done())
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}

	// Add PostStartHook for maintenaing the object count tracker.
	if c.StorageObjectCountTracker != nil {
		const storageObjectCountTrackerHookName = "storage-object-count-tracker-hook"
		if !s.isPostStartHookRegistered(storageObjectCountTrackerHookName) {
			if err := s.AddPostStartHook(storageObjectCountTrackerHookName, func(hookContext PostStartHookContext) error {
				go c.StorageObjectCountTracker.RunUntil(hookContext.Done())
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}

	for _, delegateCheck := range delegationTarget.HealthzChecks() {
		skip := false
		for _, existingCheck := range c.HealthzChecks {
			if existingCheck.Name() == delegateCheck.Name() {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		s.AddHealthChecks(delegateCheck)
	}
	s.RegisterDestroyFunc(func() {
		if err := c.Config.TracerProvider.Shutdown(context.Background()); err != nil {
			klog.Errorf("failed to shut down tracer provider: %v", err)
		}
	})

	s.listedPathProvider = routes.ListedPathProviders{s.listedPathProvider, delegationTarget}

	installAPI(name, s, c.Config)

	// use the UnprotectedHandler from the delegation target to ensure that we don't attempt to double authenticator, authorize,
	// or some other part of the filter chain in delegation cases.
	if delegationTarget.UnprotectedHandler() == nil && c.EnableIndex {
		s.Handler.NonGoRestfulMux.NotFoundHandler(routes.IndexLister{
			StatusCode:   http.StatusNotFound,
			PathProvider: s.listedPathProvider,
		})
	}

	return s, nil
}

func BuildHandlerChainWithStorageVersionPrecondition(apiHandler http.Handler, c *Config) http.Handler {
	// WithStorageVersionPrecondition needs the WithRequestInfo to run first
	handler := genericapifilters.WithStorageVersionPrecondition(apiHandler, c.StorageVersionManager, c.Serializer)
	return DefaultBuildHandlerChain(handler, c)
}

// DefaultBuildHandlerChain 构建了 apiserver 的标准 HTTP 处理器链（Handler Chain）。
// 这是一个经典的“洋葱模型”，每个处理器（中间件）将下一个处理器包裹起来，对请求进行层层处理。
//
// 参数:
//
//	apiHandler: http.Handler - 这是最核心的业务逻辑处理器，通常是 go-restful 的 Mux，负责将请求路由到具体的 REST 实现。
//	c: *Config - 包含了 apiserver 的所有配置信息，如认证、授权、审计等。
//
// 返回:
//
//	http.Handler - 经过层层包裹后的、完整的处理器链的入口。
func DefaultBuildHandlerChain(apiHandler http.Handler, c *Config) http.Handler {

	handler := apiHandler
	// --- 包裹授权 (Authorization) ---
	// 在进入此处理器之前，请求必须已经通过了认证、审计等更外层的处理器。
	// 这个处理器检查已认证的用户是否有权限执行当前请求的操作。
	// TrackCompleted 标记此阶段的结束，用于延迟跟踪。
	klog.V(4).Infof("TrackCompleted 标记此阶段的结束，用于延迟跟踪。")
	handler = filterlatency.TrackCompleted(handler)
	// WithAuthorization 是授权的核心，如果授权失败，它将终止请求并返回 403 Forbidden。
	klog.V(4).Infof("WithAuthorization 是授权的核心，如果授权失败，它将终止请求并返回 403 Forbidden。")
	handler = genericapifilters.WithAuthorization(handler, c.Authorization.Authorizer, c.Serializer)
	// TrackStarted 标记授权阶段的开始，用于延迟跟踪。
	klog.V(4).Infof(" TrackStarted 标记授权阶段的开始，用于延迟跟踪")
	handler = filterlatency.TrackStarted(handler, c.TracerProvider, "authorization")
	// --- 包裹流量控制 (Flow Control) ---
	klog.V(4).Infof("- 包裹流量控制 (Flow Control)")
	if c.FlowControl != nil {
		// 如果启用了 APF (API 优先级与公平性)
		// 创建一个请求工作量估算器，用于评估每个请求的“成本”。
		klog.V(4).Infof("创建一个请求工作量估算器，用于评估每个请求的“成本”")
		workEstimatorCfg := flowcontrolrequest.DefaultWorkEstimatorConfig()
		requestWorkEstimator := flowcontrolrequest.NewWorkEstimator(
			c.StorageObjectCountTracker.Get, c.FlowControl.GetInterestedWatchCount, workEstimatorCfg, c.FlowControl.GetMaxSeats)
		// 包裹 APF 处理器。它会根据请求的优先级和当前系统负载，决定是立即执行还是排队等待。
		klog.V(4).Infof(" 包裹 APF 处理器。它会根据请求的优先级和当前系统负载，决定是立即执行还是排队等待。")
		handler = filterlatency.TrackCompleted(handler)
		handler = genericfilters.WithPriorityAndFairness(handler, c.LongRunningFunc, c.FlowControl, requestWorkEstimator, c.RequestTimeout/4)
		handler = filterlatency.TrackStarted(handler, c.TracerProvider, "priorityandfairness")
	} else {
		// 如果使用旧的最大并发请求数限制 (MaxInFlight)
		// 这个处理器会限制同时在服务器中处理的请求总数。
		klog.V(4).Infof("这个处理器会限制同时在服务器中处理的请求总数。。")
		handler = genericfilters.WithMaxInFlightLimit(handler, c.MaxRequestsInFlight, c.MaxMutatingRequestsInFlight, c.LongRunningFunc)
	}
	// --- 包裹用户模拟 (Impersonation) ---
	// 允许高权限用户模拟成其他用户来发起请求。此处理器会校验模拟操作是否被授权。
	klog.V(4).Infof(" 允许高权限用户模拟成其他用户来发起请求。此处理器会校验模拟操作是否被授权。。")
	handler = filterlatency.TrackCompleted(handler)

	handler = genericapifilters.WithImpersonation(handler, c.Authorization.Authorizer, c.Serializer)
	handler = filterlatency.TrackStarted(handler, c.TracerProvider, "impersonation")
	// --- 包裹审计 (Audit) ---
	// 记录关于请求的详细信息（谁、在何时、做了什么），用于事后追踪和安全分析。
	klog.V(4).Infof("记录关于请求的详细信息（谁、在何时、做了什么），用于事后追踪和安全分析。。。")
	handler = filterlatency.TrackCompleted(handler)
	handler = genericapifilters.WithAudit(handler, c.AuditBackend, c.AuditPolicyRuleEvaluator, c.LongRunningFunc)
	handler = filterlatency.TrackStarted(handler, c.TracerProvider, "audit")
	// --- 包裹认证 (Authentication) ---
	// 这是非常关键的一层，它识别请求者的身份。
	// 首先，定义一个处理认证失败的 handler。如果认证失败，请求会走向这里。
	klog.V(4).Infof("首先，定义一个处理认证失败的 handler。如果认证失败，请求会走向这里。")
	failedHandler := genericapifilters.Unauthorized(c.Serializer)
	// 并为失败的认证尝试也记录审计日志。
	klog.V(4).Infof("并为失败的认证尝试也记录审计日志。")
	failedHandler = genericapifilters.WithFailedAuthenticationAudit(failedHandler, c.AuditBackend, c.AuditPolicyRuleEvaluator)

	// WithTracing comes after authentication so we can allow authenticated
	// clients to influence sampling.
	// [特性门控] 链路追踪 (Tracing) 处理器。它在认证之后，以便可以根据用户身份决定采样策略。
	klog.V(4).Infof("[特性门控] 链路追踪 (Tracing) 处理器。它在认证之后，以便可以根据用户身份决定采样策略。。")
	if c.FeatureGate.Enabled(genericfeatures.APIServerTracing) {
		handler = genericapifilters.WithTracing(handler, c.TracerProvider)
	}
	// 为 failedHandler 和主 handler 都包裹上延迟跟踪。
	klog.V(4).Infof("为 failedHandler 和主 handler 都包裹上延迟跟踪。")
	failedHandler = filterlatency.TrackCompleted(failedHandler)
	// WithAuthentication 是认证的核心。它使用配置的认证器来验证凭证。
	// 如果成功，请求继续向内层传递；如果失败，则转交给 failedHandler。
	klog.V(4).Infof("WithAuthentication 是认证的核心。它使用配置的认证器来验证凭证。。")
	handler = filterlatency.TrackCompleted(handler)
	handler = genericapifilters.WithAuthentication(handler, c.Authentication.Authenticator, failedHandler, c.Authentication.APIAudiences, c.Authentication.RequestHeaderConfig)
	handler = filterlatency.TrackStarted(handler, c.TracerProvider, "authentication") // 标记认证阶段的开始
	// --- 包裹 CORS (跨域资源共享) ---
	// 处理来自浏览器的跨域请求，根据配置的来源列表决定是否允许。
	klog.V(4).Infof("处理来自浏览器的跨域请求，根据配置的来源列表决定是否允许。。")
	handler = genericfilters.WithCORS(handler, c.CorsAllowedOriginList, nil, nil, nil, "true")

	// WithWarningRecorder must be wrapped by the timeout handler
	// to make the addition of warning headers threadsafe
	// --- 包裹 Warning 头记录器 ---
	// 此处理器会创建一个上下文，用于收集在请求处理深层产生的警告信息，
	// 并在最终响应时将它们统一添加到 "Warning" HTTP 头中。
	klog.V(4).Infof(" 包裹 Warning 头记录器 ---")
	handler = genericapifilters.WithWarningRecorder(handler)

	// WithTimeoutForNonLongRunningRequests will call the rest of the request handling in a go-routine with the
	// context with deadline. The go-routine can keep running, while the timeout logic will return a timeout to the client.
	// --- 包裹超时处理 (Timeout) ---
	// 为非长连接请求（如 get, list, create）设置一个超时时间。
	// 如果在规定时间内未处理完，它会直接向客户端返回超时错误，保护服务器。
	klog.V(4).Infof(" 包为非长连接请求（如 get, list, create）设置一个超时时间。 ---")
	handler = genericfilters.WithTimeoutForNonLongRunningRequests(handler, c.LongRunningFunc)
	// --- 包裹请求截止时间 (Deadline) ---
	// 进一步处理请求的截止时间，并与审计系统集成。
	klog.V(4).Infof(" 进一步处理请求的截止时间，并与审计系统集成。---")
	handler = genericapifilters.WithRequestDeadline(handler, c.AuditBackend, c.AuditPolicyRuleEvaluator,
		c.LongRunningFunc, c.Serializer, c.RequestTimeout)
	// --- 包裹 WaitGroup 管理 ---
	// 将请求加入 WaitGroup，用于跟踪正在处理的请求总数。这对于实现优雅停机至关重要，
	// 服务器关闭前会等待所有正在处理的请求完成。
	klog.V(4).Infof(" 将请求加入 WaitGroup，用于跟踪正在处理的请求总数。这对于实现优雅停机至关重要，---")
	handler = genericfilters.WithWaitGroup(handler, c.LongRunningFunc, c.NonLongRunningRequestWaitGroup)

	// --- 包裹优雅停机相关的处理器 ---
	// 在服务器关闭期间，优雅地终止正在进行的 watch 请求。
	klog.V(4).Infof(" 在服务器关闭期间，优雅地终止正在进行的 watch 请求。，---")
	if c.ShutdownWatchTerminationGracePeriod > 0 {
		handler = genericfilters.WithWatchTerminationDuringShutdown(handler, c.lifecycleSignals, c.WatchRequestWaitGroup)
	}
	// [HTTP/2] 以一定概率向客户端发送 GOAWAY 帧，促使客户端重连，有助于负载均衡。
	klog.V(4).Infof(" [HTTP/2] 以一定概率向客户端发送 GOAWAY 帧，促使客户端重连，有助于负载均衡。---")
	if c.SecureServing != nil && !c.SecureServing.DisableHTTP2 && c.GoawayChance > 0 {
		handler = genericfilters.WithProbabilisticGoaway(handler, c.GoawayChance)
	}
	// --- 包裹基础 HTTP 头处理器 ---
	// 为响应添加 "Cache-Control: no-cache, private" 头，防止客户端或代理缓存 API 响应。
	klog.V(4).Infof("为响应添加Cache-Control: no-cache, private 头，防止客户端或代理缓存 API 响应。")
	handler = genericapifilters.WithCacheControl(handler)
	// 为响应添加 HSTS (HTTP Strict Transport Security) 头，强制客户端后续使用 HTTPS。
	klog.V(4).Infof("为响应添加 HSTS (HTTP Strict Transport Security) 头，强制客户端后续使用 HTTPS。")
	handler = genericfilters.WithHSTS(handler, c.HSTSDirectives)

	// --- 包裹最外层的几个处理器 ---
	// 当服务器正在关闭时，向新来的请求返回 503 Service Unavailable 并附带 Retry-After 头。
	klog.V(4).Infof("当服务器正在关闭时，向新来的请求返回 503 Service Unavailable 并附带 Retry-After 头。。")
	if c.ShutdownSendRetryAfter {
		handler = genericfilters.WithRetryAfter(handler, c.lifecycleSignals.NotAcceptingNewRequest.Signaled())
	}
	// 在请求完成时，打印一条详细的 HTTP 日志。这是我们在日志里最常见到的那一行。
	handler = genericfilters.WithHTTPLogging(handler)
	// 另一个与延迟跟踪相关的包裹。
	handler = genericapifilters.WithLatencyTrackers(handler)
	// WithRoutine will execute future handlers in a separate goroutine and serving
	// handler in current goroutine to minimize the stack memory usage. It must be
	// after WithPanicRecover() to be protected from panics.
	// [特性门控] 使用 Goroutine 处理请求，以减少主 goroutine 的栈内存使用。

	if c.FeatureGate.Enabled(genericfeatures.APIServingWithRoutine) {
		handler = routine.WithRoutine(handler, c.LongRunningFunc)
	}
	// 解析请求的 URL，将其中的信息（verb, resource, namespace 等）封装成 RequestInfo 对象，存入上下文。
	// 这是非常基础且重要的一步，后续很多处理器都依赖这些信息。
	klog.V(4).Infof("解析请求的 URL，将其中的信息（verb, resource, namespace 等）封装成 RequestInfo 对象，存入上下文。")
	handler = genericapifilters.WithRequestInfo(handler, c.RequestInfoResolver)
	// 在请求对象中注入一个时间戳，记录请求到达服务器的精确时间。
	klog.V(4).Infof(" 在请求对象中注入一个时间戳，记录请求到达服务器的精确时间。")
	handler = genericapifilters.WithRequestReceivedTimestamp(handler)
	// 在 apiserver 完全准备好服务之前，阻塞新来的请求。
	klog.V(4).Infof("在 apiserver 完全准备好服务之前，阻塞新来的请求。")
	handler = genericapifilters.WithMuxAndDiscoveryComplete(handler, c.lifecycleSignals.MuxAndDiscoveryComplete.Signaled())
	// 这是最外层的保护网：捕获请求处理过程中的任何 panic，防止 apiserver 进程崩溃。
	klog.V(4).Infof("这是最外层的保护网：捕获请求处理过程中的任何 panic，防止 apiserver 进程崩溃。")
	handler = genericfilters.WithPanicRecovery(handler, c.RequestInfoResolver)
	// 为每个请求初始化一个审计事件的上下文，供后续的审计处理器使用。
	klog.V(4).Infof("为每个请求初始化一个审计事件的上下文，供后续的审计处理器使用")
	handler = genericapifilters.WithAuditInit(handler)
	// 返回经过层层包裹后的、完整的处理器链的入口。

	return handler
}

func installAPI(name string, s *GenericAPIServer, c *Config) {
	if c.EnableIndex {
		routes.Index{}.Install(s.listedPathProvider, s.Handler.NonGoRestfulMux)
	}
	if c.EnableProfiling {
		routes.Profiling{}.Install(s.Handler.NonGoRestfulMux)
		if c.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		// so far, only logging related endpoints are considered valid to add for these debug flags.
		routes.DebugFlags{}.Install(s.Handler.NonGoRestfulMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}
	if s.UnprotectedDebugSocket != nil {
		s.UnprotectedDebugSocket.InstallProfiling()
		s.UnprotectedDebugSocket.InstallDebugFlag("v", routes.StringFlagPutHandler(logs.GlogSetter))
		if c.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
	}

	if c.EnableMetrics {
		if c.EnableProfiling {
			routes.MetricsWithReset{}.Install(s.Handler.NonGoRestfulMux)
			slis.SLIMetricsWithReset{}.Install(s.Handler.NonGoRestfulMux)
		} else {
			routes.DefaultMetrics{}.Install(s.Handler.NonGoRestfulMux)
			slis.SLIMetrics{}.Install(s.Handler.NonGoRestfulMux)
		}
	}

	routes.Version{Version: c.EffectiveVersion.Info()}.Install(s.Handler.GoRestfulContainer)

	if c.EnableDiscovery {
		wrapped := discoveryendpoint.WrapAggregatedDiscoveryToHandler(s.DiscoveryGroupManager, s.AggregatedDiscoveryGroupManager)
		s.Handler.GoRestfulContainer.Add(wrapped.GenerateWebService("/apis", metav1.APIGroupList{}))
	}
	if c.FlowControl != nil {
		c.FlowControl.Install(s.Handler.NonGoRestfulMux)
	}
}

func NewRequestInfoResolver(c *Config) *apirequest.RequestInfoFactory {
	apiPrefixes := sets.NewString(strings.Trim(APIGroupPrefix, "/")) // all possible API prefixes
	legacyAPIPrefixes := sets.String{}                               // APIPrefixes that won't have groups (legacy)
	for legacyAPIPrefix := range c.LegacyAPIGroupPrefixes {
		apiPrefixes.Insert(strings.Trim(legacyAPIPrefix, "/"))
		legacyAPIPrefixes.Insert(strings.Trim(legacyAPIPrefix, "/"))
	}

	return &apirequest.RequestInfoFactory{
		APIPrefixes:          apiPrefixes,
		GrouplessAPIPrefixes: legacyAPIPrefixes,
	}
}

func (s *SecureServingInfo) HostPort() (string, int, error) {
	if s == nil || s.Listener == nil {
		return "", 0, fmt.Errorf("no listener found")
	}
	addr := s.Listener.Addr().String()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("failed to get port from listener address %q: %v", addr, err)
	}
	port, err := utilsnet.ParsePort(portStr, true)
	if err != nil {
		return "", 0, fmt.Errorf("invalid non-numeric port %q", portStr)
	}
	return host, port, nil
}

// AuthorizeClientBearerToken wraps the authenticator and authorizer in loopback authentication logic
// if the loopback client config is specified AND it has a bearer token. Note that if either authn or
// authz is nil, this function won't add a token authenticator or authorizer.
func AuthorizeClientBearerToken(loopback *restclient.Config, authn *AuthenticationInfo, authz *AuthorizationInfo) {
	if loopback == nil || len(loopback.BearerToken) == 0 {
		return
	}
	if authn == nil || authz == nil {
		// prevent nil pointer panic
		return
	}
	if authn.Authenticator == nil || authz.Authorizer == nil {
		// authenticator or authorizer might be nil if we want to bypass authz/authn
		// and we also do nothing in this case.
		return
	}

	privilegedLoopbackToken := loopback.BearerToken
	var uid = uuid.New().String()
	tokens := make(map[string]*user.DefaultInfo)
	tokens[privilegedLoopbackToken] = &user.DefaultInfo{
		Name:   user.APIServerUser,
		UID:    uid,
		Groups: []string{user.AllAuthenticated, user.SystemPrivilegedGroup},
	}

	tokenAuthenticator := authenticatorfactory.NewFromTokens(tokens, authn.APIAudiences)
	authn.Authenticator = authenticatorunion.New(tokenAuthenticator, authn.Authenticator)
}

// For testing purpose only
func SetHostnameFuncForTests(name string) {
	hostnameFunc = func() (host string, err error) {
		host = name
		err = nil
		return
	}
}
