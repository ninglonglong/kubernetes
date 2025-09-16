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

// Package options contains flags and options for initializing an apiserver
package options

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	peerreconcilers "k8s.io/apiserver/pkg/reconcilers"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/client-go/util/keyutil"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/zpages/flagz"
	"k8s.io/klog/v2"
	netutil "k8s.io/utils/net"

	"k8s.io/kubernetes/pkg/apis/authentication/validation"
	_ "k8s.io/kubernetes/pkg/features"
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
	"k8s.io/kubernetes/pkg/serviceaccount"
	"k8s.io/kubernetes/pkg/serviceaccount/externaljwt/plugin"
)

// Options define the flags and validation for a generic controlplane. If the
// structs are nil, the options are not added to the command line and not validated.
type Options struct {
	Flagz                   flagz.Reader
	GenericServerRunOptions *genericoptions.ServerRunOptions
	Etcd                    *genericoptions.EtcdOptions
	SecureServing           *genericoptions.SecureServingOptionsWithLoopback
	Audit                   *genericoptions.AuditOptions
	Features                *genericoptions.FeatureOptions
	Admission               *kubeoptions.AdmissionOptions
	Authentication          *kubeoptions.BuiltInAuthenticationOptions
	Authorization           *kubeoptions.BuiltInAuthorizationOptions
	APIEnablement           *genericoptions.APIEnablementOptions
	EgressSelector          *genericoptions.EgressSelectorOptions
	Metrics                 *metrics.Options
	Logs                    *logs.Options
	Traces                  *genericoptions.TracingOptions

	EnableLogsHandler        bool
	EventTTL                 time.Duration
	MaxConnectionBytesPerSec int64

	ProxyClientCertFile string
	ProxyClientKeyFile  string

	// PeerCAFile is the ca bundle used by this kube-apiserver to verify peer apiservers'
	// serving certs when routing a request to the peer in the case the request can not be served
	// locally due to version skew.
	PeerCAFile string

	// PeerAdvertiseAddress is the IP for this kube-apiserver which is used by peer apiservers to route a request
	// to this apiserver. This happens in cases where the peer is not able to serve the request due to
	// version skew.
	PeerAdvertiseAddress peerreconcilers.PeerAdvertiseAddress

	EnableAggregatorRouting             bool
	AggregatorRejectForwardingRedirects bool

	ServiceAccountSigningKeyFile     string
	ServiceAccountIssuer             serviceaccount.TokenGenerator
	ServiceAccountTokenMaxExpiration time.Duration

	ShowHiddenMetricsForVersion string

	SystemNamespaces []string

	ServiceAccountSigningEndpoint string

	CoordinatedLeadershipLeaseDuration time.Duration
	CoordinatedLeadershipRenewDeadline time.Duration
	CoordinatedLeadershipRetryPeriod   time.Duration
}

// completedServerRunOptions is a private wrapper that enforces a call of Complete() before Run can be invoked.
type completedOptions struct {
	Options
}

type CompletedOptions struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedOptions
}

// NewOptions creates a new ServerRunOptions object with default parameters
func NewOptions() *Options {
	s := Options{
		// 通用服务器运行选项，包含了如外部主机名、CORS 设置等。
		GenericServerRunOptions: genericoptions.NewServerRunOptions(),
		// Etcd 相关选项，用于配置与 etcd 的连接和存储。
		// storagebackend.NewDefaultConfig 设置了在 etcd 中存储 Kubernetes 对象的前缀，默认为 "/registry"。
		Etcd: genericoptions.NewEtcdOptions(storagebackend.NewDefaultConfig(kubeoptions.DefaultEtcdPathPrefix, nil)),
		// 安全服务选项，用于配置 HTTPS 证书和端口。
		SecureServing: kubeoptions.NewSecureServingOptions(),
		// 审计日志选项，用于配置 API 请求的审计。
		Audit: genericoptions.NewAuditOptions(),
		// 特性开关选项，用于启用或禁用 Kubernetes 的各项实验性功能。
		Features: genericoptions.NewFeatureOptions(),
		// 准入控制选项，用于配置准入插件链，在对象持久化之前进行验证和/或修改。
		Admission: kubeoptions.NewAdmissionOptions(),
		// 认证选项，默认启用所有内置的认证方法（如 X509 证书、ServiceAccount token 等）。
		Authentication: kubeoptions.NewBuiltInAuthenticationOptions().WithAll(),
		// 授权选项，用于配置授权模式（如 RBAC）。
		Authorization: kubeoptions.NewBuiltInAuthorizationOptions(),
		// API 启用选项，用于控制哪些 API 组和版本是启用的。
		APIEnablement: genericoptions.NewAPIEnablementOptions(),
		// 出口选择器选项，用于配置 apiserver 对外发出的网络请求的代理。
		EgressSelector: genericoptions.NewEgressSelectorOptions(),
		// 指标（Metrics）选项，用于配置 Prometheus 指标的暴露。
		Metrics: metrics.NewOptions(),
		Logs:    logs.NewOptions(),
		// 链路追踪选项，用于配置分布式追踪。
		Traces: genericoptions.NewTracingOptions(),
		// 是否启用日志处理器（/logs 端点），默认为 false，出于安全考虑。
		EnableLogsHandler:                   false,
		EventTTL:                            1 * time.Hour,
		AggregatorRejectForwardingRedirects: true,
		SystemNamespaces:                    []string{metav1.NamespaceSystem, metav1.NamespacePublic, metav1.NamespaceDefault},
		// 领导者租约的持续时间。
		CoordinatedLeadershipLeaseDuration: 15 * time.Second,
		// 领导者在租约到期前必须续订租约的最后期限。
		CoordinatedLeadershipRenewDeadline: 10 * time.Second,
		// 非领导者实例尝试获取租约的重试间隔。
		CoordinatedLeadershipRetryPeriod: 2 * time.Second,
	}

	// Overwrite the default for storage data format.
	s.Etcd.DefaultStorageMediaType = "application/vnd.kubernetes.protobuf"

	return &s
}

func (s *Options) AddFlags(fss *cliflag.NamedFlagSets) {
	// Add the generic flags.
	s.GenericServerRunOptions.AddUniversalFlags(fss.FlagSet("generic"))
	s.Etcd.AddFlags(fss.FlagSet("etcd"))
	s.SecureServing.AddFlags(fss.FlagSet("secure serving"))
	s.Audit.AddFlags(fss.FlagSet("auditing"))
	s.Features.AddFlags(fss.FlagSet("features"))
	s.Authentication.AddFlags(fss.FlagSet("authentication"))
	s.Authorization.AddFlags(fss.FlagSet("authorization"))
	s.APIEnablement.AddFlags(fss.FlagSet("API enablement"))
	s.EgressSelector.AddFlags(fss.FlagSet("egress selector"))
	s.Admission.AddFlags(fss.FlagSet("admission"))
	s.Metrics.AddFlags(fss.FlagSet("metrics"))
	logsapi.AddFlags(s.Logs, fss.FlagSet("logs"))
	s.Traces.AddFlags(fss.FlagSet("traces"))

	// Note: the weird ""+ in below lines seems to be the only way to get gofmt to
	// arrange these text blocks sensibly. Grrr.
	fs := fss.FlagSet("misc")
	fs.DurationVar(&s.EventTTL, "event-ttl", s.EventTTL,
		"Amount of time to retain events.")

	fs.BoolVar(&s.EnableLogsHandler, "enable-logs-handler", s.EnableLogsHandler,
		"If true, install a /logs handler for the apiserver logs.")
	fs.MarkDeprecated("enable-logs-handler", "Log handler functionality is deprecated") //nolint:errcheck
	fs.Lookup("enable-logs-handler").Hidden = false

	fs.Int64Var(&s.MaxConnectionBytesPerSec, "max-connection-bytes-per-sec", s.MaxConnectionBytesPerSec, ""+
		"If non-zero, throttle each user connection to this number of bytes/sec. "+
		"Currently only applies to long-running requests.")

	fs.StringVar(&s.ProxyClientCertFile, "proxy-client-cert-file", s.ProxyClientCertFile, ""+
		"Client certificate used to prove the identity of the aggregator or kube-apiserver "+
		"when it must call out during a request. This includes proxying requests to a user "+
		"api-server and calling out to webhook admission plugins. It is expected that this "+
		"cert includes a signature from the CA in the --requestheader-client-ca-file flag. "+
		"That CA is published in the 'extension-apiserver-authentication' configmap in "+
		"the kube-system namespace. Components receiving calls from kube-aggregator should "+
		"use that CA to perform their half of the mutual TLS verification.")
	fs.StringVar(&s.ProxyClientKeyFile, "proxy-client-key-file", s.ProxyClientKeyFile, ""+
		"Private key for the client certificate used to prove the identity of the aggregator or kube-apiserver "+
		"when it must call out during a request. This includes proxying requests to a user "+
		"api-server and calling out to webhook admission plugins.")

	fs.StringVar(&s.PeerCAFile, "peer-ca-file", s.PeerCAFile,
		"If set and the UnknownVersionInteroperabilityProxy feature gate is enabled, this file will be used to verify serving certificates of peer kube-apiservers. "+
			"This flag is only used in clusters configured with multiple kube-apiservers for high availability.")

	fs.StringVar(&s.PeerAdvertiseAddress.PeerAdvertiseIP, "peer-advertise-ip", s.PeerAdvertiseAddress.PeerAdvertiseIP,
		"If set and the UnknownVersionInteroperabilityProxy feature gate is enabled, this IP will be used by peer kube-apiservers to proxy requests to this kube-apiserver "+
			"when the request cannot be handled by the peer due to version skew between the kube-apiservers. "+
			"This flag is only used in clusters configured with multiple kube-apiservers for high availability. ")

	fs.StringVar(&s.PeerAdvertiseAddress.PeerAdvertisePort, "peer-advertise-port", s.PeerAdvertiseAddress.PeerAdvertisePort,
		"If set and the UnknownVersionInteroperabilityProxy feature gate is enabled, this port will be used by peer kube-apiservers to proxy requests to this kube-apiserver "+
			"when the request cannot be handled by the peer due to version skew between the kube-apiservers. "+
			"This flag is only used in clusters configured with multiple kube-apiservers for high availability. ")

	fs.BoolVar(&s.EnableAggregatorRouting, "enable-aggregator-routing", s.EnableAggregatorRouting,
		"Turns on aggregator routing requests to endpoints IP rather than cluster IP.")

	fs.BoolVar(&s.AggregatorRejectForwardingRedirects, "aggregator-reject-forwarding-redirect", s.AggregatorRejectForwardingRedirects,
		"Aggregator reject forwarding redirect response back to client.")

	fs.StringVar(&s.ServiceAccountSigningKeyFile, "service-account-signing-key-file", s.ServiceAccountSigningKeyFile, ""+
		"Path to the file that contains the current private key of the service account token issuer. The issuer will sign issued ID tokens with this private key.")

	fs.StringVar(&s.ServiceAccountSigningEndpoint, "service-account-signing-endpoint", s.ServiceAccountSigningEndpoint, ""+
		"Path to socket where a external JWT signer is listening. This flag is mutually exclusive with --service-account-signing-key-file and --service-account-key-file. Requires enabling feature gate (ExternalServiceAccountTokenSigner)")

	fs.DurationVar(&s.CoordinatedLeadershipLeaseDuration, "coordinated-leadership-lease-duration", s.CoordinatedLeadershipLeaseDuration,
		"The duration of the lease used for Coordinated Leader Election.")
	fs.DurationVar(&s.CoordinatedLeadershipRenewDeadline, "coordinated-leadership-renew-deadline", s.CoordinatedLeadershipRenewDeadline,
		"The deadline for renewing a coordinated leader election lease.")
	fs.DurationVar(&s.CoordinatedLeadershipRetryPeriod, "coordinated-leadership-retry-period", s.CoordinatedLeadershipRetryPeriod,
		"The period for retrying to renew a coordinated leader election lease.")
}

// Complete 将一个原始的 Options 对象转换为一个已完成的 CompletedOptions 对象。
// 它会填充默认值、生成证书、处理配置间的依赖关系等。
//
// alternateDNS 和 alternateIPs: 用于在生成自签名证书时，额外添加的 DNS 名称和 IP 地址。
func (o *Options) Complete(ctx context.Context, alternateDNS []string, alternateIPs []net.IP) (CompletedOptions, error) {
	if o == nil {
		return CompletedOptions{completedOptions: &completedOptions{}}, nil
	}
	// --- 1. 初始化和基础配置 ---

	// 防御性编程：如果原始 options 是 nil，返回一个空的、无害的 CompletedOptions。
	completed := completedOptions{
		Options: *o,
	}
	// 创建一个内部的 completedOptions 实例，并将原始 Options 的值复制过来。
	// 后续所有操作都将在这个 `completed` 变量上进行。
	// 这一步会处理一些非常基础的服务器选项，但在这里它通常是个空操作或非常简单的默认值填充。

	if err := completed.GenericServerRunOptions.Complete(); err != nil {
		return CompletedOptions{}, err
	}

	// set defaults
	// --- 2. 网络和证书配置 ---

	// 为服务器设置默认的广播地址 (AdvertiseAddress)。
	// 如果用户没有明确指定 `--advertise-address`，这个函数会尝试从 `--bind-address` 推断出一个合适的地址。
	// 广播地址是 apiserver 告诉其他组件“你应该来这里找我”的地址。
	if err := completed.GenericServerRunOptions.DefaultAdvertiseAddress(completed.SecureServing.SecureServingOptions); err != nil {
		return CompletedOptions{}, err
	}

	// 尝试处理 HTTPS 服务证书。
	// `MaybeDefaultWithSelfSignedCerts` 的逻辑是：
	// 1. 如果用户已经通过 `--tls-cert-file` 和 `--tls-private-key-file` 提供了证书。
	// 2. 如果没有，它会尝试在指定的证书目录中自动生成一个自签名的证书。
	//    这个自签名证书会包含服务器的广播地址、主机名以及传入的 alternateDNS/IPs。
	if err := completed.SecureServing.MaybeDefaultWithSelfSignedCerts(completed.GenericServerRunOptions.AdvertiseAddress.String(), alternateDNS, alternateIPs); err != nil {
		return CompletedOptions{}, fmt.Errorf("error creating self-signed certificates: %v", err)
	}
	// --- 3. 动态调整 Etcd 配置 ---

	// 这是一个非常精妙的性能优化。
	if o.GenericServerRunOptions.RequestTimeout > 0 {
		// Setting the EventsHistoryWindow as a maximum of the value set in the
		// watchcache-specific options and the value of the request timeout plus
		// some epsilon.
		// This is done to make sure that the list+watch pattern can still be
		// usable in large clusters with the elevated request timeout where the
		// initial list can take a considerable amount of time.
		// 将 Etcd 的“事件历史窗口”（EventsHistoryWindow）大小设置为 `RequestTimeout` 加上一个缓冲时间（15秒）和
		// 原有 `EventsHistoryWindow` 值中的较大者。
		//
		// 为什么这么做？
		// 在大集群中，客户端（如 kube-controller-manager）通过 list+watch 模式同步数据时，
		// 初始的 list 请求可能会因为数据量大而耗时很长（可能接近 RequestTimeout）。
		// 在 list 结束到 watch 开始的这段时间里，apiserver 必须在 etcd 中保留这段时间的事件历史，
		// 否则 watch 会因为“历史记录跟不上”而失败。
		// 这里动态地将事件历史窗口调大，就是为了保证 list+watch 模式的健壮性。
		completed.Etcd.StorageConfig.EventsHistoryWindow = max(completed.Etcd.StorageConfig.EventsHistoryWindow, completed.GenericServerRunOptions.RequestTimeout+15*time.Second)
	}
	// --- 4. 确定 ExternalHost ---

	// `ExternalHost` 是生成 API discovery 文档时使用的主机名。
	// 如果用户没有通过 `--external-hostname` 明确指定，就需要自动推断一个。
	if len(completed.GenericServerRunOptions.ExternalHost) == 0 {
		if len(completed.GenericServerRunOptions.AdvertiseAddress) > 0 {
			// 优先使用广播地址。
			completed.GenericServerRunOptions.ExternalHost = completed.GenericServerRunOptions.AdvertiseAddress.String()
		} else {
			// 如果连广播地址也没有，就使用本机的 hostname 作为最后的备用方案。
			hostname, err := os.Hostname()
			if err != nil {
				return CompletedOptions{}, fmt.Errorf("error finding host name: %v", err)
			}
			completed.GenericServerRunOptions.ExternalHost = hostname
		}
		klog.Infof("external host was not specified, using %v", completed.GenericServerRunOptions.ExternalHost)
	}
	// --- 5. 完成认证和授权配置 ---

	// 将授权 (Authorization) 相关的选项设置到最终状态。
	// 比如，它会根据用户的配置（如 `--authorization-mode`）创建一个授权器链。
	// put authorization options in final state
	completed.Authorization.Complete()
	// adjust authentication for completed authorization
	// 调整认证 (Authentication) 配置以适应已完成的授权配置。
	// 例如，如果 Webhook 授权器被启用，认证配置需要知道如何创建用于调用该 Webhook 的客户端。
	completed.Authentication.ApplyAuthorization(completed.Authorization)
	// --- 6. 完成 ServiceAccount 相关配置 ---

	// 调用一个辅助函数来完成 ServiceAccount 认证相关的选项。
	// 比如，加载 service-account-key-file 等。
	err := o.completeServiceAccountOptions(ctx, &completed)
	if err != nil {
		return CompletedOptions{}, err
	}
	// --- 7. 标准化 API 启用配置 ---

	// `RuntimeConfig` 用于控制启用或禁用哪些 API 版本（如 "apps/v1"）。
	// 这段代码是为了向后兼容，将一些旧的、不规范的 key (如 "v1", "api/v1")
	// 统一转换成标准的、带斜杠前缀的格式 (如 "/v1")。
	for key, value := range completed.APIEnablement.RuntimeConfig {
		if key == "v1" || strings.HasPrefix(key, "v1/") ||
			key == "api/v1" || strings.HasPrefix(key, "api/v1/") {
			delete(completed.APIEnablement.RuntimeConfig, key)
			completed.APIEnablement.RuntimeConfig["/v1"] = value
		}
		if key == "api/legacy" {
			delete(completed.APIEnablement.RuntimeConfig, key)
		}
	}
	// --- 8. 返回最终结果 ---

	// 将内部的 `completed` 实例包装在 `CompletedOptions` 中并返回。
	return CompletedOptions{
		completedOptions: &completed,
	}, nil
}

// completeServiceAccountOptions 是 Options.Complete() 的一个辅助函数，
// 专门负责完成与 ServiceAccount Token 签发相关的配置。
func (o *Options) completeServiceAccountOptions(ctx context.Context, completed *completedOptions) error {
	// --- 1. 定义警告信息模板 ---
	// 这两条警告信息用于后续处理 Bound Token 自动续期相关的配置。
	transitionWarningFmt := "service-account-extend-token-expiration is true, in order to correctly trigger safe transition logic, service-account-max-token-expiration must be set longer than %d seconds (currently %s)"
	expExtensionWarningFmt := "service-account-extend-token-expiration is true, enabling tokens valid up to %d seconds, which is longer than service-account-max-token-expiration set to %s"
	// verify service-account-max-token-expiration
	// --- 2. 校验 --service-account-max-token-expiration ---
	// `MaxExpiration` 是用户设置的、允许签发出的 ServiceAccount Token 的最长有效期。
	if completed.Authentication.ServiceAccounts.MaxExpiration != 0 { // 如果用户设置了这个值
		lowBound := time.Hour
		upBound := time.Duration(1<<32) * time.Second
		if completed.Authentication.ServiceAccounts.MaxExpiration < lowBound ||
			completed.Authentication.ServiceAccounts.MaxExpiration > upBound {
			return fmt.Errorf("the service-account-max-token-expiration must be between 1 hour and 2^32 seconds")
		}
	}
	// --- 3. 初始化 ServiceAccountIssuer (核心逻辑) ---
	// `ServiceAccountIssuer` 是一个接口，负责实际的 Token 签发工作。
	// 它有两种实现：本地签发 或 外部签发。

	// 检查用户是否通过 `--service-account-issuer` 提供了签发者标识。这是签发 Token 的前提。
	if len(completed.Authentication.ServiceAccounts.Issuers) != 0 && completed.Authentication.ServiceAccounts.Issuers[0] != "" {
		// `switch` 语句判断用户选择了哪种签发方式。
		switch {
		// 情况一：用户同时配置了本地私钥文件和外部签名端点，这是冲突的。
		case completed.ServiceAccountSigningEndpoint != "" && completed.ServiceAccountSigningKeyFile != "":
			return fmt.Errorf("service-account-signing-key-file and service-account-signing-endpoint are mutually exclusive and cannot be set at the same time")
			// 情况二：用户配置了本地私key文件 (`--service-account-signing-key-file`)。
		case completed.ServiceAccountSigningKeyFile != "":
			sk, err := keyutil.PrivateKeyFromFile(completed.ServiceAccountSigningKeyFile)
			if err != nil {
				return fmt.Errorf("failed to parse service-account-issuer-key-file: %w", err)
			}
			// 创建一个本地的 JWT Token 生成器 (`JWTTokenGenerator`) 作为 `ServiceAccountIssuer`。
			completed.ServiceAccountIssuer, err = serviceaccount.JWTTokenGenerator(completed.Authentication.ServiceAccounts.Issuers[0], sk)
			if err != nil {
				return fmt.Errorf("failed to build token generator: %w", err)
			}
			// 情况三：用户配置了外部签名端点 (`--service-account-signing-endpoint`)。
		case completed.ServiceAccountSigningEndpoint != "":
			// 创建一个与外部签名服务通信的插件客户端。这个插件实现了 `ServiceAccountIssuer` 接口。
			// `cache` 用于缓存从外部服务获取的公钥。
			plugin, cache, err := plugin.New(ctx, completed.Authentication.ServiceAccounts.Issuers[0], completed.ServiceAccountSigningEndpoint, 60*time.Second, false)
			if err != nil {
				return fmt.Errorf("while setting up external-jwt-signer: %w", err)
			}
			// 联系外部签名服务，获取其元数据（如它支持的最长 Token 有效期）。
			timedContext, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			metadata, err := plugin.GetServiceMetadata(timedContext)
			if err != nil {
				return fmt.Errorf("while setting up external-jwt-signer: %w", err)
			}
			// 校验外部签名服务的能力。
			if metadata.MaxTokenExpirationSeconds < validation.MinTokenAgeSec {
				return fmt.Errorf("max token life supported by external-jwt-signer (%ds) is less than acceptable (min %ds)", metadata.MaxTokenExpirationSeconds, validation.MinTokenAgeSec)
			}
			// 根据外部签名服务的能力，调整或验证 apiserver 的 `MaxExpiration` 配置。
			maxExternalExpiration := time.Duration(metadata.MaxTokenExpirationSeconds) * time.Second
			switch {
			// 如果 apiserver 没有设置 MaxExpiration，就使用外部服务支持的最大值作为默认值。
			case completed.Authentication.ServiceAccounts.MaxExpiration == 0:
				completed.Authentication.ServiceAccounts.MaxExpiration = maxExternalExpiration
				// 如果 apiserver 设置的 MaxExpiration 比外部服务支持的还长，这是不允许的。
			case completed.Authentication.ServiceAccounts.MaxExpiration > maxExternalExpiration:
				return fmt.Errorf("service-account-max-token-expiration cannot be set longer than the token expiration supported by service-account-signing-endpoint: %s > %s", completed.Authentication.ServiceAccounts.MaxExpiration, maxExternalExpiration)
			}
			// 针对外部签名服务的场景，更新警告信息模板。
			transitionWarningFmt = "service-account-extend-token-expiration is true, in order to correctly trigger safe transition logic, token lifetime supported by external-jwt-signer must be longer than %d seconds (currently %s)"
			expExtensionWarningFmt = "service-account-extend-token-expiration is true, tokens validity will be caped at the smaller of %d seconds and maximum token lifetime supported by external-jwt-signer (%s)"
			// 将配置好的插件和缓存赋值给 completed 选项。
			completed.ServiceAccountIssuer = plugin
			completed.Authentication.ServiceAccounts.ExternalPublicKeysGetter = cache
			// shorten ExtendedExpiration, if needed, to fit within the external signer's max expiration
			// `MaxExtendedExpiration` 是一个用于平滑迁移的更长的有效期。
			// 如果它超过了外部签名服务的能力，就将其缩短到外部服务支持的最大值。
			completed.Authentication.ServiceAccounts.MaxExtendedExpiration = min(maxExternalExpiration, completed.Authentication.ServiceAccounts.MaxExtendedExpiration)
		}
	}
	// --- 4. 处理并警告与 Token 自动续期相关的配置 ---
	// `--service-account-extend-token-expiration` 用于平滑迁移，允许在一段时间内签发有效期更长的 Token。
	// Set Max expiration and warn on conflicting configuration.
	if completed.Authentication.ServiceAccounts.ExtendExpiration && completed.Authentication.ServiceAccounts.MaxExpiration != 0 {
		// 检查 `MaxExpiration` 是否足够长，以支持平滑过渡。如果不够长，就发出警告。
		if completed.Authentication.ServiceAccounts.MaxExpiration < serviceaccount.WarnOnlyBoundTokenExpirationSeconds*time.Second {
			klog.Warningf(transitionWarningFmt, serviceaccount.WarnOnlyBoundTokenExpirationSeconds, completed.Authentication.ServiceAccounts.MaxExpiration)
		}
		// 检查 `MaxExpiration` 是否比续期后的有效期还短。如果是，说明续期功能实际上被限制了，发出警告。
		if completed.Authentication.ServiceAccounts.MaxExpiration < serviceaccount.ExpirationExtensionSeconds*time.Second {
			klog.Warningf(expExtensionWarningFmt, serviceaccount.ExpirationExtensionSeconds, completed.Authentication.ServiceAccounts.MaxExpiration)
		}
	}
	completed.ServiceAccountTokenMaxExpiration = completed.Authentication.ServiceAccounts.MaxExpiration

	return nil
}

// ServiceIPRange checks if the serviceClusterIPRange flag is nil, raising a warning if so and
// setting service ip range to the default value in kubeoptions.DefaultServiceIPCIDR
// for now until the default is removed per the deprecation timeline guidelines.
// Returns service ip range, api server service IP, and an error
func ServiceIPRange(passedServiceClusterIPRange net.IPNet) (net.IPNet, net.IP, error) {
	serviceClusterIPRange := passedServiceClusterIPRange
	if passedServiceClusterIPRange.IP == nil {
		klog.Warningf("No CIDR for service cluster IPs specified. Default value which was %s is deprecated and will be removed in future releases. Please specify it using --service-cluster-ip-range on kube-apiserver.", kubeoptions.DefaultServiceIPCIDR.String())
		serviceClusterIPRange = kubeoptions.DefaultServiceIPCIDR
	}

	size := min(netutil.RangeSize(&serviceClusterIPRange), 1<<16)
	if size < 8 {
		return net.IPNet{}, net.IP{}, fmt.Errorf("the service cluster IP range must be at least %d IP addresses", 8)
	}

	// Select the first valid IP from ServiceClusterIPRange to use as the GenericAPIServer service IP.
	apiServerServiceIP, err := netutil.GetIndexedIP(&serviceClusterIPRange, 1)
	if err != nil {
		return net.IPNet{}, net.IP{}, err
	}
	klog.V(4).Infof("Setting service IP to %q (read-write).", apiServerServiceIP)

	return serviceClusterIPRange, apiServerServiceIP, nil
}
