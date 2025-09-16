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

package options

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	basecompatibility "k8s.io/component-base/compatibility"

	"github.com/spf13/pflag"
)

const (
	corsAllowedOriginsHelpText = "List of allowed origins for CORS, comma separated. " +
		"An allowed origin can be a regular expression to support subdomain matching. " +
		"If this list is empty CORS will not be enabled. " +
		"Please ensure each expression matches the entire hostname by anchoring " +
		"to the start with '^' or including the '//' prefix, and by anchoring to the " +
		"end with '$' or including the ':' port separator suffix. " +
		"Examples of valid expressions are '//example\\.com(:|$)' and '^https://example\\.com(:|$)'"
)

// ServerRunOptions contains the options while running a generic api server.
type ServerRunOptions struct {
	AdvertiseAddress net.IP

	CorsAllowedOriginList        []string
	HSTSDirectives               []string
	ExternalHost                 string
	MaxRequestsInFlight          int
	MaxMutatingRequestsInFlight  int
	RequestTimeout               time.Duration
	GoawayChance                 float64
	LivezGracePeriod             time.Duration
	MinRequestTimeout            int
	StorageInitializationTimeout time.Duration
	ShutdownDelayDuration        time.Duration
	// We intentionally did not add a flag for this option. Users of the
	// apiserver library can wire it to a flag.
	JSONPatchMaxCopyBytes int64
	// The limit on the request body size that would be accepted and
	// decoded in a write request. 0 means no limit.
	// We intentionally did not add a flag for this option. Users of the
	// apiserver library can wire it to a flag.
	MaxRequestBodyBytes int64

	// ShutdownSendRetryAfter dictates when to initiate shutdown of the HTTP
	// Server during the graceful termination of the apiserver. If true, we wait
	// for non longrunning requests in flight to be drained and then initiate a
	// shutdown of the HTTP Server. If false, we initiate a shutdown of the HTTP
	// Server as soon as ShutdownDelayDuration has elapsed.
	// If enabled, after ShutdownDelayDuration elapses, any incoming request is
	// rejected with a 429 status code and a 'Retry-After' response.
	ShutdownSendRetryAfter bool

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

	// ComponentGlobalsRegistry is the registry where the effective versions and feature gates for all components are stored.
	ComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry
	// ComponentName is name under which the server's global variabled are registered in the ComponentGlobalsRegistry.
	ComponentName string
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
}

// NewServerRunOptions 创建一个新的 ServerRunOptions 对象。
// 这个函数是一个简化的构造函数，它在内部处理了组件版本和特性门控的全局注册，
// 然后委托给一个更具体的构造函数来完成实际的创建工作。
func NewServerRunOptions() *ServerRunOptions {
	// 这个 if 语句是一个关键的“初始化一次”逻辑。
	// 它检查“默认Kube组件”的版本信息是否已经在全局注册表中注册过。
	// 如果 EffectiveVersionFor 返回 nil，意味着尚未注册，就需要执行注册操作。
	// 这样做可以确保在同一个进程中，无论这个函数被调用多少次，注册操作都只执行一次，避免了竞态条件。
	if compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent) == nil {
		// 获取默认的可变特性门控集合。这是一个全局变量，包含了所有已知的特性开关。
		featureGate := utilfeature.DefaultMutableFeatureGate
		// 获取在编译时确定的组件“有效版本”。这个版本信息通常来自 Git 标签，
		// 它决定了组件在兼容性方面的行为表现。
		effectiveVersion := compatibility.DefaultBuildEffectiveVersion()
		// 必须成功地将组件、其有效版本和特性门控注册到全局注册表中。
		// 如果注册失败（例如，因为重复注册），utilruntime.Must 会导致程序 panic，
		// 因为这是一个启动时的关键设置，失败意味着程序无法正常运行。
		utilruntime.Must(compatibility.DefaultComponentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, featureGate))
	}
	// 在确保全局信息已注册后，调用一个更具体的构造函数来创建 ServerRunOptions。
	// 它传递了组件标识符和全局注册表，以便 ServerRunOptions 知道自己属于哪个组件，
	// 并能从中查询版本和特性信息。
	return NewServerRunOptionsForComponent(basecompatibility.DefaultKubeComponent, compatibility.DefaultComponentGlobalsRegistry)
}

// NewServerRunOptionsForComponent 是一个构造函数，它为一个特定的组件创建 ServerRunOptions，并填充了一组推荐的默认值。
// componentName: 当前组件的名称，例如 "kube-apiserver" 或 "kube-scheduler"。
// componentGlobalsRegistry: 一个全局注册表，包含了组件的版本和特性门控信息。
func NewServerRunOptionsForComponent(componentName string, componentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry) *ServerRunOptions {
	// NewServerRunOptionsForComponent 是一个构造函数，它为一个特定的组件创建 ServerRunOptions，并填充了一组推荐的默认值。
	// componentName: 当前组件的名称，例如 "kube-apiserver" 或 "kube-scheduler"。
	// componentGlobalsRegistry: 一个全局注册表，包含了组件的版本和特性门控信息。
	defaults := server.NewConfig(serializer.CodecFactory{})
	// 返回一个新的 ServerRunOptions 结构体指针。
	// 结构体的字段被初始化为从 'defaults' 对象中获取的推荐值。
	return &ServerRunOptions{
		// 服务器可以同时处理的最大非变更性请求数。默认为 400。
		MaxRequestsInFlight: defaults.MaxRequestsInFlight,
		// 服务器可以同时处理的最大变更性请求数（如创建、更新、删除）。默认为 200。
		MaxMutatingRequestsInFlight: defaults.MaxMutatingRequestsInFlight,
		// 单个请求的默认超时时间。默认为 60 秒。
		RequestTimeout: defaults.RequestTimeout,
		// /livez 健康检查在启动时的宽限期。默认为 0，表示没有宽限期。
		LivezGracePeriod: defaults.LivezGracePeriod,
		// 用户可以通过请求参数指定的最小超时时间（秒）。默认为 1800 秒（30分钟）。
		MinRequestTimeout: defaults.MinRequestTimeout,
		// 存储层（etcd）初始化的超时时间。默认为 1 分钟。
		StorageInitializationTimeout: defaults.StorageInitializationTimeout,
		// 优雅关闭前的延迟时间。默认为 0。对于 kube-apiserver，这个值通常会被覆盖为一个非零值（如 1 秒）。
		ShutdownDelayDuration: defaults.ShutdownDelayDuration,
		// 在优雅关闭期间，为终止 watch 请求留出的宽限时间。默认为 0。
		ShutdownWatchTerminationGracePeriod: defaults.ShutdownWatchTerminationGracePeriod,
		// 处理 JSON Patch 请求时允许复制的最大字节数。默认为 32 KB。
		JSONPatchMaxCopyBytes: defaults.JSONPatchMaxCopyBytes,
		// 请求体的最大字节数。默认为 3 MB。
		MaxRequestBodyBytes: defaults.MaxRequestBodyBytes,
		// 在关闭期间，是否发送 "Retry-After" HTTP 响应头。默认为 false。、
		ShutdownSendRetryAfter: false,
		// 记录下当前组件的名称。这个名称将在日志和指标中用作标识。
		ComponentName: componentName,
		// 保存对全局注册表的引用。这样，在后续的配置过程中，
		// ServerRunOptions 就可以根据组件的有效版本（EffectiveVersion）来启用或禁用某些特性，
		// 实现版本兼容性控制
		ComponentGlobalsRegistry: componentGlobalsRegistry,
	}
}

// ApplyTo applies the run options to the method receiver and returns self
func (s *ServerRunOptions) ApplyTo(c *server.Config) error {
	if err := s.ComponentGlobalsRegistry.SetFallback(); err != nil {
		return err
	}
	c.CorsAllowedOriginList = s.CorsAllowedOriginList
	c.HSTSDirectives = s.HSTSDirectives
	c.ExternalAddress = s.ExternalHost
	c.MaxRequestsInFlight = s.MaxRequestsInFlight
	c.MaxMutatingRequestsInFlight = s.MaxMutatingRequestsInFlight
	c.LivezGracePeriod = s.LivezGracePeriod
	c.RequestTimeout = s.RequestTimeout
	c.GoawayChance = s.GoawayChance
	c.MinRequestTimeout = s.MinRequestTimeout
	c.StorageInitializationTimeout = s.StorageInitializationTimeout
	c.ShutdownDelayDuration = s.ShutdownDelayDuration
	c.JSONPatchMaxCopyBytes = s.JSONPatchMaxCopyBytes
	c.MaxRequestBodyBytes = s.MaxRequestBodyBytes
	c.PublicAddress = s.AdvertiseAddress
	c.ShutdownSendRetryAfter = s.ShutdownSendRetryAfter
	c.ShutdownWatchTerminationGracePeriod = s.ShutdownWatchTerminationGracePeriod
	c.EffectiveVersion = s.ComponentGlobalsRegistry.EffectiveVersionFor(s.ComponentName)
	c.FeatureGate = s.ComponentGlobalsRegistry.FeatureGateFor(s.ComponentName)
	c.EmulationForwardCompatible = s.EmulationForwardCompatible
	c.RuntimeConfigEmulationForwardCompatible = s.RuntimeConfigEmulationForwardCompatible

	return nil
}

// DefaultAdvertiseAddress sets the field AdvertiseAddress if unset. The field will be set based on the SecureServingOptions.
// DefaultAdvertiseAddress 如果 AdvertiseAddress 字段未被设置，则为其设置一个默认值。
// 这个默认值是根据 SecureServingOptions（安全服务配置）来推断的。
func (s *ServerRunOptions) DefaultAdvertiseAddress(secure *SecureServingOptions) error {
	if secure == nil {
		return nil
	}
	// 检查 AdvertiseAddress 是否需要被设置。
	// 满足以下任一条件时，需要进行设置：
	// 1. s.AdvertiseAddress == nil: 用户完全没有通过命令行 `--advertise-address` 提供这个值。
	// 2. s.AdvertiseAddress.IsUnspecified(): 用户提供的值是 "0.0.0.0"，这是一个“未指定”的地址，
	//    不能作为广播地址，因为它对于集群中的其他组件来说没有意义。
	if s.AdvertiseAddress == nil || s.AdvertiseAddress.IsUnspecified() {
		// 调用 secure.DefaultExternalAddress() 来获取一个合适的外部地址。
		// 这个辅助函数的逻辑通常是：
		// 1. 检查 `secure.BindAddress`。如果它是一个具体的、非 "0.0.0.0" 的 IP 地址，就直接返回它。
		// 2. 如果 `secure.BindAddress` 是 "0.0.0.0"，则尝试遍历本机所有的网络接口，
		//    找到一个最合适的、可路由的默认 IP 地址（通常是分配在 `eth0` 等主网卡上的地址）。
		hostIP, err := secure.DefaultExternalAddress()
		if err != nil {
			// 如果 `DefaultExternalAddress` 找不到任何合适的 IP 地址（例如，机器没有任何配置好的网络接口），
			// 就会返回一个错误。
			// 错误信息提示用户直接设置 AdvertiseAddress，或者提供一个有效的 BindAddress 来帮助推断。
			return fmt.Errorf("Unable to find suitable network address.error='%v'. "+
				"Try to set the AdvertiseAddress directly or provide a valid BindAddress to fix this.", err)
		}
		// 将找到的 IP 地址赋值给 AdvertiseAddress。

		s.AdvertiseAddress = hostIP
	}

	return nil
}

// Validate checks validation of ServerRunOptions
func (s *ServerRunOptions) Validate() []error {
	errors := []error{}

	if s.LivezGracePeriod < 0 {
		errors = append(errors, fmt.Errorf("--livez-grace-period can not be a negative value"))
	}

	if s.MaxRequestsInFlight < 0 {
		errors = append(errors, fmt.Errorf("--max-requests-inflight can not be negative value"))
	}
	if s.MaxMutatingRequestsInFlight < 0 {
		errors = append(errors, fmt.Errorf("--max-mutating-requests-inflight can not be negative value"))
	}

	if s.RequestTimeout.Nanoseconds() < 0 {
		errors = append(errors, fmt.Errorf("--request-timeout can not be negative value"))
	}

	if s.GoawayChance < 0 || s.GoawayChance > 0.02 {
		errors = append(errors, fmt.Errorf("--goaway-chance can not be less than 0 or greater than 0.02"))
	}

	if s.MinRequestTimeout < 0 {
		errors = append(errors, fmt.Errorf("--min-request-timeout can not be negative value"))
	}

	if s.StorageInitializationTimeout < 0 {
		errors = append(errors, fmt.Errorf("--storage-initialization-timeout can not be negative value"))
	}

	if s.ShutdownDelayDuration < 0 {
		errors = append(errors, fmt.Errorf("--shutdown-delay-duration can not be negative value"))
	}

	if s.ShutdownWatchTerminationGracePeriod < 0 {
		errors = append(errors, fmt.Errorf("shutdown-watch-termination-grace-period, if provided, can not be a negative value"))
	}

	if s.JSONPatchMaxCopyBytes < 0 {
		errors = append(errors, fmt.Errorf("ServerRunOptions.JSONPatchMaxCopyBytes can not be negative value"))
	}

	if s.MaxRequestBodyBytes < 0 {
		errors = append(errors, fmt.Errorf("ServerRunOptions.MaxRequestBodyBytes can not be negative value"))
	}

	if err := validateHSTSDirectives(s.HSTSDirectives); err != nil {
		errors = append(errors, err)
	}

	if err := validateCorsAllowedOriginList(s.CorsAllowedOriginList); err != nil {
		errors = append(errors, err)
	}
	if errs := s.ComponentGlobalsRegistry.Validate(); len(errs) != 0 {
		errors = append(errors, errs...)
	}
	effectiveVersion := s.ComponentGlobalsRegistry.EffectiveVersionFor(s.ComponentName)
	if effectiveVersion == nil {
		return errors
	}
	notEmulationMode := effectiveVersion.BinaryVersion().WithPatch(0).EqualTo(effectiveVersion.EmulationVersion())
	if notEmulationMode && s.EmulationForwardCompatible {
		errors = append(errors, fmt.Errorf("ServerRunOptions.EmulationForwardCompatible cannot be set to true if the emulation version is the same as the binary version"))
	}
	if notEmulationMode && s.RuntimeConfigEmulationForwardCompatible {
		errors = append(errors, fmt.Errorf("ServerRunOptions.RuntimeConfigEmulationForwardCompatible cannot be set to true if the emulation version is the same as the binary version"))
	}
	return errors
}

func validateHSTSDirectives(hstsDirectives []string) error {
	// HSTS Headers format: Strict-Transport-Security:max-age=expireTime [;includeSubDomains] [;preload]
	// See https://tools.ietf.org/html/rfc6797#section-6.1 for more information
	allErrors := []error{}
	for _, hstsDirective := range hstsDirectives {
		if len(strings.TrimSpace(hstsDirective)) == 0 {
			allErrors = append(allErrors, fmt.Errorf("empty value in strict-transport-security-directives"))
			continue
		}
		if hstsDirective != "includeSubDomains" && hstsDirective != "preload" {
			maxAgeDirective := strings.Split(hstsDirective, "=")
			if len(maxAgeDirective) != 2 || maxAgeDirective[0] != "max-age" {
				allErrors = append(allErrors, fmt.Errorf("--strict-transport-security-directives invalid, allowed values: max-age=expireTime, includeSubDomains, preload. see https://tools.ietf.org/html/rfc6797#section-6.1 for more information"))
			}
		}
	}
	return errors.NewAggregate(allErrors)
}

func validateCorsAllowedOriginList(corsAllowedOriginList []string) error {
	allErrors := []error{}
	validateRegexFn := func(regexpStr string) error {
		if _, err := regexp.Compile(regexpStr); err != nil {
			return err
		}

		// the regular expression should pin to the start and end of the host
		// in the origin header, this will prevent CVE-2022-1996.
		// possible ways it can pin to the start of host in the origin header:
		//   - match the start of the origin with '^'
		//   - match what separates the scheme and host with '//' or '://',
		//     this pins to the start of host in the origin header.
		// possible ways it can match the end of the host in the origin header:
		//   - match the end of the origin with '$'
		//   - with a capture group that matches the host and port separator '(:|$)'
		// We will relax the validation to check if these regex markers
		// are present in the user specified expression.
		var pinStart, pinEnd bool
		for _, prefix := range []string{"^", "//"} {
			if strings.Contains(regexpStr, prefix) {
				pinStart = true
				break
			}
		}
		for _, suffix := range []string{"$", ":"} {
			if strings.Contains(regexpStr, suffix) {
				pinEnd = true
				break
			}
		}
		if !pinStart || !pinEnd {
			return fmt.Errorf("regular expression does not pin to start/end of host in the origin header")
		}
		return nil
	}

	for _, regexp := range corsAllowedOriginList {
		if len(regexp) == 0 {
			allErrors = append(allErrors, fmt.Errorf("empty value in --cors-allowed-origins, help: %s", corsAllowedOriginsHelpText))
			continue
		}

		if err := validateRegexFn(regexp); err != nil {
			err = fmt.Errorf("--cors-allowed-origins has an invalid regular expression: %v, help: %s", err, corsAllowedOriginsHelpText)
			allErrors = append(allErrors, err)
		}
	}
	return errors.NewAggregate(allErrors)
}

// AddUniversalFlags adds flags for a specific APIServer to the specified FlagSet
func (s *ServerRunOptions) AddUniversalFlags(fs *pflag.FlagSet) {
	// Note: the weird ""+ in below lines seems to be the only way to get gofmt to
	// arrange these text blocks sensibly. Grrr.

	fs.IPVar(&s.AdvertiseAddress, "advertise-address", s.AdvertiseAddress, ""+
		"The IP address on which to advertise the apiserver to members of the cluster. This "+
		"address must be reachable by the rest of the cluster. If blank, the --bind-address "+
		"will be used. If --bind-address is unspecified, the host's default interface will "+
		"be used.")

	fs.StringSliceVar(&s.CorsAllowedOriginList, "cors-allowed-origins", s.CorsAllowedOriginList, corsAllowedOriginsHelpText)

	fs.StringSliceVar(&s.HSTSDirectives, "strict-transport-security-directives", s.HSTSDirectives, ""+
		"List of directives for HSTS, comma separated. If this list is empty, then HSTS directives will not "+
		"be added. Example: 'max-age=31536000,includeSubDomains,preload'")

	fs.StringVar(&s.ExternalHost, "external-hostname", s.ExternalHost,
		"The hostname to use when generating externalized URLs for this master (e.g. Swagger API Docs or OpenID Discovery).")

	fs.IntVar(&s.MaxRequestsInFlight, "max-requests-inflight", s.MaxRequestsInFlight, ""+
		"This and --max-mutating-requests-inflight are summed to determine the server's total concurrency limit "+
		"(which must be positive) if --enable-priority-and-fairness is true. "+
		"Otherwise, this flag limits the maximum number of non-mutating requests in flight, "+
		"or a zero value disables the limit completely.")

	fs.IntVar(&s.MaxMutatingRequestsInFlight, "max-mutating-requests-inflight", s.MaxMutatingRequestsInFlight, ""+
		"This and --max-requests-inflight are summed to determine the server's total concurrency limit "+
		"(which must be positive) if --enable-priority-and-fairness is true. "+
		"Otherwise, this flag limits the maximum number of mutating requests in flight, "+
		"or a zero value disables the limit completely.")

	fs.DurationVar(&s.RequestTimeout, "request-timeout", s.RequestTimeout, ""+
		"An optional field indicating the duration a handler must keep a request open before timing "+
		"it out. This is the default request timeout for requests but may be overridden by flags such as "+
		"--min-request-timeout for specific types of requests.")

	fs.Float64Var(&s.GoawayChance, "goaway-chance", s.GoawayChance, ""+
		"To prevent HTTP/2 clients from getting stuck on a single apiserver, randomly close a connection (GOAWAY). "+
		"The client's other in-flight requests won't be affected, and the client will reconnect, likely landing on a different apiserver after going through the load balancer again. "+
		"This argument sets the fraction of requests that will be sent a GOAWAY. Clusters with single apiservers, or which don't use a load balancer, should NOT enable this. "+
		"Min is 0 (off), Max is .02 (1/50 requests); .001 (1/1000) is a recommended starting point.")

	fs.DurationVar(&s.LivezGracePeriod, "livez-grace-period", s.LivezGracePeriod, ""+
		"This option represents the maximum amount of time it should take for apiserver to complete its startup sequence "+
		"and become live. From apiserver's start time to when this amount of time has elapsed, /livez will assume "+
		"that unfinished post-start hooks will complete successfully and therefore return true.")

	fs.IntVar(&s.MinRequestTimeout, "min-request-timeout", s.MinRequestTimeout, ""+
		"An optional field indicating the minimum number of seconds a handler must keep "+
		"a request open before timing it out. Currently only honored by the watch request "+
		"handler, which picks a randomized value above this number as the connection timeout, "+
		"to spread out load.")

	fs.DurationVar(&s.StorageInitializationTimeout, "storage-initialization-timeout", s.StorageInitializationTimeout,
		"Maximum amount of time to wait for storage initialization before declaring apiserver ready. Defaults to 1m.")

	fs.DurationVar(&s.ShutdownDelayDuration, "shutdown-delay-duration", s.ShutdownDelayDuration, ""+
		"Time to delay the termination. During that time the server keeps serving requests normally. The endpoints /healthz and /livez "+
		"will return success, but /readyz immediately returns failure. Graceful termination starts after this delay "+
		"has elapsed. This can be used to allow load balancer to stop sending traffic to this server.")

	fs.BoolVar(&s.ShutdownSendRetryAfter, "shutdown-send-retry-after", s.ShutdownSendRetryAfter, ""+
		"If true the HTTP Server will continue listening until all non long running request(s) in flight have been drained, "+
		"during this window all incoming requests will be rejected with a status code 429 and a 'Retry-After' response header, "+
		"in addition 'Connection: close' response header is set in order to tear down the TCP connection when idle.")

	fs.DurationVar(&s.ShutdownWatchTerminationGracePeriod, "shutdown-watch-termination-grace-period", s.ShutdownWatchTerminationGracePeriod, ""+
		"This option, if set, represents the maximum amount of grace period the apiserver will wait "+
		"for active watch request(s) to drain during the graceful server shutdown window.")

	s.ComponentGlobalsRegistry.AddFlags(fs)
	fs.BoolVar(&s.EmulationForwardCompatible, "emulation-forward-compatible", s.EmulationForwardCompatible, ""+
		"If true, for any beta+ APIs enabled by default or by --runtime-config at the emulation version, their future versions with higher priority/stability will be auto enabled even if they introduced after the emulation version. "+
		"Can only be set to true if the emulation version is lower than the binary version.")
	fs.BoolVar(&s.RuntimeConfigEmulationForwardCompatible, "runtime-config-emulation-forward-compatible", s.RuntimeConfigEmulationForwardCompatible, ""+
		"If true, APIs identified by group/version that are enabled in the --runtime-config flag will be installed even if it is introduced after the emulation version. "+
		"If false, server would fail to start if any APIs identified by group/version that are enabled in the --runtime-config flag are introduced after the emulation version. "+
		"Can only be set to true if the emulation version is lower than the binary version.")
}

// Complete fills missing fields with defaults.
func (s *ServerRunOptions) Complete() error {
	return s.ComponentGlobalsRegistry.SetFallback()
}
