/*
Copyright 2014 The Kubernetes Authors.

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

// Package app does all of the work necessary to create a Kubernetes
// APIServer by binding together the API, master and APIServer infrastructure.
// It can be configured and called directly or via the hyperkube framework.
package app

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/notfoundhandler"
	"k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/apiserver/pkg/util/webhook"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
	"k8s.io/component-base/term"
	utilversion "k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/component-base/zpages/flagz"
	"k8s.io/klog/v2"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
	kubeapiserveradmission "k8s.io/kubernetes/pkg/kubeapiserver/admission"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// NewAPIServerCommand creates a *cobra.Command object with default parameters
func NewAPIServerCommand() *cobra.Command {
	s := options.NewServerRunOptions()
	ctx := genericapiserver.SetupSignalContext()
	featureGate := s.GenericServerRunOptions.ComponentGlobalsRegistry.FeatureGateFor(basecompatibility.DefaultKubeComponent)

	cmd := &cobra.Command{
		Use: "kube-apiserver",
		Long: `The Kubernetes API server validates and configures data
for the api objects which include pods, services, replicationcontrollers, and
others. The API Server services REST operations and provides the frontend to the
cluster's shared state through which all other components interact.`,

		// stop printing usage when the command errors
		SilenceUsage: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if err := s.GenericServerRunOptions.ComponentGlobalsRegistry.Set(); err != nil {
				return err
			}
			// silence client-go warnings.
			// kube-apiserver loopback clients should not log self-issued warnings.
			rest.SetDefaultWarningHandler(rest.NoWarnings{})
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()
			fs := cmd.Flags()
			// Activate logging as soon as possible, after that
			// show flags with the final logging configuration.
			if err := logsapi.ValidateAndApply(s.Logs, featureGate); err != nil {
				return err
			}
			cliflag.PrintFlags(fs)

			// set default options
			completedOptions, err := s.Complete(ctx)
			if err != nil {
				return err
			}

			// validate options
			if errs := completedOptions.Validate(); len(errs) != 0 {
				return utilerrors.NewAggregate(errs)
			}
			// add feature enablement metrics
			featureGate.(featuregate.MutableFeatureGate).AddMetrics()
			// add component version metrics
			s.GenericServerRunOptions.ComponentGlobalsRegistry.AddMetrics()
			return Run(ctx, completedOptions)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}
	cmd.SetContext(ctx)

	fs := cmd.Flags()
	namedFlagSets := s.Flags()
	s.Flagz = flagz.NamedFlagSetsReader{
		FlagSets: namedFlagSets,
	}
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, namedFlagSets, cols)

	return cmd
}

// Run runs the specified APIServer.  This should never exit.
func Run(ctx context.Context, opts options.CompletedOptions) error {
	// To help debugging, immediately log version
	klog.Infof("Version: %+v", utilversion.Get())
	klog.Infof("ningminglong pakage docker ningminglongxxxxx: %+v", utilversion.Get())

	klog.InfoS("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	config, err := NewConfig(opts)
	if err != nil {
		return err
	}
	completed, err := config.Complete()
	if err != nil {
		return err
	}
	server, err := CreateServerChain(completed)
	if err != nil {
		return err
	}

	prepared, err := server.PrepareRun()
	if err != nil {
		return err
	}

	return prepared.Run(ctx)
}

// CreateServerChain creates the apiservers connected via delegation.
// CreateServerChain 函数的作用是: 根据已经完成的完整配置(CompletedConfig)，
// 实例化并链接 apiserver 的所有组件，形成一个完整的 HTTP 请求处理链。
// 这个链条的最终形态是一个 `APIAggregator` 服务器，它作为整个 apiserver 的入口。
// 请求会像穿过一个“责任链”一样，依次经过这个链条上的服务器。
func CreateServerChain(config CompletedConfig) (*aggregatorapiserver.APIAggregator, error) {
	klog.InfoS("Creating server chain")

	// ------------------------------------------------------------------
	//  链条的起点: Not Found Handler
	// ------------------------------------------------------------------
	// 创建一个特殊的 404 Handler。如果一个请求穿过了整个服务器链条都没有被任何一个组件处理，
	// 最终就会由这个 Handler 来响应一个 404 Not Found 错误。
	klog.V(4).InfoS("Creating not-found handler")
	notFoundHandler := notfoundhandler.New(config.KubeAPIs.ControlPlane.Generic.Serializer, genericapifilters.NoMuxAndDiscoveryIncompleteKey)
	// ------------------------------------------------------------------
	//  第一环: APIExtensions Server
	// ------------------------------------------------------------------
	// `APIExtensions` 服务器负责处理 CRD (CustomResourceDefinition) 相关的 API，
	// 即 `/apis/apiextensions.k8s.io/...` 的请求。
	klog.InfoS("Creating APIExtensions server")
	// 这里是关键的“链接”操作:
	// `New()` 方法会创建一个 `APIExtensions` 服务器实例。
	// 它接受一个 `delegate` (委托) 参数。`delegate` 的意思是：“如果这个请求不归我管，我就把它交给你处理”。
	// 在这里，`APIExtensions` 的委托是 `notFoundHandler`。
	// 所以，如果一个请求不是针对 CRD 的，`APIExtensions` 就会把它扔给 `notFoundHandler`。
	apiExtensionsServer, err := config.ApiExtensions.New(genericapiserver.NewEmptyDelegateWithCustomHandler(notFoundHandler))
	if err != nil {
		return nil, err
	}
	// 检查 CRD API 本身是否被启用。这个信息后续会传递给 Aggregator Server。
	crdAPIEnabled := config.ApiExtensions.GenericConfig.MergedResourceConfig.ResourceEnabled(apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions"))
	// ------------------------------------------------------------------
	//  第二环: KubeAPIs Server (核心服务器)
	// ------------------------------------------------------------------
	// `KubeAPIs` 服务器是 `kube-apiserver` 的心脏，负责处理所有内置的核心 API，
	// 例如 `/api/v1/pods`, `/apis/apps/v1/deployments` 等。
	klog.InfoS("Creating KubeAPIs server")
	// 再次进行“链接”操作:
	// `KubeAPIs` 的委托是 `apiExtensionsServer.GenericAPIServer`。
	// 这意味着：
	// 1. 请求首先到达 `KubeAPIs` 服务器。
	// 2. 如果请求是针对核心 API 的 (e.g., /api/v1/pods)，`KubeAPIs` 自己处理。
	// 3. 如果请求不是核心 API，`KubeAPIs` 就会把它委托给 `APIExtensions` 服务器。
	// 4. `APIExtensions` 服务器检查这个请求是不是针对 CRD 的。如果是，它就处理；如果不是，它再委托给 `notFoundHandler`。
	kubeAPIServer, err := config.KubeAPIs.New(apiExtensionsServer.GenericAPIServer)
	if err != nil {
		return nil, err
	}

	// aggregator comes last in the chain

	// ------------------------------------------------------------------
	//  第三环 (也是最后一环/入口): Aggregator Server
	// ------------------------------------------------------------------
	// `Aggregator` 服务器是整个请求链的入口。它负责处理 `APIService` 相关的 API，
	// 并将来自外部 API (如 `metrics-server`) 的请求代理出去。
	klog.InfoS("Creating Aggregator server")
	// `CreateAggregatorServer` 函数创建 `Aggregator` 服务器实例。
	// 它的委托是 `kubeAPIServer.ControlPlane.GenericAPIServer`。
	// 这就完成了整个链条的构建！
	aggregatorServer, err := controlplaneapiserver.CreateAggregatorServer(config.Aggregator, // Aggregator 自己的配置
		kubeAPIServer.ControlPlane.GenericAPIServer,                                    // 它的委托对象: KubeAPIs 服务器
		apiExtensionsServer.Informers.Apiextensions().V1().CustomResourceDefinitions(), // 用于发现 CRD 的 Informer
		crdAPIEnabled,        // CRD API 是否启用
		apiVersionPriorities) // API 发现时的优先级
	if err != nil {
		// we don't need special handling for innerStopCh because the aggregator server doesn't create any go routines
		return nil, err
	}
	// ------------------------------------------------------------------
	//  返回链条的头部
	// ------------------------------------------------------------------
	klog.InfoS("Server chain created successfully")
	// 函数返回 `aggregatorServer`，它现在是整个 HTTP 处理链的“头”。
	// 所有进入 `kube-apiserver` 的请求都将首先由它来处理。
	return aggregatorServer, nil
}

// CreateKubeAPIServerConfig creates all the resources for running the API server, but runs none of them
func CreateKubeAPIServerConfig(
	opts options.CompletedOptions,
	genericConfig *genericapiserver.Config,
	versionedInformers clientgoinformers.SharedInformerFactory,
	storageFactory *serverstorage.DefaultStorageFactory,
) (
	*controlplane.Config, // 返回的核心 API 配置
	aggregatorapiserver.ServiceResolver, // 一个服务解析器，用于 API 聚合
	[]admission.PluginInitializer, // 一系列准入插件初始化器
	error,
) {
	klog.V(2).InfoS("Creating kube-apiserver specific configuration")
	// global stuff
	// 步骤 1: 全局能力设置 (Capabilities)
	// capabilities.Setup 是一个全局函数，用于设置进程级别的能力。
	// - opts.AllowPrivileged: 是否允许创建特权容器。这会影响到 PodSecurity admission 插件的行为。
	// - opts.MaxConnectionBytesPerSec: 如果设置了，用于限制 Pod 日志等流式 API 的带宽。
	klog.V(4).InfoS("Setting up global capabilities", "allowPrivileged", opts.AllowPrivileged, "maxConnectionBytesPerSec", opts.MaxConnectionBytesPerSec)
	capabilities.Setup(opts.AllowPrivileged, opts.MaxConnectionBytesPerSec)
	// additional admission initializers
	// 步骤 2: 创建 Kubernetes 特有的准入插件初始化器 (Admission Initializers)
	// `kubeAdmissionConfig.New()` 会返回一个 `PluginInitializer` 列表。
	// `PluginInitializer` 是一个非常重要的接口，它的作用是向准入插件“注入”它们需要的依赖，
	// 例如 `client-go` 客户端、`InformerFactory`、`Authorizer` 等。
	// 这样可以避免准入插件直接访问全局变量，实现了依赖解耦。
	klog.V(4).InfoS("Creating additional kube-specific admission plugin initializers")
	kubeAdmissionConfig := &kubeapiserveradmission.Config{}
	kubeInitializers, err := kubeAdmissionConfig.New()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create admission plugin initializer: %w", err)
	}
	// 步骤 3: 构建服务解析器 (Service Resolver)
	// `ServiceResolver` 的作用是告诉 API Aggregator 如何找到一个由 `APIService` 资源指向的后端服务。
	// - 如果 `EnableAggregatorRouting` 为 true，它会创建一个特殊的解析器，允许请求被直接代理到 Service 的 ClusterIP。
	// - 否则，它会返回一个 nil 解析器，Aggregator 将无法通过 Service 名称代理请求。
	klog.V(2).InfoS("Building service resolver for aggregator")
	serviceResolver, err := buildServiceResolver(opts.EnableAggregatorRouting, genericConfig.LoopbackClientConfig.Host, versionedInformers)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error building service resolver: %w", err)
	}
	// 步骤 4: 创建 controlplane 通用配置 (A Layer of Abstraction)
	// 这里的 `controlplaneapiserver.CreateConfig` 是一个帮助函数，它封装了为任何“控制平面”类型 apiserver
	// 创建配置的通用逻辑。它会：
	// - 安装所有核心 API 资源的 REST Storage (e.g., `/api/v1/pods`, `/apis/apps/v1/deployments`)。
	// - 将这些 REST Storage 注册到 `genericConfig` 的 Handler 中。
	// - 配置并初始化所有启用的准入插件。
	klog.V(4).InfoS("Creating generic controlplane configuration")
	controlplaneConfig, admissionInitializers, err := controlplaneapiserver.CreateConfig(opts.CompletedOptions, genericConfig, versionedInformers, storageFactory, serviceResolver, kubeInitializers)
	if err != nil {
		return nil, nil, nil, err
	}

	// 步骤 5: 组装最终的、包含了 "Extra" 配置的 `controlplane.Config`
	// `controlplane.Config` 是一个聚合结构体，它不仅包含了通用的 `controlplaneConfig`，
	// 还增加了很多 `kube-apiserver` 独有的配置字段。
	klog.V(4).InfoS("Assembling final controlplane config with extra kube-specific settings")
	config := &controlplane.Config{
		// 嵌入上一步创建的通用控制平面配置。
		ControlPlane: *controlplaneConfig,
		// `Extra` 字段包含了所有不属于通用 apiserver 模型，但 `kube-apiserver` 运行时又必需的配置。
		Extra: controlplane.Extra{
			// Kubelet 客户端配置，用于 apiserver 连接 kubelet（例如，执行 `kubectl exec/logs`）。
			KubeletClientConfig: opts.KubeletConfig,
			// Service 相关的网络配置。
			ServiceIPRange:          opts.PrimaryServiceClusterIPRange,   // Service ClusterIP 的主范围
			APIServerServiceIP:      opts.APIServerServiceIP,             // `kubernetes.default.svc` 的 IP
			SecondaryServiceIPRange: opts.SecondaryServiceClusterIPRange, // Service ClusterIP 的次范围 (用于双栈网络)

			APIServerServicePort: 443, // `kubernetes.default.svc` 的端口

			ServiceNodePortRange:      opts.ServiceNodePortRange,
			KubernetesServiceNodePort: opts.KubernetesServiceNodePort, // 如果 `kubernetes` service 是 NodePort 类型，它的端口号
			// Endpoints 控制器的类型 (e.g., "lease", "endpointslicemirroring")。
			EndpointReconcilerType: reconcilers.Type(opts.EndpointReconcilerType),
			// 控制平面节点的数量，用于选举等。
			MasterCount: opts.MasterCount,
		},
	}
	// 步骤 6: 为出站连接应用网络代理 (Egress Selector)
	// 如果配置了 Egress Selector (网络出口选择器)，需要确保 apiserver 的出站连接
	// (如连接 kubelet, 或执行 proxy subresource) 都遵循这个网络策略。
	if config.ControlPlane.Generic.EgressSelector != nil {
		klog.V(2).InfoS("Applying egress selector to kubelet client and proxy transport")
		// Use the config.ControlPlane.Generic.EgressSelector lookup to find the dialer to connect to the kubelet
		// 为 Kubelet 客户端配置 `Lookup` 函数，使其在建立连接前查询 Egress Selector。
		config.Extra.KubeletClientConfig.Lookup = config.ControlPlane.Generic.EgressSelector.Lookup

		// Use the config.ControlPlane.Generic.EgressSelector lookup as the transport used by the "proxy" subresources.
		// 为 "proxy" 子资源（如 `.../proxy`）使用的 HTTP Transport 配置一个自定义的 Dialer。
		// 这确保了 `kubectl proxy <pod>` 这样的命令也能遵循网络出口策略。
		networkContext := egressselector.Cluster.AsNetworkContext()
		dialer, err := config.ControlPlane.Generic.EgressSelector.Lookup(networkContext)
		if err != nil {
			return nil, nil, nil, err
		}
		// 必须 Clone()，避免修改共享的原始 ProxyTransport。
		c := config.ControlPlane.Extra.ProxyTransport.Clone()
		c.DialContext = dialer
		config.ControlPlane.ProxyTransport = c
	}
	// 步骤 7: 返回所有创建好的配置和共享资源
	klog.InfoS("Kube-apiserver specific configuration created successfully")
	return config, serviceResolver, admissionInitializers, nil
}

var testServiceResolver webhook.ServiceResolver

// SetServiceResolverForTests allows the service resolver to be overridden during tests.
// Tests using this function must run serially as this function is not safe to call concurrently with server start.
func SetServiceResolverForTests(resolver webhook.ServiceResolver) func() {
	if testServiceResolver != nil {
		panic("test service resolver is set: tests are either running concurrently or clean up was skipped")
	}

	testServiceResolver = resolver

	return func() {
		testServiceResolver = nil
	}
}

func buildServiceResolver(enabledAggregatorRouting bool, hostname string, informer clientgoinformers.SharedInformerFactory) (webhook.ServiceResolver, error) {
	if testServiceResolver != nil {
		return testServiceResolver, nil
	}

	endpointSliceGetter, err := proxy.NewEndpointSliceIndexerGetter(informer.Discovery().V1().EndpointSlices())
	if err != nil {
		return nil, err
	}

	var serviceResolver webhook.ServiceResolver
	if enabledAggregatorRouting {
		serviceResolver = aggregatorapiserver.NewEndpointServiceResolver(
			informer.Core().V1().Services().Lister(),
			endpointSliceGetter,
		)
	} else {
		serviceResolver = aggregatorapiserver.NewClusterIPServiceResolver(
			informer.Core().V1().Services().Lister(),
		)
	}

	// resolve kubernetes.default.svc locally
	if localHost, err := url.Parse(hostname); err == nil {
		serviceResolver = aggregatorapiserver.NewLoopbackServiceResolver(serviceResolver, localHost)
	}
	return serviceResolver, nil
}
