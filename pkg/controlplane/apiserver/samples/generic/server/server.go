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

package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	_ "k8s.io/apiserver/pkg/admission"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	genericapiserver "k8s.io/apiserver/pkg/server"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/notfoundhandler"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
	"k8s.io/component-base/term"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"

	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	_ "k8s.io/kubernetes/pkg/features"
	// add the kubernetes feature gates
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// NewCommand creates a *cobra.Command object with default parameters
func NewCommand() *cobra.Command {
	s := NewOptions()
	cmd := &cobra.Command{
		Use: "sample-generic-apiserver",
		Long: `The sample generic apiserver is part of a generic controlplane,
a system serving APIs like Kubernetes, but without the container domain specific
APIs.`,

		// stop printing usage when the command errors
		SilenceUsage: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			// silence client-go warnings.
			// kube-apiserver loopback clients should not log self-issued warnings.
			rest.SetDefaultWarningHandler(rest.NoWarnings{})
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// 检查是否请求了版本信息。如果用户加了 `--version` 参数，
			// 这个函数会打印版本信息并立即退出程序。
			verflag.PrintAndExitIfRequested()
			fs := cmd.Flags()

			// Activate logging as soon as possible, after that
			// show flags with the final logging configuration.
			// 激活日志系统。这是启动流程中最先做的几件事之一，
			// 以确保后续的所有操作（包括打印 flag）都能被正确地记录下来。
			// `ValidateAndApply` 会验证日志配置，并根据配置（如日志级别、格式等）应用到全局 klog。
			if err := logsapi.ValidateAndApply(s.Logs, utilfeature.DefaultFeatureGate); err != nil {
				return err
			}
			cliflag.PrintFlags(fs)
			// 设置信号处理上下文。`SetupSignalContext()` 会创建一个 context.Context，
			// 这个 context 会在程序接收到 SIGINT (Ctrl+C) 或 SIGTERM 信号时被取消 (canceled)。
			// 整个应用程序都应该监听这个 context 的取消事件，以实现优雅关闭。
			ctx := genericapiserver.SetupSignalContext()
			// --- 2. 配置的完成与校验 ---

			// 调用 Options 对象的 Complete 方法。
			// 这个方法是配置流程的关键一步，它会：
			// 1. 读取命令行参数和配置文件。
			// 2. 填充所有未设置的字段的默认值。
			// 3. 进行一些需要运行时信息的初始化（比如加载证书文件）。
			// 最终生成一个“已完成”的配置对象 `completedOptions`。
			// 这里的 `[]string{}` 和 `[]net.IP{}` 是用于某些特殊场景的参数，通常为空。
			completedOptions, err := s.Complete(ctx, []string{}, []net.IP{})
			if err != nil {
				return err
			}

			// 调用已完成配置的 Validate 方法。
			// 这个方法会检查所有配置项的合法性，例如：
			// - 两个相互冲突的参数是否被同时设置。
			// - 必须的参数是否被遗漏。
			// - 文件路径是否存在。
			// 它返回一个错误列表，因为可能存在多个配置问题。
			if errs := completedOptions.Validate(); len(errs) != 0 {
				return utilerrors.NewAggregate(errs)
			}

			// add feature enablement metrics
			// 添加特性门控的监控指标。
			// 这会创建一个 Prometheus 指标，用来暴露当前启用了哪些特性门控（Feature Gate）及其阶段（Alpha, Beta, GA）。
			utilfeature.DefaultMutableFeatureGate.AddMetrics()

			// --- 4. 启动服务器 ---

			// 调用真正的服务器启动函数 `Run`，并将“已完成并已验证”的配置和
			// 带有信号处理的上下文传递给它。
			// `Run` 函数会阻塞，直到服务器关闭。
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

	var namedFlagSets cliflag.NamedFlagSets
	s.AddFlags(&namedFlagSets)
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())

	fs := cmd.Flags()
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, namedFlagSets, cols)

	return cmd
}

func NewOptions() *options.Options {
	// 首先，调用标准的构造函数，获取一个包含了所有 kube-apiserver 推荐默认值的 Options 对象。
	// 这为我们提供了一个完整的、功能齐全的配置作为起点。
	s := options.NewOptions()
	// --- 定制化修改 1：覆盖默认关闭的插件列表 ---
	// 将 AdmissionOptions 中的 DefaultOffPlugins 字段设置为我们自己定义的列表。
	// 这里的 `DefaultOffAdmissionPlugins()` 可能是这个包里定义的，也可能来自其他地方。
	// 这个操作允许这个“最小化”的 apiserver 拥有与标准 apiserver 不同的默认插件安全策略。
	s.Admission.GenericAdmission.DefaultOffPlugins = DefaultOffAdmissionPlugins()
	// --- 定制化修改 2：修改服务器证书的存储路径 ---
	// 获取当前的工作目录。
	wd, _ := os.Getwd()
	// 将安全服务（HTTPS）的证书目录修改为当前工作目录下的一个子目录 ".sample-minimal-controlplane"。
	// 标准的 apiserver 通常使用 /var/run/kubernetes 或由 kubeadm 指定的路径。
	// 这个修改表明，我们希望服务器在一个本地的、临时的、可控的目录中生成或查找其证书，
	// 这非常适合测试和本地开发场景，避免了对系统目录的写入权限要求。
	s.SecureServing.ServerCert.CertDirectory = filepath.Join(wd, ".sample-minimal-controlplane")
	// --- 定制化修改 3：“连接”一个特殊的 ServiceAccount Token 获取方式 ---
	// 这是最关键的修改。它为 ServiceAccount 认证提供了一个“可选的” token 获取器。
	// 在标准的 Kubernetes 中，ServiceAccount token 的验证通常需要查询相关的 Pod 和 Secret 对象。
	// 但在这个“最小化”的环境中，可能根本没有 Pod 或 Node 资源。
	// 通过设置 OptionalTokenGetter，我们为认证系统提供了一个“后门”或“备用方案”：
	// “如果你通过常规方式找不到 token 对应的 Pod，请尝试调用我（genericTokenGetter）来直接获取 token 的 Secret”。
	// 这使得 ServiceAccount 认证可以在一个没有 Pod 的环境中工作。
	// Wire ServiceAccount authentication without relying on pods and nodes.
	s.Authentication.ServiceAccounts.OptionalTokenGetter = genericTokenGetter

	return s
}

// Run runs the specified APIServer. This should never exit.
func Run(ctx context.Context, opts options.CompletedOptions) error {
	// To help debugging, immediately log version
	klog.Infof("Version: %+v", version.Get())

	klog.InfoS("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))
	// --- 2. 创建服务器配置和实例 (核心构建流程) ---

	// `NewConfig(opts)`:
	// 这是一个关键的转换步骤。它接收命令行选项 `opts`，并将其转换为一个更底层的、
	// 用于构建 `genericapiserver` 的 `Config` 对象。
	// 这个 `Config` 对象包含了所有创建 apiserver 所需的细节，如认证、授权、准入控制链、存储后端等。
	config, err := NewConfig(opts)
	if err != nil {
		return err
	}

	// `config.Complete()`:
	// 调用这个底层 `Config` 对象的 `Complete` 方法。这会进一步填充一些在 `NewConfig` 阶段
	// 无法确定的默认值或内部依赖。
	completed, err := config.Complete()
	if err != nil {
		return err
	}
	// `CreateServerChain(completed)`:
	// 根据已完成的 `completed` 配置，创建实际的服务器实例链。
	// “链” (Chain) 是因为 kube-apiserver 实际上可能由多个 apiserver 实例聚合而成：
	// 1. 一个核心的 `kube-apiserver`，提供 Kubernetes 的核心 API (如 /api/v1)。
	// 2. 一个 `aggregator-apiserver`，用于代理对扩展 API (APIService) 的请求。
	// 这个函数负责将它们正确地组装起来。
	server, err := CreateServerChain(completed)
	if err != nil {
		return err
	}
	// --- 3. 准备并运行服务器 ---

	// `server.PrepareRun()`:
	// 在服务器正式启动（开始监听端口）之前，执行一系列的准备工作。
	// 这可能包括：
	// - 启动一些必须先于 API 服务运行的后台控制器（Post-start hooks）。
	// - 初始化健康检查和就绪检查的端点。
	// - 设置监听端口。
	// 它返回一个 `PreparedRun` 对象，这个对象代表一个“准备就绪，可以随时启动”的服务器。
	prepared, err := server.PrepareRun()
	if err != nil {
		return err
	}
	// `prepared.Run(ctx)`:
	// 这是最终的启动命令。它会：
	// 1. 开始在配置的端口上监听网络请求。
	// 2. 启动所有配置的后台任务和控制器。
	// 3. 阻塞在这里，持续运行，直到传入的 `ctx` (上下文) 被取消。
	//    `ctx` 通常与操作系统的 SIGINT/SIGTERM 信号关联，当用户按下 Ctrl+C 或系统发送关闭信号时，
	//    `ctx` 会被取消，`Run` 函数会开始执行优雅关闭流程，然后返回。
	return prepared.Run(ctx)
}

// CreateServerChain creates the apiservers connected via delegation.
func CreateServerChain(config CompletedConfig) (*aggregatorapiserver.APIAggregator, error) {
	// 1. CRDs
	// --- 1. 创建 APIExtensions Server (处理 CRD) ---
	// 这是链条的第一环，专门负责 CustomResourceDefinition (CRD) 相关的 API。
	// 它必须首先被创建，因为后续的核心 API 服务器可能会依赖于它。

	// 创建一个 "404 Not Found" 处理器。当一个请求没有被任何已注册的 API 路径匹配时，
	// 这个处理器会被调用。`NoMuxAndDiscoveryIncompleteKey` 是一个特殊的上下文键，
	// 用于在这种情况下发出一个警告，提示可能是因为服务发现信息尚未就绪。
	notFoundHandler := notfoundhandler.New(config.ControlPlane.Generic.Serializer, genericapifilters.NoMuxAndDiscoveryIncompleteKey)
	// 创建 APIExtensions Server 实例。
	// `NewEmptyDelegateWithCustomHandler(notFoundHandler)` 创建了一个“空的委托”。
	// 这意味着，如果 APIExtensions Server 自己无法处理某个请求，它不会将请求传递给下一环，
	// 而是直接使用我们提供的 `notFoundHandler` 来响应 404。
	apiExtensionsServer, err := config.APIExtensions.New(genericapiserver.NewEmptyDelegateWithCustomHandler(notFoundHandler))
	if err != nil {
		return nil, fmt.Errorf("failed to create apiextensions-apiserver: %w", err)
	}
	// 检查 CRD API (`customresourcedefinitions`) 是否被启用了。
	// 这个布尔值后续会传递给聚合服务器，用于服务发现。
	crdAPIEnabled := config.APIExtensions.GenericConfig.MergedResourceConfig.ResourceEnabled(apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions"))

	// 2. Natively implemented resources
	// --- 2. 创建 Native API Server (核心 Kubernetes API) ---
	// 这是链条的第二环，负责所有 Kubernetes 内置的、原生的 API 资源，如 Pods, Services, Deployments 等。

	// 创建核心 API 服务器实例（这里命名为 "sample-generic-controlplane"，但实际上是主 apiserver）。
	// 关键一步：将 `apiExtensionsServer.GenericAPIServer` 作为它的“委托”（Delegate）。
	// 这意味着，如果核心 API 服务器无法处理某个请求（例如，请求的是一个 CRD），
	// 它会将这个请求【委托】给上一环的 `apiExtensionsServer` 去处理。这就形成了“链条”。
	nativeAPIs, err := config.ControlPlane.New("sample-generic-controlplane", apiExtensionsServer.GenericAPIServer)
	if err != nil {
		return nil, fmt.Errorf("failed to create generic controlplane apiserver: %w", err)
	}
	// 创建一个 loopback kubernetes 客户端，用于后续获取存储提供者。
	client, err := kubernetes.NewForConfig(config.ControlPlane.Generic.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	// 获取所有内置资源的存储提供者（Storage Providers）。
	// 这些提供者封装了对 etcd 的读写操作。
	storageProviders, err := config.ControlPlane.GenericStorageProviders(client.Discovery())

	if err != nil {
		return nil, fmt.Errorf("failed to create storage providers: %w", err)
	}
	// 将这些存储提供者“安装”到核心 API 服务器中。
	// 这一步会将所有内置资源的 REST API 端点（如 /api/v1/pods）注册到服务器的 HTTP Mux 中。
	if err := nativeAPIs.InstallAPIs(storageProviders...); err != nil {
		return nil, fmt.Errorf("failed to install APIs: %w", err)
	}

	// 3. Aggregator for APIServices, discovery and OpenAPI
	// --- 3. 创建 Aggregator Server (聚合层) ---
	// 这是链条的最顶端，也是最终的入口。它负责处理：
	// - 对扩展 API (APIService) 的请求代理。
	// - 统一的服务发现 (/apis)。
	// - 统一的 OpenAPI 规范。

	// 创建聚合服务器实例。
	// 关键一步：将 `nativeAPIs.GenericAPIServer` 作为它的“委托”（Delegate）。
	// 这意味着，如果聚合服务器自己无法处理某个请求（例如，请求的不是一个 APIService），
	// 它会将请求【委托】给上一环的 `nativeAPIs` 服务器去处理。
	// `apiExtensionsServer.Informers...` 等参数用于帮助聚合器了解 CRD 的状态，以便正确生成服务发现信息。
	aggregatorServer, err := controlplaneapiserver.CreateAggregatorServer(config.Aggregator, nativeAPIs.GenericAPIServer, apiExtensionsServer.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdAPIEnabled, controlplaneapiserver.DefaultGenericAPIServicePriorities())
	if err != nil {
		// we don't need special handling for innerStopCh because the aggregator server doesn't create any go routines
		return nil, fmt.Errorf("failed to create kube-aggregator: %w", err)
	}

	return aggregatorServer, nil
}
