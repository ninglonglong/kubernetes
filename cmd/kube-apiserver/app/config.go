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

package app

import (
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/klog/v2"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"

	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"
)

type Config struct {
	Options options.CompletedOptions

	Aggregator    *aggregatorapiserver.Config
	KubeAPIs      *controlplane.Config
	ApiExtensions *apiextensionsapiserver.Config

	ExtraConfig
}

type ExtraConfig struct {
}

type completedConfig struct {
	Options options.CompletedOptions

	Aggregator    aggregatorapiserver.CompletedConfig
	KubeAPIs      controlplane.CompletedConfig
	ApiExtensions apiextensionsapiserver.CompletedConfig

	ExtraConfig
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

func (c *Config) Complete() (CompletedConfig, error) {
	return CompletedConfig{&completedConfig{
		Options: c.Options,

		Aggregator:    c.Aggregator.Complete(),
		KubeAPIs:      c.KubeAPIs.Complete(),
		ApiExtensions: c.ApiExtensions.Complete(),

		ExtraConfig: c.ExtraConfig,
	}}, nil
}

// NewConfig creates all the resources for running kube-apiserver, but runs none of them.
func NewConfig(opts options.CompletedOptions) (*Config, error) {
	// 记录函数入口，V(4) 级别用于详细调试。
	klog.V(4).InfoS("Entering NewConfig to create server configuration from completed options")

	// 步骤 1: 初始化 Config 对象
	// c 是最终要返回的总配置对象，它将聚合 apiserver 的所有组件配置。
	c := &Config{
		Options: opts,
	}
	klog.V(4).InfoS("Initialized the main Config object")

	// 步骤 2: 构建通用的 GenericAPIServer 配置 (BuildGenericConfig)
	// 这是整个 apiserver 的基础。它负责设置认证、授权、安全端口、etcd 连接、审计等
	// 所有 apiserver（包括 kube-apiserver, apiextensions-apiserver, aggregator-apiserver）
	// 都需要的基础设施。
	klog.InfoS("Building generic apiserver configuration")
	// `BuildGenericConfig` 接受：
	// - opts.CompletedOptions: 已经补全的通用配置。
	// - []*runtime.Scheme: 一个 Scheme 列表，用于告诉 apiserver 如何序列化/反序列化
	//   各种 API 对象 (例如，内置的 Core API, apiextensions API, aggregator API)。
	// - DefaultAPIResourceConfigSource(): 定义了默认启用的 API 资源组和版本（例如 v1, apps/v1）。
	// - GetOpenAPIDefinitions: 一个函数，用于生成 OpenAPI (Swagger) 文档。
	genericConfig, versionedInformers, storageFactory, err := controlplaneapiserver.BuildGenericConfig(
		opts.CompletedOptions,
		[]*runtime.Scheme{legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme},
		controlplane.DefaultAPIResourceConfigSource(),
		generatedopenapi.GetOpenAPIDefinitions,
	)
	if err != nil {
		return nil, err
	}

	klog.InfoS("Generic apiserver configuration built successfully")
	// 步骤 3: 创建核心 KubeAPIs 的配置 (CreateKubeAPIServerConfig)
	// 这一步是专门为 "kube-apiserver" 自身服务的。它负责处理 Kubernetes 的核心 API 资源
	// (如 Pods, Deployments, Services 等)。
	klog.InfoS("Creating KubeAPIs configuration")
	// `CreateKubeAPIServerConfig` 会：
	// - 基于 `genericConfig` 进行扩展。
	// - 使用 `storageFactory` 为所有核心 API 资源（如 Pods, Deployments）创建到 etcd 的存储后端。
	// - 配置 ServiceAccount 相关的逻辑。
	// - 初始化 `serviceResolver`，用于解析 Service ClusterIP。
	// - 初始化 `pluginInitializer`，用于后续注入依赖到 admission 插件。
	kubeAPIs, serviceResolver, pluginInitializer, err := CreateKubeAPIServerConfig(opts, genericConfig, versionedInformers, storageFactory)
	if err != nil {
		return nil, err
	}
	// 将创建好的 KubeAPIs 配置存入总配置对象 `c` 中。
	c.KubeAPIs = kubeAPIs
	klog.InfoS("KubeAPIs configuration created successfully")
	// 步骤 4: 创建 APIExtensions 服务器的配置 (CreateAPIExtensionsConfig)
	// 这个组件负责处理 CustomResourceDefinitions (CRDs)。它本身也是一个迷你的 apiserver，
	// 运行在 kube-apiserver 的进程内，但处理的是 apiextensions.k8s.io 组的 API。
	klog.InfoS("Creating APIExtensions configuration")
	// `CreateAPIExtensionsConfig` 会：
	// - 复用 `genericConfig` 作为基础。
	// - 配置 CRD 的存储、验证、版本转换等逻辑。
	// - 设置与 CRD 相关的 admission 插件。
	// - 配置 webhook 认证解析器，用于安全地调用 CRD 的 conversion webhooks。
	apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*kubeAPIs.ControlPlane.Generic, kubeAPIs.ControlPlane.VersionedInformers, pluginInitializer, opts.CompletedOptions, opts.MasterCount,
		serviceResolver, webhook.NewDefaultAuthenticationInfoResolverWrapper(kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Generic.EgressSelector, kubeAPIs.ControlPlane.Generic.LoopbackClientConfig, kubeAPIs.ControlPlane.Generic.TracerProvider))
	if err != nil {
		return nil, err
	}
	// 将创建好的 ApiExtensions 配置存入总配置对象 `c` 中。
	c.ApiExtensions = apiExtensions
	klog.InfoS("APIExtensions configuration created successfully")

	// 步骤 5: 创建 Aggregator 服务器的配置 (CreateAggregatorConfig)
	// 这个组件负责处理 APIService 资源，它实现了 "API 聚合层"。
	// 当你请求一个由其他 apiserver (如 metrics-server) 提供的 API 时，
	// Aggregator 会将请求代理到那个外部服务。
	// `CreateAggregatorConfig` 会：
	// - 同样复用 `genericConfig` 作为基础。
	// - 配置 APIService 的发现和代理逻辑。
	// - `ProxyTransport` 是用于安全地将请求代理到其他服务的 HTTP transport。
	aggregator, err := controlplaneapiserver.CreateAggregatorConfig(*kubeAPIs.ControlPlane.Generic, opts.CompletedOptions, kubeAPIs.ControlPlane.VersionedInformers, serviceResolver, kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Extra.PeerProxy, pluginInitializer)
	if err != nil {
		return nil, err
	}
	c.Aggregator = aggregator

	return c, nil
}
