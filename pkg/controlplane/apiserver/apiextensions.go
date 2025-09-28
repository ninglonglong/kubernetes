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
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apiextensionsoptions "k8s.io/apiextensions-apiserver/pkg/cmd/server/options"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/informers"

	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
)

// CreateAPIExtensionsConfig 函数的作用是为 `apiextensions-apiserver` 创建一个专门的配置对象。
// `apiextensions-apiserver` 是一个内嵌在 `kube-apiserver` 中的、独立的 API 服务，
// 它专门负责处理所有与 CustomResourceDefinition (CRD) 相关的 API 请求（例如，创建、读取、更新、删除 CRD 对象本身）。
// 这个函数通过获取主 kube-apiserver 的配置，并对其进行一系列的定制化修改，来生成这个专用配置。
func CreateAPIExtensionsConfig(
	// kubeAPIServerConfig 是主 kube-apiserver 的通用配置，它包含了大部分共享的设置，
	// 比如认证、授权、etcd 连接信息等。这是配置复用的基础。
	kubeAPIServerConfig server.Config,
	// kubeInformers 是一个共享的 Informer 工厂，它为所有内部组件提供访问 Kubernetes 资源
	// 本地缓存的能力，避免了重复创建 Informer 和重复从 etcd 拉取数据。
	kubeInformers informers.SharedInformerFactory,
	// pluginInitializers 是一系列用于初始化准入控制插件的函数。
	pluginInitializers []admission.PluginInitializer,
	// commandOptions 是从命令行参数解析和补全后得到的完整选项。这里主要用到了它的 Etcd 相关配置。
	commandOptions options.CompletedOptions,
	// masterCount 是集群中 master 节点的数量，这对于一些需要选举 leader 的组件（比如 CRD 的 condition controller）很重要。
	masterCount int,
	// serviceResolver 是我们之前分析过的“服务导航系统”，它告诉 aggregator 如何找到后端服务的 IP 地址。
	// 虽然 apiextensions-apiserver 主要处理 CRD，但它也可能需要这个来解析 webhook 地址等。
	serviceResolver webhook.ServiceResolver,
	// authResolverWrapper 是一个用于解析 webhook 认证信息的包装器。
	// 当 CRD 的 Validation/Conversion Webhook 需要调用时，apiextensions-apiserver 需要知道如何安全地与这些 webhook 通信。
	authResolverWrapper webhook.AuthenticationInfoResolverWrapper,
) (*apiextensionsapiserver.Config, error) {
	// make a shallow copy to let us twiddle a few things
	// most of the config actually remains the same.  We only need to mess with a couple items related to the particulars of the apiextensions
	// 1. 创建一个主配置的“浅拷贝”。
	// 这么做的目的是为了在不污染原始 `kubeAPIServerConfig` 的情况下，对其进行修改。
	// 大部分配置（如认证、授权）都是共享和复用的，我们只修改少数几个与 apiextensions-apiserver 相关的特定配置。
	genericConfig := kubeAPIServerConfig
	// 清空 PostStartHooks。因为主 apiserver 和 apiextensions-apiserver 都是在同一个进程中运行，
	// 如果不清空，会导致相同的 PostStartHook 被重复注册，从而在启动时引发冲突和失败。
	genericConfig.PostStartHooks = map[string]server.PostStartHookConfigEntry{}
	// 清空 RESTOptionsGetter。apiextensions-apiserver 有自己独特的存储选项获取逻辑（CRDRESTOptionsGetter），
	// 所以需要把主 apiserver 的这个设置清空，避免混淆。
	genericConfig.RESTOptionsGetter = nil

	// copy the etcd options so we don't mutate originals.
	// we assume that the etcd options have been completed already.  avoid messing with anything outside
	// of changes to StorageConfig as that may lead to unexpected behavior when the options are applied.

	// 2. 定制化 Etcd 存储配置。
	// 同样，先拷贝一份 Etcd 的配置，避免修改原始对象。
	etcdOptions := *commandOptions.Etcd
	// this is where the true decodable levels come from.
	// 为 Etcd 存储设置专门的编解码器 (Codec)。
	// 这里指定了 CRD 对象在存入 etcd 时，应该如何被序列化。它同时支持 v1 和 v1beta1 两个版本。
	etcdOptions.StorageConfig.Codec = apiextensionsapiserver.Codecs.LegacyCodec(v1beta1.SchemeGroupVersion, v1.SchemeGroupVersion)
	// prefer the more compact serialization (v1beta1) for storage until https://issue.k8s.io/82292 is resolved for objects whose v1 serialization is too big but whose v1beta1 serialization can be stored
	etcdOptions.StorageConfig.EncodeVersioner = runtime.NewMultiGroupVersioner(v1beta1.SchemeGroupVersion, schema.GroupKind{Group: v1beta1.GroupName})
	// 跳过 Etcd 的健康检查端点。因为主 apiserver 已经注册了 Etcd 的健康检查，这里必须跳过，以避免重复注册。
	etcdOptions.SkipHealthEndpoints = true // avoid double wiring of health checks
	// 将定制好的 Etcd 配置应用到我们拷贝的 genericConfig 中。
	if err := etcdOptions.ApplyTo(&genericConfig); err != nil {
		return nil, err
	}

	// override MergedResourceConfig with apiextensions defaults and registry
	// 3. 应用 API 启用/禁用配置。
	// 使用 apiextensions-apiserver 自己的默认资源配置（哪些 API 版本默认开启），来覆盖 genericConfig 中的设置。
	if err := commandOptions.APIEnablement.ApplyTo(
		&genericConfig,
		apiextensionsapiserver.DefaultAPIResourceConfigSource(),
		apiextensionsapiserver.Scheme); err != nil {
		return nil, err
	}
	// 4. 组装最终的 apiextensionsapiserver.Config 对象。
	apiextensionsConfig := &apiextensionsapiserver.Config{
		// GenericConfig 包含了所有通用的、被复用的配置。
		GenericConfig: &server.RecommendedConfig{
			Config:                genericConfig, // 我们刚刚精心修改过的通用配置
			SharedInformerFactory: kubeInformers, // 共享的 Informer 工厂
		},
		// ExtraConfig 包含了所有 apiextensions-apiserver 独有的、特殊的配置。
		ExtraConfig: apiextensionsapiserver.ExtraConfig{
			// CRD 有自己的一套获取 etcd 存储选项的逻辑，因为它需要处理不同版本的 CRD 对象。
			CRDRESTOptionsGetter: apiextensionsoptions.NewCRDRESTOptionsGetter(etcdOptions, genericConfig.ResourceTransformers, genericConfig.StorageObjectCountTracker),
			// master 节点的数量
			MasterCount: masterCount,
			// 用于解析 webhook 认证信息的包装器
			AuthResolverWrapper: authResolverWrapper,
			// “服务导航系统”
			ServiceResolver: serviceResolver,
		},
	}

	// we need to clear the poststarthooks so we don't add them multiple times to all the servers (that fails)
	// 再次确保 PostStartHooks 被清空，这是一个双重保险。
	apiextensionsConfig.GenericConfig.PostStartHooks = map[string]server.PostStartHookConfigEntry{}
	// 返回最终构建完成的、为 apiextensions-apiserver 量身定制的配置对象。
	return apiextensionsConfig, nil
}
