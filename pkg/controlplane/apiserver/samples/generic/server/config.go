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
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/util/webhook"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kubernetes/pkg/controlplane"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"
)

type Config struct {
	Options options.CompletedOptions

	Aggregator    *aggregatorapiserver.Config
	ControlPlane  *controlplaneapiserver.Config
	APIExtensions *apiextensionsapiserver.Config

	ExtraConfig
}

type ExtraConfig struct {
}

type completedConfig struct {
	Options options.CompletedOptions

	Aggregator    aggregatorapiserver.CompletedConfig
	ControlPlane  controlplaneapiserver.CompletedConfig
	APIExtensions apiextensionsapiserver.CompletedConfig

	ExtraConfig
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// Complete 填充 Config 对象及其所有子配置对象中缺失的字段。
// 这个方法本身不做太多实际工作，而是将“补全”的任务委派给每个子配置对象自己的 Complete 方法。
func (c *Config) Complete() (CompletedConfig, error) {
	// 创建并返回一个 CompletedConfig 对象。
	// 这个对象内部的 `completedConfig` 结构体包含了所有补全后的子配置。
	return CompletedConfig{&completedConfig{
		// 直接将原始的 CompletedOptions 传递下去。
		Options: c.Options,

		// --- 核心的委派逻辑 ---
		// 调用每个子配置对象各自的 Complete() 方法。
		// 1. `c.Aggregator.Complete()`: 补全聚合层 apiserver 的配置。
		//    这会填充与 API Service 代理、认证等相关的默认值。
		// 2. `c.ControlPlane.Complete()`: 补全核心控制平面 apiserver 的配置。
		//    这是最复杂的一步，会处理核心 API 的存储、版本、默认准入插件等。
		// 3. `c.APIExtensions.Complete()`: 补全 API 扩展 apiserver 的配置。
		//    这会处理与 CRD (CustomResourceDefinition) 相关的配置。
		Aggregator:    c.Aggregator.Complete(),
		ControlPlane:  c.ControlPlane.Complete(),
		APIExtensions: c.APIExtensions.Complete(),
		// 直接传递 ExtraConfig，因为它通常不需要“补全”操作。
		ExtraConfig: c.ExtraConfig,
	}}, nil
}

// NewConfig creates all the self-contained pieces making up an
// sample-generic-controlplane, but does not wire them yet into a server object.
func NewConfig(opts options.CompletedOptions) (*Config, error) {
	c := &Config{
		Options: opts,
	}

	genericConfig, versionedInformers, storageFactory, err := controlplaneapiserver.BuildGenericConfig(
		opts,
		[]*runtime.Scheme{legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme},
		controlplane.DefaultAPIResourceConfigSource(),
		generatedopenapi.GetOpenAPIDefinitions,
	)
	if err != nil {
		return nil, err
	}

	serviceResolver := webhook.NewDefaultServiceResolver()
	kubeAPIs, pluginInitializer, err := controlplaneapiserver.CreateConfig(opts, genericConfig, versionedInformers, storageFactory, serviceResolver, nil)
	if err != nil {
		return nil, err
	}
	c.ControlPlane = kubeAPIs

	authInfoResolver := webhook.NewDefaultAuthenticationInfoResolverWrapper(kubeAPIs.ProxyTransport, kubeAPIs.Generic.EgressSelector, kubeAPIs.Generic.LoopbackClientConfig, kubeAPIs.Generic.TracerProvider)
	apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*kubeAPIs.Generic, kubeAPIs.VersionedInformers, pluginInitializer, opts, 3, serviceResolver, authInfoResolver)
	if err != nil {
		return nil, err
	}
	c.APIExtensions = apiExtensions

	aggregator, err := controlplaneapiserver.CreateAggregatorConfig(*kubeAPIs.Generic, opts, kubeAPIs.VersionedInformers, serviceResolver, kubeAPIs.ProxyTransport, kubeAPIs.Extra.PeerProxy, pluginInitializer)
	if err != nil {
		return nil, err
	}
	c.Aggregator = aggregator
	c.Aggregator.ExtraConfig.DisableRemoteAvailableConditionController = true

	return c, nil
}
