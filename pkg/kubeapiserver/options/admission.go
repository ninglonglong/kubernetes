/*
Copyright 2018 The Kubernetes Authors.

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
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/informers"
	"k8s.io/component-base/featuregate"
)

// AdmissionOptions holds the admission options.
// It is a wrap of generic AdmissionOptions.
type AdmissionOptions struct {
	// GenericAdmission holds the generic admission options.
	GenericAdmission *genericoptions.AdmissionOptions
	// DEPRECATED flag, should use EnabledAdmissionPlugins and DisabledAdmissionPlugins.
	// They are mutually exclusive, specify both will lead to an error.
	PluginNames []string
}

// NewAdmissionOptions creates a new instance of AdmissionOptions
// Note:
//
//	In addition it calls RegisterAllAdmissionPlugins to register
//	all kube-apiserver admission plugins.
//
//	Provides the list of RecommendedPluginOrder that holds sane values
//	that can be used by servers that don't care about admission chain.
//	Servers that do care can overwrite/append that field after creation.

// NewAdmissionOptions 创建一个新的 AdmissionOptions 实例。
//
// 注意：
//  1. 除了创建对象，它还会调用 RegisterAllAdmissionPlugins 来注册所有内置于 kube-apiserver 的准入插件。
//  2. 它提供了 RecommendedPluginOrder 字段，其中包含了一组经过深思熟虑的、合理的插件执行顺序。
//     不关心准入链顺序的服务器可以直接使用这个默认值。
//     而关心顺序的服务器可以在创建此对象后，覆盖或追加该字段的内容。
func NewAdmissionOptions() *AdmissionOptions {
	// 首先，调用通用的构造函数，创建一个基础的、不包含任何特定插件的 AdmissionOptions 对象。
	// 这个通用对象只提供了存放插件列表、顺序等配置的“容器”。
	options := genericoptions.NewAdmissionOptions()
	// register all admission plugins
	// 注册所有 kube-apiserver 内置的准入插件。
	// 这是一个非常关键的步骤：它将所有插件的“工厂函数”注册到 options.Plugins 这个“插件注册表”中。
	// 这样，后续的逻辑就可以根据名称（如 "MutatingAdmissionWebhook"）来查找并初始化对应的插件实例。
	RegisterAllAdmissionPlugins(options.Plugins)
	// set RecommendedPluginOrder
	// 设置推荐的插件执行顺序。
	// AllOrderedPlugins 是一个预先定义好的字符串切片，它规定了所有已知插件的执行顺序。
	// 这个顺序至关重要，例如，修改性质的插件（Mutating）必须在验证性质的插件（Validating）之前执行。
	options.RecommendedPluginOrder = AllOrderedPlugins
	// set DefaultOffPlugins
	// 设置默认关闭的插件列表。
	// Kubernetes 有一些准入插件因为安全风险、性能影响或功能待完善等原因，默认是不开启的。
	// 这个列表告诉 API 服务器，即使用户没有通过 --disable-admission-plugins 参数明确禁用它们，
	// 也不要默认启用它们。
	options.DefaultOffPlugins = DefaultOffAdmissionPlugins()
	// 返回一个包装了通用选项的 kube-apiserver 特定 AdmissionOptions 对象。
	// 这种包装结构为未来在不破坏通用性的前提下，扩展 kube-apiserver 自身的准入选项提供了可能。
	return &AdmissionOptions{
		GenericAdmission: options,
	}
}

// AddFlags adds flags related to admission for kube-apiserver to the specified FlagSet
func (a *AdmissionOptions) AddFlags(fs *pflag.FlagSet) {
	if a == nil {
		return
	}
	registerAllAdmissionPluginFlags(fs)
	fs.StringSliceVar(&a.PluginNames, "admission-control", a.PluginNames, ""+
		"Admission is divided into two phases. "+
		"In the first phase, only mutating admission plugins run. "+
		"In the second phase, only validating admission plugins run. "+
		"The names in the below list may represent a validating plugin, a mutating plugin, or both. "+
		"The order of plugins in which they are passed to this flag does not matter. "+
		"Comma-delimited list of: "+strings.Join(a.GenericAdmission.Plugins.Registered(), ", ")+".")
	fs.MarkDeprecated("admission-control", "Use --enable-admission-plugins or --disable-admission-plugins instead. Will be removed in a future version.")
	fs.Lookup("admission-control").Hidden = false

	a.GenericAdmission.AddFlags(fs)
}

// Validate verifies flags passed to kube-apiserver AdmissionOptions.
// Kube-apiserver verifies PluginNames and then call generic AdmissionOptions.Validate.
func (a *AdmissionOptions) Validate() []error {
	if a == nil {
		return nil
	}
	var errs []error
	if a.PluginNames != nil &&
		(a.GenericAdmission.EnablePlugins != nil || a.GenericAdmission.DisablePlugins != nil) {
		errs = append(errs, fmt.Errorf("admission-control and enable-admission-plugins/disable-admission-plugins flags are mutually exclusive"))
	}

	registeredPlugins := sets.NewString(a.GenericAdmission.Plugins.Registered()...)
	for _, name := range a.PluginNames {
		if !registeredPlugins.Has(name) {
			errs = append(errs, fmt.Errorf("admission-control plugin %q is unknown", name))
		}
	}

	errs = append(errs, a.GenericAdmission.Validate()...)

	return errs
}

// ApplyTo adds the admission chain to the server configuration.
// Kube-apiserver just call generic AdmissionOptions.ApplyTo.
func (a *AdmissionOptions) ApplyTo(
	c *server.Config,
	informers informers.SharedInformerFactory,
	kubeClient kubernetes.Interface,
	dynamicClient dynamic.Interface,
	features featuregate.FeatureGate,
	pluginInitializers ...admission.PluginInitializer,
) error {
	if a == nil {
		return nil
	}

	if a.PluginNames != nil {
		// pass PluginNames to generic AdmissionOptions
		a.GenericAdmission.EnablePlugins, a.GenericAdmission.DisablePlugins = computePluginNames(a.PluginNames, a.GenericAdmission.RecommendedPluginOrder)
	}

	return a.GenericAdmission.ApplyTo(c, informers, kubeClient, dynamicClient, features, pluginInitializers...)
}

// explicitly disable all plugins that are not in the enabled list
func computePluginNames(explicitlyEnabled []string, all []string) (enabled []string, disabled []string) {
	return explicitlyEnabled, sets.NewString(all...).Difference(sets.NewString(explicitlyEnabled...)).List()
}
