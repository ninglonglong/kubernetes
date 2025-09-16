/*
Copyright 2017 The Kubernetes Authors.

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
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	admissionmetrics "k8s.io/apiserver/pkg/admission/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
	mutatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/mutating"
	validatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	mutatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/mutating"
	validatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/validating"
	apiserverapi "k8s.io/apiserver/pkg/apis/apiserver"
	apiserverapiv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	apiserverapiv1alpha1 "k8s.io/apiserver/pkg/apis/apiserver/v1alpha1"
	"k8s.io/apiserver/pkg/server"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	"k8s.io/component-base/featuregate"
)

var configScheme = runtime.NewScheme()

func init() {
	utilruntime.Must(apiserverapi.AddToScheme(configScheme))
	utilruntime.Must(apiserverapiv1alpha1.AddToScheme(configScheme))
	utilruntime.Must(apiserverapiv1.AddToScheme(configScheme))
}

// AdmissionOptions holds the admission options
type AdmissionOptions struct {
	// RecommendedPluginOrder holds an ordered list of plugin names we recommend to use by default
	RecommendedPluginOrder []string
	// DefaultOffPlugins is a set of plugin names that is disabled by default
	DefaultOffPlugins sets.Set[string]

	// EnablePlugins indicates plugins to be enabled passed through `--enable-admission-plugins`.
	EnablePlugins []string
	// DisablePlugins indicates plugins to be disabled passed through `--disable-admission-plugins`.
	DisablePlugins []string
	// ConfigFile is the file path with admission control configuration.
	ConfigFile string
	// Plugins contains all registered plugins.
	Plugins *admission.Plugins
	// Decorators is a list of admission decorator to wrap around the admission plugins
	Decorators admission.Decorators
}

// NewAdmissionOptions creates a new instance of AdmissionOptions
// Note:
//
//	In addition it calls RegisterAllAdmissionPlugins to register
//	all generic admission plugins.
//
//	Provides the list of RecommendedPluginOrder that holds sane values
//	that can be used by servers that don't care about admission chain.
//	Servers that do care can overwrite/append that field after creation.
//
// NewAdmissionOptions 创建一个新的 AdmissionOptions 实例。
//
// 注意：
//  1. 除了创建对象，它还会调用 RegisterAllAdmissionPlugins 来注册所有“通用”的准入插件。
//  2. 它提供了 RecommendedPluginOrder 字段，其中包含了一组合理的默认值，
//     不关心准入链顺序的服务器可以直接使用。
//     而关心顺序的服务器可以在创建此对象后，覆盖或追加该字段的内容。
func NewAdmissionOptions() *AdmissionOptions {
	// 直接创建一个 AdmissionOptions 结构体实例，并填充所有字段的默认值。
	options := &AdmissionOptions{
		// Plugins: 初始化一个空的插件注册表。
		Plugins: admission.NewPlugins(),
		// Decorators: 设置一个默认的装饰器。
		// 这个装饰器 `admissionmetrics.WithControllerMetrics` 会包装每个准-入插件，
		// 使得每个插件的执行延迟、错误率等指标都能被自动收集和暴露到 /metrics 端点。
		// 这是实现可观测性的关键一步。
		Decorators: admission.Decorators{admission.DecoratorFunc(admissionmetrics.WithControllerMetrics)},
		// This list is mix of mutating admission plugins and validating
		// admission plugins. The apiserver always runs the validating ones
		// after all the mutating ones, so their relative order in this list
		// doesn't matter.
		// RecommendedPluginOrder: 提供一个最基础、最通用的插件执行顺序。
		// 这个列表混合了修改型（mutating）和验证型（validating）插件。
		// API 服务器的核心逻辑会自动保证所有的修改型插件都在验证型插件之前执行，
		// 所以它们在这个列表中的相对顺序并不重要。
		RecommendedPluginOrder: []string{lifecycle.PluginName, // "PodLifecycle" (旧称，现在可能有所变化)，处理 Pod 的生命周期，如终止等。
			mutatingadmissionpolicy.PluginName,   // "MutatingAdmissionPolicy"，处理 CEL 表达式定义的修改策略。
			mutatingwebhook.PluginName,           // "MutatingAdmissionWebhook"，调用外部 Webhook 来修改对象。
			validatingadmissionpolicy.PluginName, // "ValidatingAdmissionPolicy"，处理 CEL 表达式定义的验证策略。
			validatingwebhook.PluginName},        // "ValidatingAdmissionWebhook"，调用外部 Webhook 来验证对象。
		// DefaultOffPlugins: 默认情况下，通用 API 服务器没有需要默认关闭的插件。
		// 这个列表为空，意味着所有注册的插件默认都是“开启”状态，除非用户明确禁用。
		DefaultOffPlugins: sets.Set[string]{},
	}
	// 注册所有“通用”的准入插件。
	// 注意：这里的 `server.RegisterAllAdmissionPlugins` 和 kube-apiserver 中的那个函数不同。
	// 它只注册那些不依赖于 Kubernetes 核心 API (即不依赖于 `k8s.io/kubernetes` 包) 的通用插件。
	// 例如，它会注册 `MutatingAdmissionWebhook` 和 `ValidatingAdmissionWebhook`，但不会注册 `LimitRanger` 或 `ResourceQuota`。
	server.RegisterAllAdmissionPlugins(options.Plugins)
	return options
}

// AddFlags adds flags related to admission for a specific APIServer to the specified FlagSet
func (a *AdmissionOptions) AddFlags(fs *pflag.FlagSet) {
	if a == nil {
		return
	}

	fs.StringSliceVar(&a.EnablePlugins, "enable-admission-plugins", a.EnablePlugins, ""+
		"admission plugins that should be enabled in addition to default enabled ones ("+
		strings.Join(a.defaultEnabledPluginNames(), ", ")+"). "+
		"Comma-delimited list of admission plugins: "+strings.Join(a.Plugins.Registered(), ", ")+". "+
		"The order of plugins in this flag does not matter.")
	fs.StringSliceVar(&a.DisablePlugins, "disable-admission-plugins", a.DisablePlugins, ""+
		"admission plugins that should be disabled although they are in the default enabled plugins list ("+
		strings.Join(a.defaultEnabledPluginNames(), ", ")+"). "+
		"Comma-delimited list of admission plugins: "+strings.Join(a.Plugins.Registered(), ", ")+". "+
		"The order of plugins in this flag does not matter.")
	fs.StringVar(&a.ConfigFile, "admission-control-config-file", a.ConfigFile,
		"File with admission control configuration.")
}

// ApplyTo adds the admission chain to the server configuration.
// In case admission plugin names were not provided by a cluster-admin they will be prepared from the recommended/default values.
// In addition the method lazily initializes a generic plugin that is appended to the list of pluginInitializers
// note this method uses:
//
//	genericconfig.Authorizer
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

	// Admission depends on CoreAPI to set SharedInformerFactory and ClientConfig.
	if informers == nil {
		return fmt.Errorf("admission depends on a Kubernetes core API shared informer, it cannot be nil")
	}
	if kubeClient == nil || dynamicClient == nil {
		return fmt.Errorf("admission depends on a Kubernetes core API client, it cannot be nil")
	}

	pluginNames := a.enabledPluginNames()

	pluginsConfigProvider, err := admission.ReadAdmissionConfiguration(pluginNames, a.ConfigFile, configScheme)
	if err != nil {
		return fmt.Errorf("failed to read plugin config: %v", err)
	}

	discoveryClient := cacheddiscovery.NewMemCacheClient(kubeClient.Discovery())
	discoveryRESTMapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)
	genericInitializer := initializer.New(kubeClient, dynamicClient, informers, c.Authorization.Authorizer, features,
		c.DrainedNotify(), discoveryRESTMapper)
	initializersChain := admission.PluginInitializers{genericInitializer}
	initializersChain = append(initializersChain, pluginInitializers...)

	admissionPostStartHook := func(hookContext server.PostStartHookContext) error {
		discoveryRESTMapper.Reset()
		go utilwait.Until(discoveryRESTMapper.Reset, 30*time.Second, hookContext.Done())
		return nil
	}

	err = c.AddPostStartHook("start-apiserver-admission-initializer", admissionPostStartHook)
	if err != nil {
		return fmt.Errorf("failed to add post start hook for policy admission: %w", err)
	}

	admissionChain, err := a.Plugins.NewFromPlugins(pluginNames, pluginsConfigProvider, initializersChain, a.Decorators)
	if err != nil {
		return err
	}

	c.AdmissionControl = admissionmetrics.WithStepMetrics(admissionChain)
	return nil
}

// Validate verifies flags passed to AdmissionOptions.
func (a *AdmissionOptions) Validate() []error {
	if a == nil {
		return nil
	}

	errs := []error{}

	registeredPlugins := sets.NewString(a.Plugins.Registered()...)
	for _, name := range a.EnablePlugins {
		if !registeredPlugins.Has(name) {
			errs = append(errs, fmt.Errorf("enable-admission-plugins plugin %q is unknown", name))
		}
	}

	for _, name := range a.DisablePlugins {
		if !registeredPlugins.Has(name) {
			errs = append(errs, fmt.Errorf("disable-admission-plugins plugin %q is unknown", name))
		}
	}

	enablePlugins := sets.NewString(a.EnablePlugins...)
	disablePlugins := sets.NewString(a.DisablePlugins...)
	if len(enablePlugins.Intersection(disablePlugins).List()) > 0 {
		errs = append(errs, fmt.Errorf("%v in enable-admission-plugins and disable-admission-plugins "+
			"overlapped", enablePlugins.Intersection(disablePlugins).List()))
	}

	// Verify RecommendedPluginOrder.
	recommendPlugins := sets.NewString(a.RecommendedPluginOrder...)
	intersections := registeredPlugins.Intersection(recommendPlugins)
	if !intersections.Equal(recommendPlugins) {
		// Developer error, this should never run in.
		errs = append(errs, fmt.Errorf("plugins %v in RecommendedPluginOrder are not registered",
			recommendPlugins.Difference(intersections).List()))
	}
	if !intersections.Equal(registeredPlugins) {
		// Developer error, this should never run in.
		errs = append(errs, fmt.Errorf("plugins %v registered are not in RecommendedPluginOrder",
			registeredPlugins.Difference(intersections).List()))
	}

	return errs
}

// enabledPluginNames makes use of RecommendedPluginOrder, DefaultOffPlugins,
// EnablePlugins, DisablePlugins fields
// to prepare a list of ordered plugin names that are enabled.
func (a *AdmissionOptions) enabledPluginNames() []string {
	allOffPlugins := append(sets.List[string](a.DefaultOffPlugins), a.DisablePlugins...)
	disabledPlugins := sets.NewString(allOffPlugins...)
	enabledPlugins := sets.NewString(a.EnablePlugins...)
	disabledPlugins = disabledPlugins.Difference(enabledPlugins)

	orderedPlugins := []string{}
	for _, plugin := range a.RecommendedPluginOrder {
		if !disabledPlugins.Has(plugin) {
			orderedPlugins = append(orderedPlugins, plugin)
		}
	}

	return orderedPlugins
}

// Return names of plugins which are enabled by default
func (a *AdmissionOptions) defaultEnabledPluginNames() []string {
	defaultOnPluginNames := []string{}
	for _, pluginName := range a.RecommendedPluginOrder {
		if !a.DefaultOffPlugins.Has(pluginName) {
			defaultOnPluginNames = append(defaultOnPluginNames, pluginName)
		}
	}

	return defaultOnPluginNames
}
