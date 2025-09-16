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

package options

// This file exists to force the desired plugin implementations to be linked.
// This should probably be part of some configuration fed into the build for a
// given binary target.
import (
	"github.com/spf13/pflag"

	mutatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/mutating"
	validatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/validating"

	// Admission policies
	"k8s.io/kubernetes/plugin/pkg/admission/admit"
	"k8s.io/kubernetes/plugin/pkg/admission/alwayspullimages"
	"k8s.io/kubernetes/plugin/pkg/admission/antiaffinity"
	certapproval "k8s.io/kubernetes/plugin/pkg/admission/certificates/approval"
	"k8s.io/kubernetes/plugin/pkg/admission/certificates/ctbattest"
	certsigning "k8s.io/kubernetes/plugin/pkg/admission/certificates/signing"
	certsubjectrestriction "k8s.io/kubernetes/plugin/pkg/admission/certificates/subjectrestriction"
	"k8s.io/kubernetes/plugin/pkg/admission/defaulttolerationseconds"
	"k8s.io/kubernetes/plugin/pkg/admission/deny"
	"k8s.io/kubernetes/plugin/pkg/admission/eventratelimit"
	"k8s.io/kubernetes/plugin/pkg/admission/extendedresourcetoleration"
	"k8s.io/kubernetes/plugin/pkg/admission/gc"
	"k8s.io/kubernetes/plugin/pkg/admission/imagepolicy"
	"k8s.io/kubernetes/plugin/pkg/admission/limitranger"
	"k8s.io/kubernetes/plugin/pkg/admission/namespace/autoprovision"
	"k8s.io/kubernetes/plugin/pkg/admission/namespace/exists"
	"k8s.io/kubernetes/plugin/pkg/admission/network/defaultingressclass"
	"k8s.io/kubernetes/plugin/pkg/admission/network/denyserviceexternalips"
	"k8s.io/kubernetes/plugin/pkg/admission/noderestriction"
	"k8s.io/kubernetes/plugin/pkg/admission/nodetaint"
	"k8s.io/kubernetes/plugin/pkg/admission/podnodeselector"
	"k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction"
	"k8s.io/kubernetes/plugin/pkg/admission/podtopologylabels"
	podpriority "k8s.io/kubernetes/plugin/pkg/admission/priority"
	"k8s.io/kubernetes/plugin/pkg/admission/runtimeclass"
	"k8s.io/kubernetes/plugin/pkg/admission/security/podsecurity"
	"k8s.io/kubernetes/plugin/pkg/admission/serviceaccount"
	"k8s.io/kubernetes/plugin/pkg/admission/storage/persistentvolume/resize"
	"k8s.io/kubernetes/plugin/pkg/admission/storage/storageclass/setdefault"
	"k8s.io/kubernetes/plugin/pkg/admission/storage/storageobjectinuseprotection"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
	"k8s.io/apiserver/pkg/admission/plugin/resourcequota"
	mutatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/mutating"
	validatingwebhook "k8s.io/apiserver/pkg/admission/plugin/webhook/validating"
)

// AllOrderedPlugins is the list of all the plugins in order.
var AllOrderedPlugins = []string{
	admit.PluginName,                        // AlwaysAdmit
	autoprovision.PluginName,                // NamespaceAutoProvision
	lifecycle.PluginName,                    // NamespaceLifecycle
	exists.PluginName,                       // NamespaceExists
	antiaffinity.PluginName,                 // LimitPodHardAntiAffinityTopology
	limitranger.PluginName,                  // LimitRanger
	serviceaccount.PluginName,               // ServiceAccount
	noderestriction.PluginName,              // NodeRestriction
	nodetaint.PluginName,                    // TaintNodesByCondition
	alwayspullimages.PluginName,             // AlwaysPullImages
	imagepolicy.PluginName,                  // ImagePolicyWebhook
	podsecurity.PluginName,                  // PodSecurity
	podnodeselector.PluginName,              // PodNodeSelector
	podpriority.PluginName,                  // Priority
	defaulttolerationseconds.PluginName,     // DefaultTolerationSeconds
	podtolerationrestriction.PluginName,     // PodTolerationRestriction
	eventratelimit.PluginName,               // EventRateLimit
	extendedresourcetoleration.PluginName,   // ExtendedResourceToleration
	setdefault.PluginName,                   // DefaultStorageClass
	storageobjectinuseprotection.PluginName, // StorageObjectInUseProtection
	gc.PluginName,                           // OwnerReferencesPermissionEnforcement
	resize.PluginName,                       // PersistentVolumeClaimResize
	runtimeclass.PluginName,                 // RuntimeClass
	certapproval.PluginName,                 // CertificateApproval
	certsigning.PluginName,                  // CertificateSigning
	ctbattest.PluginName,                    // ClusterTrustBundleAttest
	certsubjectrestriction.PluginName,       // CertificateSubjectRestriction
	defaultingressclass.PluginName,          // DefaultIngressClass
	denyserviceexternalips.PluginName,       // DenyServiceExternalIPs
	podtopologylabels.PluginName,            // PodTopologyLabels

	// new admission plugins should generally be inserted above here
	// webhook, resourcequota, and deny plugins must go at the end

	mutatingadmissionpolicy.PluginName,   // MutatingAdmissionPolicy
	mutatingwebhook.PluginName,           // MutatingAdmissionWebhook
	validatingadmissionpolicy.PluginName, // ValidatingAdmissionPolicy
	validatingwebhook.PluginName,         // ValidatingAdmissionWebhook
	resourcequota.PluginName,             // ResourceQuota
	deny.PluginName,                      // AlwaysDeny
}

// registerAllAdmissionPluginFlags registers legacy CLI flag options for admission plugins.
// No new plugins should use CLI flags to configure themselves.
func registerAllAdmissionPluginFlags(fs *pflag.FlagSet) {
	defaulttolerationseconds.RegisterFlags(fs)
}

// RegisterAllAdmissionPlugins registers all admission plugins.
// The order of registration is irrelevant, see AllOrderedPlugins for execution order.
func RegisterAllAdmissionPlugins(plugins *admission.Plugins) {
	admit.Register(plugins)                        // DEPRECATED as no real meaning	// DEPRECATED: 一个总是允许所有请求的插件，用于测试，已无实际意义。
	alwayspullimages.Register(plugins)             // 强制将 Pod 的镜像拉取策略（imagePullPolicy）设置为 "Always"。
	antiaffinity.Register(plugins)                 // 实现旧版的 Pod 反亲和性注解（现在推荐使用 PodSpec 中的 affinity 字段）。
	defaulttolerationseconds.Register(plugins)     // 为 Pod 上 "not-ready" 和 "unreachable" 的容忍度（Toleration）设置默认的容忍时间（tolerationSeconds）。
	defaultingressclass.Register(plugins)          // 为没有指定 IngressClass 的 Ingress 对象设置一个默认的 IngressClass。
	denyserviceexternalips.Register(plugins)       // 拒绝在 Service 中使用 ExternalIPs 字段，这是一个安全特性。
	deny.Register(plugins)                         // DEPRECATED as no real meaning
	eventratelimit.Register(plugins)               // 限制用户可以产生的事件（Event）数量，防止事件泛滥。
	extendedresourcetoleration.Register(plugins)   // 自动为使用扩展资源（如 GPU）的 Pod 添加相应的容忍度。
	gc.Register(plugins)                           // 为新创建的对象设置 OwnerReference 的初始状态，以便垃圾回收控制器（Garbage Collector）能正确工作。
	imagepolicy.Register(plugins)                  // [已废弃] 基于外部 Webhook 策略来决定是否允许使用某个容器镜像。
	limitranger.Register(plugins)                  // [核心资源插件] 强制 Pod 和容器的资源请求（requests）和限制（limits），并可为命名空间中的对象设置默认值。
	autoprovision.Register(plugins)                // 自动创建不存在的命名空间（默认关闭）。
	exists.Register(plugins)                       // 确保所有需要命名空间的对象都被创建在已经存在的命名空间中。
	noderestriction.Register(plugins)              // [核心安全插件] 限制 kubelet 只能修改其自身所在的 Node 对象和其上的 Pod 对象。
	nodetaint.Register(plugins)                    // 在旧版本中用于 Taint-based evictions，现在主要由 `TaintNodesByCondition` 替代。
	podnodeselector.Register(plugins)              // 强制 Pod 遵守其所在命名空间中定义的节点选择器（NodeSelector）。
	podtolerationrestriction.Register(plugins)     // 限制 Pod 可以拥有的容忍度，以匹配其所在命名空间中定义的白名单。
	runtimeclass.Register(plugins)                 // 根据 RuntimeClass 的定义来处理 Pod 的调度等属性。
	resourcequota.Register(plugins)                // [核心配额插件] 检查进入的请求是否会超出命名空间中定义的资源配额（ResourceQuota）。
	podsecurity.Register(plugins)                  // [核心安全插件] Pod 安全标准（Pod Security Standards）的实现，替代了 PodSecurityPolicy。
	podpriority.Register(plugins)                  // [核心调度插件] 根据 PriorityClass 为 Pod 设置优先级，并阻止低优先级 Pod 抢占高优先级 Pod。
	serviceaccount.Register(plugins)               // [核心插件] 实现 ServiceAccount 的自动化，如为 Pod 自动挂载 ServiceAccount token。
	setdefault.Register(plugins)                   // 为 API 对象设置各种默认值，是一个通用的默认值填充插件。
	resize.Register(plugins)                       // 允许在线扩容 PVC。
	storageobjectinuseprotection.Register(plugins) // [核心保护插件] 防止正在被 Pod 使用的 PV 和 PVC 被意外删除。
	certapproval.Register(plugins)                 // 处理 CertificateSigningRequest (CSR) 的批准逻辑。
	certsigning.Register(plugins)                  // 处理 CSR 的签发逻辑。
	ctbattest.Register(plugins)                    // 用于证书透明度（Certificate Transparency）的证明。
	certsubjectrestriction.Register(plugins)       // 限制 CSR 中允许的主题（Subject）字段。
	podtopologylabels.Register(plugins)            // 根据节点拓扑信息，自动为 PVC（PersistentVolumeClaim）添加或验证标签。
}

// DefaultOffAdmissionPlugins get admission plugins off by default for kube-apiserver.
func DefaultOffAdmissionPlugins() sets.Set[string] {
	defaultOnPlugins := sets.New(
		lifecycle.PluginName,                    // NamespaceLifecycle
		limitranger.PluginName,                  // LimitRanger
		serviceaccount.PluginName,               // ServiceAccount
		setdefault.PluginName,                   // DefaultStorageClass
		resize.PluginName,                       // PersistentVolumeClaimResize
		defaulttolerationseconds.PluginName,     // DefaultTolerationSeconds
		mutatingwebhook.PluginName,              // MutatingAdmissionWebhook
		validatingwebhook.PluginName,            // ValidatingAdmissionWebhook
		resourcequota.PluginName,                // ResourceQuota
		storageobjectinuseprotection.PluginName, // StorageObjectInUseProtection
		podpriority.PluginName,                  // Priority
		nodetaint.PluginName,                    // TaintNodesByCondition
		runtimeclass.PluginName,                 // RuntimeClass
		certapproval.PluginName,                 // CertificateApproval
		certsigning.PluginName,                  // CertificateSigning
		ctbattest.PluginName,                    // ClusterTrustBundleAttest
		certsubjectrestriction.PluginName,       // CertificateSubjectRestriction
		defaultingressclass.PluginName,          // DefaultIngressClass
		podsecurity.PluginName,                  // PodSecurity
		podtopologylabels.PluginName,            // PodTopologyLabels, only active when feature gate PodTopologyLabelsAdmission is enabled.
		mutatingadmissionpolicy.PluginName,      // Mutatingadmissionpolicy, only active when feature gate MutatingAdmissionpolicy is enabled
		validatingadmissionpolicy.PluginName,    // ValidatingAdmissionPolicy, only active when feature gate ValidatingAdmissionPolicy is enabled
	)

	return sets.New(AllOrderedPlugins...).Difference(defaultOnPlugins)
}
