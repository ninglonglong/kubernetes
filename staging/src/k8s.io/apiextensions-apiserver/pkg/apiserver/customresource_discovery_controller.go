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

package apiserver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"k8s.io/klog/v2"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	autoscaling "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
)

type DiscoveryController struct {
	versionHandler  *versionDiscoveryHandler
	groupHandler    *groupDiscoveryHandler
	resourceManager discoveryendpoint.ResourceManager

	crdLister  listers.CustomResourceDefinitionLister
	crdsSynced cache.InformerSynced

	// To allow injection for testing.
	syncFn func(version schema.GroupVersion) error

	queue workqueue.TypedRateLimitingInterface[schema.GroupVersion]
}

// NewDiscoveryController 构造一个新的 DiscoveryController 实例。
func NewDiscoveryController(
	crdInformer informers.CustomResourceDefinitionInformer, // CRD 的 Informer，用于监听 CRD 变化。
	versionHandler *versionDiscoveryHandler, // 处理版本级别发现（如 /apis/group/v1）的 HTTP 处理器。
	groupHandler *groupDiscoveryHandler, // 处理组级别发现（如 /apis/group）的 HTTP 处理器。
	resourceManager discoveryendpoint.ResourceManager, // 新的、聚合式的服务发现管理器。
) *DiscoveryController {
	// --- 1. 创建并初始化控制器结构体 ---
	c := &DiscoveryController{
		// 存储依赖项，这些是控制器需要去更新的目标。
		versionHandler:  versionHandler,
		groupHandler:    groupHandler,
		resourceManager: resourceManager,
		// 从 Informer 获取 Lister 和 Synced 函数。
		// Lister 提供了一个快速的、只读的、从本地缓存中获取 CRD 的方法。
		crdLister: crdInformer.Lister(),
		// Synced 函数用于判断 Informer 的本地缓存是否已与 API Server 完全同步。
		crdsSynced: crdInformer.Informer().HasSynced,
		// --- 2. 创建工作队列 (Work Queue) ---
		// 这是控制器模式的核心。它是一个带速率限制的队列，用于存放需要处理的工作项。
		// 工作项的类型是 `schema.GroupVersion`，意味着当一个 CRD 变化时，
		// 我们会将受影响的 API 组和版本（如 "apps/v1"）放入队列中等待处理。
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[schema.GroupVersion](),
			workqueue.TypedRateLimitingQueueConfig[schema.GroupVersion]{Name: "DiscoveryController"},
		),
	}
	// --- 3. 注册事件处理器 (Event Handlers) ---
	// 将控制器的处理函数注册到 CRD Informer 上。
	// 当 Informer 监听到 CRD 资源发生变化时，会调用这些函数。
	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		// 当一个 CRD 被创建时，调用 c.addCustomResourceDefinition。
		AddFunc: c.addCustomResourceDefinition,
		// 当一个 CRD 被更新时，调用 c.updateCustomResourceDefinition。
		UpdateFunc: c.updateCustomResourceDefinition,
		// 当一个 CRD 被删除时，调用 c.deleteCustomResourceDefinition。
		DeleteFunc: c.deleteCustomResourceDefinition,
	})
	// 这些 Add/Update/Delete 函数的内部逻辑通常是：
	// 1. 从事件中解析出 CRD 对象。
	// 2. 确定这个变化影响了哪些 GroupVersion。
	// 3. 将这些 GroupVersion 作为工作项添加到上面的 `queue` 中。

	// --- 4. 设置同步函数 (Sync Function) ---
	// `c.sync` 是实际执行“协调”逻辑的函数。
	// 控制器的主循环会从队列中取出一个 GroupVersion，然后调用 `c.sync(groupVersion)`。
	// `c.sync` 的工作是：
	// 1. 根据 GroupVersion，从 crdLister 中获取所有相关的 CRD。
	// 2. 计算出这个 GroupVersion 最新的、正确的服务发现信息。
	// 3. 更新 `versionHandler`, `groupHandler` 和 `resourceManager` 的状态。
	c.syncFn = c.sync

	return c
}

func (c *DiscoveryController) sync(version schema.GroupVersion) error {

	apiVersionsForDiscovery := []metav1.GroupVersionForDiscovery{}
	apiResourcesForDiscovery := []metav1.APIResource{}
	aggregatedAPIResourcesForDiscovery := []apidiscoveryv2.APIResourceDiscovery{}
	versionsForDiscoveryMap := map[metav1.GroupVersion]bool{}

	crds, err := c.crdLister.List(labels.Everything())
	if err != nil {
		return err
	}
	foundVersion := false
	foundGroup := false
	for _, crd := range crds {
		if !apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Established) {
			continue
		}

		if crd.Spec.Group != version.Group {
			continue
		}

		foundThisVersion := false
		var storageVersionHash string
		for _, v := range crd.Spec.Versions {
			if !v.Served {
				continue
			}
			// If there is any Served version, that means the group should show up in discovery
			foundGroup = true

			gv := metav1.GroupVersion{Group: crd.Spec.Group, Version: v.Name}
			if !versionsForDiscoveryMap[gv] {
				versionsForDiscoveryMap[gv] = true
				apiVersionsForDiscovery = append(apiVersionsForDiscovery, metav1.GroupVersionForDiscovery{
					GroupVersion: crd.Spec.Group + "/" + v.Name,
					Version:      v.Name,
				})
			}
			if v.Name == version.Version {
				foundThisVersion = true
			}
			if v.Storage {
				storageVersionHash = discovery.StorageVersionHash(gv.Group, gv.Version, crd.Spec.Names.Kind)
			}
		}

		if !foundThisVersion {
			continue
		}
		foundVersion = true

		verbs := metav1.Verbs([]string{"delete", "deletecollection", "get", "list", "patch", "create", "update", "watch"})
		// if we're terminating we don't allow some verbs
		if apiextensionshelpers.IsCRDConditionTrue(crd, apiextensionsv1.Terminating) {
			verbs = metav1.Verbs([]string{"delete", "deletecollection", "get", "list", "watch"})
		}

		apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
			Name:               crd.Status.AcceptedNames.Plural,
			SingularName:       crd.Status.AcceptedNames.Singular,
			Namespaced:         crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
			Kind:               crd.Status.AcceptedNames.Kind,
			Verbs:              verbs,
			ShortNames:         crd.Status.AcceptedNames.ShortNames,
			Categories:         crd.Status.AcceptedNames.Categories,
			StorageVersionHash: storageVersionHash,
		})

		subresources, err := apiextensionshelpers.GetSubresourcesForVersion(crd, version.Version)
		if err != nil {
			return err
		}

		if c.resourceManager != nil {
			var scope apidiscoveryv2.ResourceScope
			if crd.Spec.Scope == apiextensionsv1.NamespaceScoped {
				scope = apidiscoveryv2.ScopeNamespace
			} else {
				scope = apidiscoveryv2.ScopeCluster
			}
			apiResourceDiscovery := apidiscoveryv2.APIResourceDiscovery{
				Resource:         crd.Status.AcceptedNames.Plural,
				SingularResource: crd.Status.AcceptedNames.Singular,
				Scope:            scope,
				ResponseKind: &metav1.GroupVersionKind{
					Group:   version.Group,
					Version: version.Version,
					Kind:    crd.Status.AcceptedNames.Kind,
				},
				Verbs:      verbs,
				ShortNames: crd.Status.AcceptedNames.ShortNames,
				Categories: crd.Status.AcceptedNames.Categories,
			}
			if subresources != nil && subresources.Status != nil {
				apiResourceDiscovery.Subresources = append(apiResourceDiscovery.Subresources, apidiscoveryv2.APISubresourceDiscovery{
					Subresource: "status",
					ResponseKind: &metav1.GroupVersionKind{
						Group:   version.Group,
						Version: version.Version,
						Kind:    crd.Status.AcceptedNames.Kind,
					},
					Verbs: metav1.Verbs([]string{"get", "patch", "update"}),
				})
			}
			if subresources != nil && subresources.Scale != nil {
				apiResourceDiscovery.Subresources = append(apiResourceDiscovery.Subresources, apidiscoveryv2.APISubresourceDiscovery{
					Subresource: "scale",
					ResponseKind: &metav1.GroupVersionKind{
						Group:   autoscaling.GroupName,
						Version: "v1",
						Kind:    "Scale",
					},
					Verbs: metav1.Verbs([]string{"get", "patch", "update"}),
				})

			}
			aggregatedAPIResourcesForDiscovery = append(aggregatedAPIResourcesForDiscovery, apiResourceDiscovery)
		}

		if subresources != nil && subresources.Status != nil {
			apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
				Name:       crd.Status.AcceptedNames.Plural + "/status",
				Namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
				Kind:       crd.Status.AcceptedNames.Kind,
				Verbs:      metav1.Verbs([]string{"get", "patch", "update"}),
			})
		}

		if subresources != nil && subresources.Scale != nil {
			apiResourcesForDiscovery = append(apiResourcesForDiscovery, metav1.APIResource{
				Group:      autoscaling.GroupName,
				Version:    "v1",
				Kind:       "Scale",
				Name:       crd.Status.AcceptedNames.Plural + "/scale",
				Namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
				Verbs:      metav1.Verbs([]string{"get", "patch", "update"}),
			})
		}
	}

	if !foundGroup {
		c.groupHandler.unsetDiscovery(version.Group)
		c.versionHandler.unsetDiscovery(version)

		if c.resourceManager != nil {
			c.resourceManager.RemoveGroup(version.Group)
		}
		return nil
	}

	sortGroupDiscoveryByKubeAwareVersion(apiVersionsForDiscovery)

	apiGroup := metav1.APIGroup{
		Name:     version.Group,
		Versions: apiVersionsForDiscovery,
		// the preferred versions for a group is the first item in
		// apiVersionsForDiscovery after it put in the right ordered
		PreferredVersion: apiVersionsForDiscovery[0],
	}
	c.groupHandler.setDiscovery(version.Group, discovery.NewAPIGroupHandler(Codecs, apiGroup))

	if !foundVersion {
		c.versionHandler.unsetDiscovery(version)

		if c.resourceManager != nil {
			c.resourceManager.RemoveGroupVersion(metav1.GroupVersion{
				Group:   version.Group,
				Version: version.Version,
			})
		}
		return nil
	}
	c.versionHandler.setDiscovery(version, discovery.NewAPIVersionHandler(Codecs, version, discovery.APIResourceListerFunc(func() []metav1.APIResource {
		return apiResourcesForDiscovery
	})))

	sort.Slice(aggregatedAPIResourcesForDiscovery, func(i, j int) bool {
		return aggregatedAPIResourcesForDiscovery[i].Resource < aggregatedAPIResourcesForDiscovery[j].Resource
	})
	if c.resourceManager != nil {
		c.resourceManager.AddGroupVersion(version.Group, apidiscoveryv2.APIVersionDiscovery{
			Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			Version:   version.Version,
			Resources: aggregatedAPIResourcesForDiscovery,
		})
		// Default priority for CRDs
		c.resourceManager.SetGroupVersionPriority(metav1.GroupVersion(version), 1000, 100)
	}
	return nil
}

func sortGroupDiscoveryByKubeAwareVersion(gd []metav1.GroupVersionForDiscovery) {
	sort.Slice(gd, func(i, j int) bool {
		return version.CompareKubeAwareVersionStrings(gd[i].Version, gd[j].Version) > 0
	})
}

func (c *DiscoveryController) Run(stopCh <-chan struct{}, synchedCh chan<- struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	defer klog.Info("Shutting down DiscoveryController")

	klog.Info("Starting DiscoveryController")
	// --- 2. 等待 Informer 缓存同步 ---
	// `cache.WaitForCacheSync` 会阻塞，直到 `c.crdsSynced` 函数返回 true，或者 `stopCh` 被关闭。
	// 这一步至关重要，它确保了在控制器开始工作之前，其本地缓存 (`crdLister`) 已经与 API Server 完全同步。
	// 如果没有这一步，控制器可能会基于一个不完整的、过时的数据集做出错误的决策。
	if !cache.WaitForCacheSync(stopCh, c.crdsSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	// initially sync all group versions to make sure we serve complete discovery
	// --- 3. 执行初始的全量同步 ---
	// 在启动后台 worker 之前，先主动地、一次性地同步所有已存在的 CRD 的服务发现信息。
	// 这确保了 API 服务器在启动后能立即提供完整的服务发现，而不是等待事件逐个触发。
	if err := wait.PollUntilContextCancel(context.Background(), time.Second, true, func(ctx context.Context) (bool, error) {
		// 从本地缓存中列出所有的 CRD。

		crds, err := c.crdLister.List(labels.Everything())
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to initially list CRDs: %v", err))
			return false, nil
		}
		for _, crd := range crds {
			for _, v := range crd.Spec.Versions {
				gv := schema.GroupVersion{Group: crd.Spec.Group, Version: v.Name}
				// 直接调用 sync 函数来处理每个 GroupVersion。
				if err := c.sync(gv); err != nil {
					utilruntime.HandleError(fmt.Errorf("failed to initially sync CRD version %v: %v", gv, err))
					return false, nil
				}
			}
		}
		// 所有 CRD 都已成功同步，返回 true 退出轮询。
		return true, nil
	}); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			utilruntime.HandleError(fmt.Errorf("timed out waiting for initial discovery sync"))
			return
		}
		utilruntime.HandleError(fmt.Errorf("unexpected error: %w", err))
		return
	}
	// --- 4. 通知调用者初始同步已完成 ---
	// 关闭 `synchedCh` channel，这是一个信号，告诉 `Run` 的调用者（通常是 APIServer 的启动逻辑）
	// 服务发现已经准备就绪，可以继续下一步了。
	close(synchedCh)

	// only start one worker thread since its a slow moving API
	// --- 5. 启动后台 Worker ---
	// 启动一个后台 goroutine 来持续处理队列中的工作项。
	// `wait.Until` 会定期（每秒）调用 `c.runWorker`，直到 `stopCh` 被关闭。
	// 注释中提到“只启动一个 worker”，是因为 CRD 的变化频率不高（"slow moving API"），
	// 一个 worker 就足以处理了。对于变化频繁的资源（如 Pods），通常会启动多个 worker。
	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

func (c *DiscoveryController) runWorker() {
	// `for c.processNextWorkItem()` 会一直循环，直到 `processNextWorkItem` 返回 `false`。
	// `processNextWorkItem` 只在队列被关闭时才会返回 `false`。
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
// processNextWorkItem 负责处理队列中的一个工作项。当需要退出时，它返回 false。
func (c *DiscoveryController) processNextWorkItem() bool {
	// --- 1. 从队列中获取工作项 ---
	// `c.queue.Get()` 是一个阻塞操作。如果队列为空，它会一直等待，直到有新的工作项被加入。
	key, quit := c.queue.Get()
	// 如果 `quit` 为 true，说明队列已经被关闭了（通过 `c.queue.ShutDown()`）。
	// 这是 worker 协程退出的信号。
	if quit {
		return false
	}
	// `defer c.queue.Done(key)` 至关重要。它通知队列，我们已经处理完了这个 key。
	// 如果不调用 `Done`，这个 key 会被认为一直在处理中，这会影响速率限制器的计算。
	defer c.queue.Done(key)
	// --- 2. 调用核心的同步函数 ---
	// `c.syncFn(key)` 就是我们之前分析过的 `c.sync(key)` 函数。
	// 它会执行实际的协调逻辑（例如，检查名称冲突、更新服务发现等）。
	err := c.syncFn(key)
	// --- 3. 根据处理结果操作队列 ---

	if err == nil {
		// 如果 `syncFn` 返回 nil，表示这个 key 已经被成功处理了。

		// `c.queue.Forget(key)` 告诉速率限制器“忘记”这个 key。
		// 如果这个 key 之前因为处理失败而被多次重试，`Forget` 会重置它的失败计数，
		// 这样下次它再出现时，就不会被立即限流。
		c.queue.Forget(key)
		return true
	}

	// 如果 `syncFn` 返回了错误，表示处理失败，我们需要重试。

	// `utilruntime.HandleError` 是一个标准的错误处理函数，它会打印错误日志，但不会让程序 panic。

	utilruntime.HandleError(fmt.Errorf("%v failed with: %v", key, err))

	// `c.queue.AddRateLimited(key)` 将处理失败的 key 重新放回队列中，以便稍后重试。
	// “RateLimited”意味着它不会立即被重新处理，而是会根据速率限制器的策略（通常是指数退避）
	// 等待一段时间后再被 `Get()` 取出。这可以防止因一个有问题的资源而导致控制器陷入疯狂的重试循环。
	c.queue.AddRateLimited(key)

	return true
}

func (c *DiscoveryController) enqueue(obj *apiextensionsv1.CustomResourceDefinition) {
	for _, v := range obj.Spec.Versions {
		c.queue.Add(schema.GroupVersion{Group: obj.Spec.Group, Version: v.Name})
	}
}

func (c *DiscoveryController) addCustomResourceDefinition(obj interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Adding customresourcedefinition %s", castObj.Name)
	c.enqueue(castObj)
}

func (c *DiscoveryController) updateCustomResourceDefinition(oldObj, newObj interface{}) {
	castNewObj := newObj.(*apiextensionsv1.CustomResourceDefinition)
	castOldObj := oldObj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Updating customresourcedefinition %s", castOldObj.Name)
	// Enqueue both old and new object to make sure we remove and add appropriate Versions.
	// The working queue will resolve any duplicates and only changes will stay in the queue.
	c.enqueue(castNewObj)
	c.enqueue(castOldObj)
}

func (c *DiscoveryController) deleteCustomResourceDefinition(obj interface{}) {
	castObj, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		castObj, ok = tombstone.Obj.(*apiextensionsv1.CustomResourceDefinition)
		if !ok {
			klog.Errorf("Tombstone contained object that is not expected %#v", obj)
			return
		}
	}
	klog.V(4).Infof("Deleting customresourcedefinition %q", castObj.Name)
	c.enqueue(castObj)
}
