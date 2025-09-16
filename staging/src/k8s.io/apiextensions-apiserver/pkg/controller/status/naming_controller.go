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

package status

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	apiextensionshelpers "k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	informers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	listers "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1"
)

// This controller is reserving names. To avoid conflicts, be sure to run only one instance of the worker at a time.
// This could eventually be lifted, but starting simple.
type NamingConditionController struct {
	crdClient client.CustomResourceDefinitionsGetter

	crdLister listers.CustomResourceDefinitionLister
	crdSynced cache.InformerSynced
	// crdMutationCache backs our lister and keeps track of committed updates to avoid racy
	// write/lookup cycles.  It's got 100 slots by default, so it unlikely to overrun
	// TODO to revisit this if naming conflicts are found to occur in the wild
	crdMutationCache cache.MutationCache

	// To allow injection for testing.
	syncFn func(key string) error

	queue workqueue.TypedRateLimitingInterface[string]
}

func NewNamingConditionController(
	logger klog.Logger,
	crdInformer informers.CustomResourceDefinitionInformer,
	crdClient client.CustomResourceDefinitionsGetter,
) *NamingConditionController {
	c := &NamingConditionController{
		crdClient: crdClient,
		crdLister: crdInformer.Lister(),
		crdSynced: crdInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "crd_naming_condition_controller"},
		),
	}

	informerIndexer := crdInformer.Informer().GetIndexer()
	c.crdMutationCache = cache.NewIntegerResourceVersionMutationCache(logger, informerIndexer, informerIndexer, 60*time.Second, false)

	crdInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addCustomResourceDefinition,
		UpdateFunc: c.updateCustomResourceDefinition,
		DeleteFunc: c.deleteCustomResourceDefinition,
	},
		cache.HandlerOptions{
			Logger: &logger,
		},
	)

	c.syncFn = c.sync

	return c
}

func (c *NamingConditionController) getAcceptedNamesForGroup(group string) (allResources sets.String, allKinds sets.String) {
	allResources = sets.String{}
	allKinds = sets.String{}

	list, err := c.crdLister.List(labels.Everything())
	if err != nil {
		panic(err)
	}

	for _, curr := range list {
		if curr.Spec.Group != group {
			continue
		}

		// for each item here, see if we have a mutation cache entry that is more recent
		// this makes sure that if we tight loop on update and run, our mutation cache will show
		// us the version of the objects we just updated to.
		item := curr
		obj, exists, err := c.crdMutationCache.GetByKey(curr.Name)
		if exists && err == nil {
			item = obj.(*apiextensionsv1.CustomResourceDefinition)
		}

		allResources.Insert(item.Status.AcceptedNames.Plural)
		allResources.Insert(item.Status.AcceptedNames.Singular)
		allResources.Insert(item.Status.AcceptedNames.ShortNames...)

		allKinds.Insert(item.Status.AcceptedNames.Kind)
		allKinds.Insert(item.Status.AcceptedNames.ListKind)
	}

	return allResources, allKinds
}

func (c *NamingConditionController) calculateNamesAndConditions(in *apiextensionsv1.CustomResourceDefinition) (apiextensionsv1.CustomResourceDefinitionNames, apiextensionsv1.CustomResourceDefinitionCondition, apiextensionsv1.CustomResourceDefinitionCondition) {
	// Get the names that have already been claimed
	allResources, allKinds := c.getAcceptedNamesForGroup(in.Spec.Group)

	namesAcceptedCondition := apiextensionsv1.CustomResourceDefinitionCondition{
		Type:   apiextensionsv1.NamesAccepted,
		Status: apiextensionsv1.ConditionUnknown,
	}

	requestedNames := in.Spec.Names
	acceptedNames := in.Status.AcceptedNames
	newNames := in.Status.AcceptedNames

	// Check each name for mismatches.  If there's a mismatch between spec and status, then try to deconflict.
	// Continue on errors so that the status is the best match possible
	if err := equalToAcceptedOrFresh(requestedNames.Plural, acceptedNames.Plural, allResources); err != nil {
		namesAcceptedCondition.Status = apiextensionsv1.ConditionFalse
		namesAcceptedCondition.Reason = "PluralConflict"
		namesAcceptedCondition.Message = err.Error()
	} else {
		newNames.Plural = requestedNames.Plural
	}
	if err := equalToAcceptedOrFresh(requestedNames.Singular, acceptedNames.Singular, allResources); err != nil {
		namesAcceptedCondition.Status = apiextensionsv1.ConditionFalse
		namesAcceptedCondition.Reason = "SingularConflict"
		namesAcceptedCondition.Message = err.Error()
	} else {
		newNames.Singular = requestedNames.Singular
	}
	if !reflect.DeepEqual(requestedNames.ShortNames, acceptedNames.ShortNames) {
		errs := []error{}
		existingShortNames := sets.NewString(acceptedNames.ShortNames...)
		for _, shortName := range requestedNames.ShortNames {
			// if the shortname is already ours, then we're fine
			if existingShortNames.Has(shortName) {
				continue
			}
			if err := equalToAcceptedOrFresh(shortName, "", allResources); err != nil {
				errs = append(errs, err)
			}

		}
		if err := utilerrors.NewAggregate(errs); err != nil {
			namesAcceptedCondition.Status = apiextensionsv1.ConditionFalse
			namesAcceptedCondition.Reason = "ShortNamesConflict"
			namesAcceptedCondition.Message = err.Error()
		} else {
			newNames.ShortNames = requestedNames.ShortNames
		}
	}

	if err := equalToAcceptedOrFresh(requestedNames.Kind, acceptedNames.Kind, allKinds); err != nil {
		namesAcceptedCondition.Status = apiextensionsv1.ConditionFalse
		namesAcceptedCondition.Reason = "KindConflict"
		namesAcceptedCondition.Message = err.Error()
	} else {
		newNames.Kind = requestedNames.Kind
	}
	if err := equalToAcceptedOrFresh(requestedNames.ListKind, acceptedNames.ListKind, allKinds); err != nil {
		namesAcceptedCondition.Status = apiextensionsv1.ConditionFalse
		namesAcceptedCondition.Reason = "ListKindConflict"
		namesAcceptedCondition.Message = err.Error()
	} else {
		newNames.ListKind = requestedNames.ListKind
	}

	newNames.Categories = requestedNames.Categories

	// if we haven't changed the condition, then our names must be good.
	if namesAcceptedCondition.Status == apiextensionsv1.ConditionUnknown {
		namesAcceptedCondition.Status = apiextensionsv1.ConditionTrue
		namesAcceptedCondition.Reason = "NoConflicts"
		namesAcceptedCondition.Message = "no conflicts found"
	}

	// set EstablishedCondition initially to false, then set it to true in establishing controller.
	// The Establishing Controller will see the NamesAccepted condition when it arrives through the shared informer.
	// At that time the API endpoint handler will serve the endpoint, avoiding a race
	// which we had if we set Established to true here.
	establishedCondition := apiextensionsv1.CustomResourceDefinitionCondition{
		Type:    apiextensionsv1.Established,
		Status:  apiextensionsv1.ConditionFalse,
		Reason:  "NotAccepted",
		Message: "not all names are accepted",
	}
	if old := apiextensionshelpers.FindCRDCondition(in, apiextensionsv1.Established); old != nil {
		establishedCondition = *old
	}
	if establishedCondition.Status != apiextensionsv1.ConditionTrue && namesAcceptedCondition.Status == apiextensionsv1.ConditionTrue {
		establishedCondition = apiextensionsv1.CustomResourceDefinitionCondition{
			Type:    apiextensionsv1.Established,
			Status:  apiextensionsv1.ConditionFalse,
			Reason:  "Installing",
			Message: "the initial names have been accepted",
		}
	}

	return newNames, namesAcceptedCondition, establishedCondition
}

func equalToAcceptedOrFresh(requestedName, acceptedName string, usedNames sets.String) error {
	if requestedName == acceptedName {
		return nil
	}
	if !usedNames.Has(requestedName) {
		return nil
	}

	return fmt.Errorf("%q is already in use", requestedName)
}

// sync 是控制器的核心协调函数。它接收一个工作项的 key，通常是 "namespace/name" 或 "name"（对于集群级资源）。
func (c *NamingConditionController) sync(key string) error {
	// --- 1. 从本地缓存获取最新的 CRD 对象 ---
	// `c.crdLister.Get(key)` 从 Informer 的本地缓存中获取 CRD 对象。这非常快，不会访问 API Server。
	inCustomResourceDefinition, err := c.crdLister.Get(key)
	if apierrors.IsNotFound(err) {
		// CRD was deleted and has freed its names.
		// Reconsider all other CRDs in the same group.
		// 如果在缓存中找不到，说明这个 CRD 已经被删除了。
		// 一个 CRD 被删除后，它之前占用的名称就被释放了。这可能会让同组中其他之前因冲突而失败的 CRD 有机会成功。
		// 所以，我们需要重新检查该组中的所有其他 CRD。
		if err := c.requeueAllOtherGroupCRDs(key); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	// --- 2. 快速路径优化：检查是否需要处理 ---
	// 如果 .spec.names (用户期望的名称) 和 .status.acceptedNames (系统已接受的名称) 完全相同，
	// 说明这个 CRD 的名称已经稳定，并且没有新的变更请求，我们无需做任何事情。
	// 这是一种常见的优化，可以避免不必要的计算和 API 调用。
	// Skip checking names if Spec and Status names are same.
	if equality.Semantic.DeepEqual(inCustomResourceDefinition.Spec.Names, inCustomResourceDefinition.Status.AcceptedNames) {
		return nil
	}
	// --- 3. 核心计算逻辑 ---
	// 调用 `calculateNamesAndConditions` 来执行实际的名称冲突检查。
	// 这个函数会查看集群中所有其他的 CRD，判断当前 CRD 的 `.spec.names` 是否可用。
	// 它返回：
	// - `acceptedNames`: 最终被系统接受的名称。如果无冲突，它等于 `.spec.names`；否则可能是空的。
	// - `namingCondition`: 一个描述名称是否被接受的 Condition 对象 (`NamesAccepted: True/False`)。
	// - `establishedCondition`: 一个描述 CRD 是否已建立的 Condition 对象。
	acceptedNames, namingCondition, establishedCondition := c.calculateNamesAndConditions(inCustomResourceDefinition)
	// --- 4. 再次优化：检查状态是否真的改变 ---
	// 比较计算出的新状态 (`acceptedNames`, `namingCondition`) 和 CRD 当前的状态。
	// 如果没有任何变化，我们也不需要执行更新操作。
	// nothing to do if accepted names and NamesAccepted condition didn't change
	if reflect.DeepEqual(inCustomResourceDefinition.Status.AcceptedNames, acceptedNames) &&
		apiextensionshelpers.IsCRDConditionEquivalent(&namingCondition, apiextensionshelpers.FindCRDCondition(inCustomResourceDefinition, apiextensionsv1.NamesAccepted)) {
		return nil
	}
	// --- 5. 更新 CRD 状态 ---
	// 如果状态确实发生了变化，就需要通过 API Server 更新 CRD 的 `.status` 字段。

	// 创建一个 CRD 对象的深拷贝，因为我们不能直接修改从缓存中获取的对象。
	crd := inCustomResourceDefinition.DeepCopy()
	// 将新计算出的状态应用到这个深拷贝对象上。
	crd.Status.AcceptedNames = acceptedNames
	apiextensionshelpers.SetCRDCondition(crd, namingCondition)
	apiextensionshelpers.SetCRDCondition(crd, establishedCondition)
	// 调用 `crdClient` 的 `UpdateStatus` 方法，将变更提交到 API Server。
	// 注意：只更新 `.status` 子资源，而不是整个 CRD 对象。这是最佳实践。
	updatedObj, err := c.crdClient.CustomResourceDefinitions().UpdateStatus(context.TODO(), crd, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// deleted or changed in the meantime, we'll get called again
		// 如果返回 NotFound 或 Conflict 错误：
		// - NotFound: CRD 在我们计算和更新的间隙被删除了。
		// - Conflict: 在我们更新的间隙，有其他人也更新了这个 CRD，导致我们的版本过时了。
		// 这两种情况都是正常的，Informer 会再次把这个 CRD 的变更事件发给我们，所以我们什么都不用做，直接返回 nil。
		return nil
	}
	if err != nil {
		return err
	}

	// if the update was successful, go ahead and add the entry to the mutation cache
	// --- 6. 更新本地缓存和触发后续检查 ---
	// 如果 `UpdateStatus` 调用成功，API Server 会返回更新后的对象 `updatedObj`。
	// 将这个最新的对象更新到 `c.crdMutationCache` 中。这是一个本地的、写操作的缓存，
	// 用于防止控制器因读到旧的缓存（Informer 的缓存有延迟）而做出错误判断。
	c.crdMutationCache.Mutation(updatedObj)

	// we updated our status, so we may be releasing a name.  When this happens, we need to rekick everything in our group
	// if we fail to rekick, just return as normal.  We'll get everything on a resync
	// 因为我们刚刚更新了一个 CRD 的状态，这可能意味着它释放了一个之前占用的名称，
	// 或者占用了新的名称。这可能会影响到同组中的其他 CRD。
	// 因此，再次触发对同组中所有其他 CRD 的重新检查。
	if err := c.requeueAllOtherGroupCRDs(key); err != nil {
		// 如果触发失败，也不算致命错误，因为定期的 resync 最终会修正所有状态。

		return err
	}

	return nil
}

func (c *NamingConditionController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting NamingConditionController")
	defer klog.Info("Shutting down NamingConditionController")

	if !cache.WaitForCacheSync(stopCh, c.crdSynced) {
		return
	}

	// only start one worker thread since its a slow moving API and the naming conflict resolution bits aren't thread-safe
	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

func (c *NamingConditionController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
func (c *NamingConditionController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncFn(key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	c.queue.AddRateLimited(key)

	return true
}

func (c *NamingConditionController) enqueue(obj *apiextensionsv1.CustomResourceDefinition) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

func (c *NamingConditionController) addCustomResourceDefinition(obj interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Adding %s", castObj.Name)
	c.enqueue(castObj)
}

func (c *NamingConditionController) updateCustomResourceDefinition(obj, _ interface{}) {
	castObj := obj.(*apiextensionsv1.CustomResourceDefinition)
	klog.V(4).Infof("Updating %s", castObj.Name)
	c.enqueue(castObj)
}

func (c *NamingConditionController) deleteCustomResourceDefinition(obj interface{}) {
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
	klog.V(4).Infof("Deleting %q", castObj.Name)
	c.enqueue(castObj)
}

func (c *NamingConditionController) requeueAllOtherGroupCRDs(name string) error {
	pluralGroup := strings.SplitN(name, ".", 2)
	list, err := c.crdLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, curr := range list {
		if curr.Spec.Group == pluralGroup[1] && curr.Name != name {
			c.queue.Add(curr.Name)
		}
	}
	return nil
}
