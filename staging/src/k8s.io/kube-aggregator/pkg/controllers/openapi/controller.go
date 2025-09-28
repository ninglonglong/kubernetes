/*
Copyright 2016 The Kubernetes Authors.

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

package openapi

import (
	"fmt"
	"net/http"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/controllers/openapi/aggregator"
)

const (
	successfulUpdateDelay      = time.Minute
	successfulUpdateDelayLocal = time.Second
	failedUpdateMaxExpDelay    = time.Hour
)

type syncAction int

const (
	syncRequeue syncAction = iota
	syncRequeueRateLimited
	syncNothing
)

// AggregationController periodically check for changes in OpenAPI specs of APIServices and update/remove
// them if necessary.
type AggregationController struct {
	openAPIAggregationManager aggregator.SpecAggregator
	queue                     workqueue.TypedRateLimitingInterface[string]
	downloader                *aggregator.Downloader

	// To allow injection for testing.
	syncHandler func(key string) (syncAction, error)
}

// NewAggregationController creates new OpenAPI aggregation controller.
// NewAggregationController 是 OpenAPI 聚合控制器 (AggregationController) 的构造函数。
// 它的作用是创建一个新的控制器实例，并初始化其所有必要的组件。
// 这个控制器是 Kubernetes 控制器模式的一个经典实现，它负责监控 APIService 资源的变化，
// 并根据这些变化来动态地更新聚合后的 OpenAPI 规范。
func NewAggregationController(
	// downloader 是一个 OpenAPI 规范下载器。
	// 当控制器检测到一个 APIService 更新时，它会使用这个 downloader
	// 从该 APIService 指向的后端服务（如 metrics-server）下载其独立的 OpenAPI 规范。
	downloader *aggregator.Downloader,
	// openAPIAggregationManager 是一个 OpenAPI 规范聚合器。
	// 在 downloader 下载了所有必要的规范之后，这个 manager 负责将这些独立的规范
	// 合并（聚合）成一个单一的、完整的 OpenAPI 文档，并更新到 apiserver 的 /openapi/v2 端点
	openAPIAggregationManager aggregator.SpecAggregator) *AggregationController {
	c := &AggregationController{
		// 创建并初始化 AggregationController 结构体实例。
		// 依赖注入：将传入的 openAPIAggregationManager 实例赋值给控制器内部的字段。
		// 这个 manager 将在核心同步逻辑 `sync` 方法中被调用，以执行实际的聚合工作。
		openAPIAggregationManager: openAPIAggregationManager,
		// 初始化工作队列 (workqueue)。这是 Kubernetes 控制器模式的“心脏”。
		// 它是一个带有速率限制的、类型安全的工作队列，用于解耦“事件检测”和“事件处理”。
		// 当 Informer 检测到 APIService 变化时，只会将该 APIService 的 key（一个字符串）放入这个队列，
		// 而不会立即处理它。一个或多个 worker goroutine 会从这个队列中取出 key 进行处理。
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			// 配置速率限制器：这里使用了一个指数级退避的失败率限制器。
			// - 如果一个 key（任务）处理成功，它会在 `successfulUpdateDelay` 这么短的延迟后才能再次入队。
			// - 如果一个 key 处理失败（例如，下载后端 spec 失败），它会被重新放回队列，
			//   但其重试的延迟会以指数级增加，直到达到 `failedUpdateMaxExpDelay` 的上限。
			// 这样做可以避免在后端服务故障时，控制器对其进行无休止的、高频率的“攻击”。
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](successfulUpdateDelay, failedUpdateMaxExpDelay),
			// 为队列命名，这对于监控和调试非常有用。
			// 这个队列的 metrics 会以 "open_api_aggregation_controller_..." 的形式暴露出来。
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "open_api_aggregation_controller"},
		),

		// 依赖注入：将传入的 downloader 实例赋值给控制器内部的字段。
		// 这个 downloader 同样将在 `sync` 方法中被使用。
		downloader: downloader,
	}
	// 设置核心同步处理函数 (sync handler)。
	// `c.sync` 是定义在 AggregationController 上的一个方法（未在此处显示），
	// 它包含了处理单个队列项（即一个 APIService 的 key）的完整业务逻辑。
	// 将这个方法赋值给 `syncHandler` 字段，使得控制器的通用运行逻辑（`Run` 方法中的 worker）
	// 可以调用这个具体的业务处理函数。
	c.syncHandler = c.sync

	return c
}

// Run starts OpenAPI AggregationController
func (c *AggregationController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting OpenAPI AggregationController")
	defer klog.Info("Shutting down OpenAPI AggregationController")

	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

func (c *AggregationController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
func (c *AggregationController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	defer c.queue.Done(key)
	if quit {
		return false
	}
	klog.V(4).Infof("OpenAPI AggregationController: Processing item %s", key)

	action, err := c.syncHandler(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("loading OpenAPI spec for %q failed with: %v", key, err))
	}

	switch action {
	case syncRequeue:
		c.queue.AddAfter(key, successfulUpdateDelay)
	case syncRequeueRateLimited:
		klog.Infof("OpenAPI AggregationController: action for item %s: Rate Limited Requeue.", key)
		c.queue.AddRateLimited(key)
	case syncNothing:
		c.queue.Forget(key)
	}

	return true
}

// sync 方法是 AggregationController 的核心处理函数。
// 每当 worker goroutine 从工作队列中取出一个 `key` 时，这个函数就会被调用。
// `key` 通常是被更改的 APIService 对象的名称（例如 "v1beta1.metrics.k8s.io"）。
//
// 这个函数的目标是：将由 `key` 标识的 APIService 的当前状态，与聚合后的 OpenAPI 规范的状态进行同步。
//
// 返回值 (syncAction, error) 告诉调用者（worker 协程）处理完这个 key 之后应该采取什么行动。
func (c *AggregationController) sync(key string) (syncAction, error) {
	// 1. 【执行核心任务】
	// 调用 openAPIAggregationManager 的 UpdateAPIServiceSpec 方法，并把 key 传递给它。
	// 这是整个同步过程的核心所在。`UpdateAPIServiceSpec` 内部会执行以下一系列操作：
	//   a. 根据 `key` 从 Informer 缓存中获取最新的 APIService 对象。
	//   b. 如果 APIService 对象存在，就使用 `downloader` 从其后端服务下载 OpenAPI 规范。
	//   c. 将下载到的规范，更新到聚合器（manager）内部的聚合视图中。
	//   d. 如果 APIService 对象不存在（已被删除），则从聚合视图中移除其对应的规范。
	if err := c.openAPIAggregationManager.UpdateAPIServiceSpec(key); err != nil {
		if err == aggregator.ErrAPIServiceNotFound {
			return syncNothing, nil
		} else {
			return syncRequeueRateLimited, err
		}
	}
	// 3. 【成功处理分支】
	// 如果 `UpdateAPIServiceSpec` 执行成功，没有返回任何错误。
	// 这意味着我们成功地更新了 APIService 的规范（无论是添加、更新还是删除）。
	// 返回 `syncRequeue` 和 `nil` 错误，告诉 worker：
	// “这次任务成功了。但请在一段固定的延迟后（即构造函数中配置的 `successfulUpdateDelay`），
	// 再次将这个 key 放入队列。”
	// 这是一个周期性同步的策略，确保即使没有新的事件发生，我们也能定期刷新和检查状态，
	// 这有助于处理一些静默的失败或状态漂移。
	return syncRequeue, nil
}

// AddAPIService adds a new API Service to OpenAPI Aggregation.
func (c *AggregationController) AddAPIService(handler http.Handler, apiService *v1.APIService) {
	if apiService.Spec.Service == nil {
		return
	}
	if err := c.openAPIAggregationManager.AddUpdateAPIService(apiService, handler); err != nil {
		utilruntime.HandleError(fmt.Errorf("adding %q to AggregationController failed with: %v", apiService.Name, err))
	}
	c.queue.AddAfter(apiService.Name, time.Second)
}

// UpdateAPIService updates API Service's info and handler.
func (c *AggregationController) UpdateAPIService(handler http.Handler, apiService *v1.APIService) {
	if apiService.Spec.Service == nil {
		return
	}
	if err := c.openAPIAggregationManager.UpdateAPIServiceSpec(apiService.Name); err != nil {
		utilruntime.HandleError(fmt.Errorf("Error updating APIService %q with err: %v", apiService.Name, err))
	}
	key := apiService.Name
	if c.queue.NumRequeues(key) > 0 {
		// The item has failed before. Remove it from failure queue and
		// update it in a second
		c.queue.Forget(key)
		c.queue.AddAfter(key, time.Second)
	}
	// Else: The item has been succeeded before and it will be updated soon (after successfulUpdateDelay)
	// we don't add it again as it will cause a duplication of items.
}

// RemoveAPIService removes API Service from OpenAPI Aggregation Controller.
func (c *AggregationController) RemoveAPIService(apiServiceName string) {
	c.openAPIAggregationManager.RemoveAPIService(apiServiceName)
	// This will only remove it if it was failing before. If it was successful, processNextWorkItem will figure it out
	// and will not add it again to the queue.
	c.queue.Forget(apiServiceName)
}
