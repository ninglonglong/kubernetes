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

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/server/healthz"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// PostStartHookFunc is a function that is called after the server has started.
// It must properly handle cases like:
//  1. asynchronous start in multiple API server processes
//  2. conflicts between the different processes all trying to perform the same action
//  3. partially complete work (API server crashes while running your hook)
//  4. API server access **BEFORE** your hook has completed
//
// Think of it like a mini-controller that is super privileged and gets to run in-process
// If you use this feature, tag @deads2k on github who has promised to review code for anyone's PostStartHook
// until it becomes easier to use.
type PostStartHookFunc func(context PostStartHookContext) error

// PreShutdownHookFunc is a function that can be added to the shutdown logic.
type PreShutdownHookFunc func() error

// PostStartHookContext provides information about this API server to a PostStartHookFunc
type PostStartHookContext struct {
	// LoopbackClientConfig is a config for a privileged loopback connection to the API server
	LoopbackClientConfig *restclient.Config
	// Context gets cancelled when the server stops.
	context.Context
}

// PostStartHookProvider is an interface in addition to provide a post start hook for the api server
type PostStartHookProvider interface {
	PostStartHook() (string, PostStartHookFunc, error)
}

type postStartHookEntry struct {
	hook PostStartHookFunc
	// originatingStack holds the stack that registered postStartHooks. This allows us to show a more helpful message
	// for duplicate registration.
	originatingStack string

	// done will be closed when the postHook is finished
	done chan struct{}
}

type PostStartHookConfigEntry struct {
	hook PostStartHookFunc
	// originatingStack holds the stack that registered postStartHooks. This allows us to show a more helpful message
	// for duplicate registration.
	originatingStack string
}

type preShutdownHookEntry struct {
	hook PreShutdownHookFunc
}

// AddPostStartHook allows you to add a PostStartHook.
func (s *GenericAPIServer) AddPostStartHook(name string, hook PostStartHookFunc) error {
	if len(name) == 0 {
		return fmt.Errorf("missing name")
	}
	if hook == nil {
		return fmt.Errorf("hook func may not be nil: %q", name)
	}
	if s.disabledPostStartHooks.Has(name) {
		klog.V(1).Infof("skipping %q because it was explicitly disabled", name)
		return nil
	}

	s.postStartHookLock.Lock()
	defer s.postStartHookLock.Unlock()

	if s.postStartHooksCalled {
		return fmt.Errorf("unable to add %q because PostStartHooks have already been called", name)
	}
	if postStartHook, exists := s.postStartHooks[name]; exists {
		// this is programmer error, but it can be hard to debug
		return fmt.Errorf("unable to add %q because it was already registered by: %s", name, postStartHook.originatingStack)
	}

	// done is closed when the poststarthook is finished.  This is used by the health check to be able to indicate
	// that the poststarthook is finished
	done := make(chan struct{})
	if err := s.AddBootSequenceHealthChecks(postStartHookHealthz{name: "poststarthook/" + name, done: done}); err != nil {
		return err
	}
	s.postStartHooks[name] = postStartHookEntry{hook: hook, originatingStack: string(debug.Stack()), done: done}

	return nil
}

// AddPostStartHookOrDie allows you to add a PostStartHook, but dies on failure
func (s *GenericAPIServer) AddPostStartHookOrDie(name string, hook PostStartHookFunc) {
	if err := s.AddPostStartHook(name, hook); err != nil {
		klog.Fatalf("Error registering PostStartHook %q: %v", name, err)
	}
}

// AddPreShutdownHook allows you to add a PreShutdownHook.
func (s *GenericAPIServer) AddPreShutdownHook(name string, hook PreShutdownHookFunc) error {
	if len(name) == 0 {
		return fmt.Errorf("missing name")
	}
	if hook == nil {
		return nil
	}

	s.preShutdownHookLock.Lock()
	defer s.preShutdownHookLock.Unlock()

	if s.preShutdownHooksCalled {
		return fmt.Errorf("unable to add %q because PreShutdownHooks have already been called", name)
	}
	if _, exists := s.preShutdownHooks[name]; exists {
		return fmt.Errorf("unable to add %q because it is already registered", name)
	}

	s.preShutdownHooks[name] = preShutdownHookEntry{hook: hook}

	return nil
}

// AddPreShutdownHookOrDie allows you to add a PostStartHook, but dies on failure
func (s *GenericAPIServer) AddPreShutdownHookOrDie(name string, hook PreShutdownHookFunc) {
	if err := s.AddPreShutdownHook(name, hook); err != nil {
		klog.Fatalf("Error registering PreShutdownHook %q: %v", name, err)
	}
}

// RunPostStartHooks runs the PostStartHooks for the server.
// RunPostStartHooks 的作用是：执行所有已经注册到 GenericAPIServer 的 PostStartHook (启动后钩子函数)。
// 这个函数会在服务器成功监听端口之后被调用。
//
// 参数:
//   - ctx: 一个父 context，它将被传递给每一个钩子函数，用于控制钩子函数内部 goroutine 的生命周期。
func (s *GenericAPIServer) RunPostStartHooks(ctx context.Context) {
	// 步骤 1: 加锁以保护内部状态
	// `postStartHookLock` 是一个互斥锁，用于保护 `postStartHooks` map 和 `postStartHooksCalled` 标志。
	// 这确保了在并发场景下（虽然在正常启动流程中这里不是并发的，但作为健壮的库代码，加锁是必要的），
	// 对这些共享资源的访问是线程安全的。
	s.postStartHookLock.Lock()

	defer s.postStartHookLock.Unlock()
	// 步骤 2: 设置标志位，表示钩子已经被调用
	// 这个 `postStartHooksCalled` 标志位的作用是防止 `RunPostStartHooks` 被重复调用。
	// 在 `AddPostStartHook` 方法中会检查这个标志，如果为 true，就会 panic，
	// 因为在钩子已经开始运行后再添加新的钩子是不安全的。
	s.postStartHooksCalled = true

	// 步骤 3: 创建 PostStartHookContext
	// `PostStartHookContext` 是一个结构体，它将所有钩子函数可能需要的共享资源打包在一起。
	// 这是一种很好的“依赖注入”实践，避免了钩子函数需要访问全局变量。
	context := PostStartHookContext{
		// LoopbackClientConfig: 提供了用于连接 apiserver 自身的客户端配置。
		// 很多控制器需要调用 apiserver 的 API，这个配置就是它们的“通行证”。
		LoopbackClientConfig: s.LoopbackClientConfig,
		// Context: 传入的父 context，它的 Done() channel 将被用作所有钩子函数中
		// 后台 goroutine 的停止信号。当 apiserver 关闭时，这个 context 会被取消，
		// 从而通知所有控制器停止工作。
		Context: ctx,
	}
	klog.V(2).InfoS("Running post-start hooks", "count", len(s.postStartHooks))
	// 步骤 4: 遍历并并发启动所有钩子函数
	// `s.postStartHooks` 是一个 map，key 是钩子的名称 (用于日志和调试)，value 是钩子函数本身。
	for hookName, hookEntry := range s.postStartHooks {
		// **核心步骤：为每一个钩子函数都启动一个新的 goroutine！**
		// 这意味着所有的 PostStartHook 都是并发执行的，它们不会相互阻塞。
		// 例如，`apiservice-registration-controller` 的启动不会等待 `crd-registration-controller`。
		go runPostStartHook(hookName, hookEntry, context)
	}
}

// RunPreShutdownHooks runs the PreShutdownHooks for the server
func (s *GenericAPIServer) RunPreShutdownHooks() error {
	var errorList []error

	s.preShutdownHookLock.Lock()
	defer s.preShutdownHookLock.Unlock()
	s.preShutdownHooksCalled = true

	for hookName, hookEntry := range s.preShutdownHooks {
		if err := runPreShutdownHook(hookName, hookEntry); err != nil {
			errorList = append(errorList, err)
		}
	}
	return utilerrors.NewAggregate(errorList)
}

// isPostStartHookRegistered checks whether a given PostStartHook is registered
func (s *GenericAPIServer) isPostStartHookRegistered(name string) bool {
	s.postStartHookLock.Lock()
	defer s.postStartHookLock.Unlock()
	_, exists := s.postStartHooks[name]
	return exists
}

func runPostStartHook(name string, entry postStartHookEntry, context PostStartHookContext) {
	var err error
	func() {
		// don't let the hook *accidentally* panic and kill the server
		defer utilruntime.HandleCrash()
		err = entry.hook(context)
	}()
	// if the hook intentionally wants to kill server, let it.
	if err != nil {
		klog.Fatalf("PostStartHook %q failed: %v", name, err)
	}
	close(entry.done)
}

func runPreShutdownHook(name string, entry preShutdownHookEntry) error {
	var err error
	func() {
		// don't let the hook *accidentally* panic and kill the server
		defer utilruntime.HandleCrash()
		err = entry.hook()
	}()
	if err != nil {
		return fmt.Errorf("PreShutdownHook %q failed: %v", name, err)
	}
	return nil
}

// postStartHookHealthz implements a healthz check for poststarthooks.  It will return a "hookNotFinished"
// error until the poststarthook is finished.
type postStartHookHealthz struct {
	name string

	// done will be closed when the postStartHook is finished
	done chan struct{}
}

var _ healthz.HealthChecker = postStartHookHealthz{}

func (h postStartHookHealthz) Name() string {
	return h.name
}

var errHookNotFinished = errors.New("not finished")

func (h postStartHookHealthz) Check(req *http.Request) error {
	select {
	case <-h.done:
		return nil
	default:
		return errHookNotFinished
	}
}
