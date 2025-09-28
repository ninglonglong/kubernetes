/*
Copyright 2015 The Kubernetes Authors.

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

package endpoints

import (
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	restful "github.com/emicklei/go-restful/v3"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/endpoints/deprecation"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	utilwarning "k8s.io/apiserver/pkg/endpoints/warning"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storageversion"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	versioninfo "k8s.io/component-base/version"
)

const (
	RouteMetaGVK              = "x-kubernetes-group-version-kind"
	RouteMetaSelectableFields = "x-kubernetes-selectable-fields"
	RouteMetaAction           = "x-kubernetes-action"
)

type APIInstaller struct {
	group             *APIGroupVersion
	prefix            string // Path prefix where API resources are to be registered.
	minRequestTimeout time.Duration
}

// Struct capturing information about an action (MethodGet, MethodPost, MethodWatch, MethodProxy, etc).
type action struct {
	Verb          string               // Verb identifying the action (MethodGet, MethodPost, MethodWatch, MethodProxy, etc).
	Path          string               // The path of the action
	Params        []*restful.Parameter // List of parameters associated with the action.
	Namer         handlers.ScopeNamer
	AllNamespaces bool // true iff the action is namespaced but works on aggregate result for all namespaces
}

func ConvertGroupVersionIntoToDiscovery(list []metav1.APIResource) ([]apidiscoveryv2.APIResourceDiscovery, error) {
	var apiResourceList []apidiscoveryv2.APIResourceDiscovery
	parentResources := make(map[string]int)

	// Loop through all top-level resources
	for _, r := range list {
		if strings.Contains(r.Name, "/") {
			// Skip subresources for now so we can get the list of resources
			continue
		}

		var scope apidiscoveryv2.ResourceScope
		if r.Namespaced {
			scope = apidiscoveryv2.ScopeNamespace
		} else {
			scope = apidiscoveryv2.ScopeCluster
		}

		resource := apidiscoveryv2.APIResourceDiscovery{
			Resource: r.Name,
			Scope:    scope,
			ResponseKind: &metav1.GroupVersionKind{
				Group:   r.Group,
				Version: r.Version,
				Kind:    r.Kind,
			},
			Verbs:            r.Verbs,
			ShortNames:       r.ShortNames,
			Categories:       r.Categories,
			SingularResource: r.SingularName,
		}
		apiResourceList = append(apiResourceList, resource)
		parentResources[r.Name] = len(apiResourceList) - 1
	}

	// Loop through all subresources
	for _, r := range list {
		// Split resource name and subresource name
		split := strings.SplitN(r.Name, "/", 2)

		if len(split) != 2 {
			// Skip parent resources
			continue
		}

		var scope apidiscoveryv2.ResourceScope
		if r.Namespaced {
			scope = apidiscoveryv2.ScopeNamespace
		} else {
			scope = apidiscoveryv2.ScopeCluster
		}

		parentidx, exists := parentResources[split[0]]
		if !exists {
			// If a subresource exists without a parent, create a parent
			apiResourceList = append(apiResourceList, apidiscoveryv2.APIResourceDiscovery{
				Resource: split[0],
				Scope:    scope,
				// avoid nil panics in v0.26.0-v0.26.3 client-go clients
				// see https://github.com/kubernetes/kubernetes/issues/118361
				ResponseKind: &metav1.GroupVersionKind{},
			})
			parentidx = len(apiResourceList) - 1
			parentResources[split[0]] = parentidx
		}

		if apiResourceList[parentidx].Scope != scope {
			return nil, fmt.Errorf("Error: Parent %s (scope: %s) and subresource %s (scope: %s) scope do not match", split[0], apiResourceList[parentidx].Scope, split[1], scope)
			//
		}

		subresource := apidiscoveryv2.APISubresourceDiscovery{
			Subresource: split[1],
			Verbs:       r.Verbs,
			// avoid nil panics in v0.26.0-v0.26.3 client-go clients
			// see https://github.com/kubernetes/kubernetes/issues/118361
			ResponseKind: &metav1.GroupVersionKind{},
		}
		if r.Kind != "" {
			subresource.ResponseKind = &metav1.GroupVersionKind{
				Group:   r.Group,
				Version: r.Version,
				Kind:    r.Kind,
			}
		}
		apiResourceList[parentidx].Subresources = append(apiResourceList[parentidx].Subresources, subresource)
	}

	return apiResourceList, nil
}

// An interface to see if one storage supports override its default verb for monitoring
type StorageMetricsOverride interface {
	// OverrideMetricsVerb gives a storage object an opportunity to override the verb reported to the metrics endpoint
	OverrideMetricsVerb(oldVerb string) (newVerb string)
}

// An interface to see if an object supports swagger documentation as a method
type documentable interface {
	SwaggerDoc() map[string]string
}

// toDiscoveryKubeVerb maps an action.Verb to the logical kube verb, used for discovery
var toDiscoveryKubeVerb = map[string]string{
	request.MethodConnect:          "", // do not list in discovery.
	request.MethodDelete:           "delete",
	request.MethodDeleteCollection: "deletecollection",
	request.MethodGet:              "get",
	request.MethodList:             "list",
	request.MethodPatch:            "patch",
	request.MethodPost:             "create",
	request.MethodProxy:            "proxy",
	request.MethodPut:              "update",
	request.MethodWatch:            "watch",
	request.MethodWatchList:        "watch",
}

// Install handlers for API resources.
// Install 是 APIInstaller 的主方法。它负责创建并填充一个 `restful.WebService`，
// 这个 WebService 包含了该 API 版本下所有资源的 REST 路由。
// 它返回：
// - `apiResources`: 用于服务发现的 APIResource 列表。
// - `resourceInfos`: 用于 StorageVersion API 的 ResourceInfo 列表。
// - `ws`: 包含所有已注册路由的 WebService 对象。
// - `errors`: 安装过程中遇到的错误列表。
func (a *APIInstaller) Install() ([]metav1.APIResource, []*storageversion.ResourceInfo, *restful.WebService, []error) {
	var apiResources []metav1.APIResource
	var resourceInfos []*storageversion.ResourceInfo
	var errors []error

	// `newWebService` 会创建一个新的 `restful.WebService`，并设置其根路径，
	// 例如 `/apis/apps/v1`。
	ws := a.newWebService()

	// Register the paths in a deterministic (sorted) order to get a deterministic swagger spec.

	// --- 核心逻辑：按确定性顺序安装资源 ---
	// 为了得到一个确定性的 Swagger/OpenAPI 规范（这对于 API 文档和客户端生成的稳定性至关重要），
	// 我们必须按照一个固定的顺序来注册路由。这里选择按路径的字母顺序。

	// 从 a.group.Storage (这是一个 map[string]rest.Storage) 中提取所有的路径（也就是资源名）。
	paths := make([]string, len(a.group.Storage))
	var i int = 0
	for path := range a.group.Storage {
		paths[i] = path
		i++
	}
	// 对路径进行字符串排序。
	sort.Strings(paths)
	// 按照排好序的路径，遍历并注册每个资源。
	for _, path := range paths {
		// `registerResourceHandlers` 是实际干活的函数。
		// 它会检查 `a.group.Storage[path]` 这个 `rest.Storage` 实现了哪些接口
		// (如 Getter, Lister, Creater, Updater 等)，
		// 然后为每个实现的接口，在 `ws` (WebService) 上创建对应的 HTTP 路由。
		// 例如，如果实现了 Getter，它会创建一个 `GET /.../{name}` 的路由。
		apiResource, resourceInfo, err := a.registerResourceHandlers(path, a.group.Storage[path], ws)
		// 收集 `registerResourceHandlers` 的返回结果。
		if err != nil {
			errors = append(errors, fmt.Errorf("error in registering resource: %s, %v", path, err))
		}
		if apiResource != nil {
			// `apiResource` 是一个描述这个资源的元数据对象，用于服务发现。
			apiResources = append(apiResources, *apiResource)
		}
		if resourceInfo != nil {
			// `resourceInfo` 是一个包含存储版本信息的对象，用于 StorageVersion API。
			resourceInfos = append(resourceInfos, resourceInfo)
		}
	}
	// 返回所有收集到的信息。
	return apiResources, resourceInfos, ws, errors
}

// newWebService creates a new restful webservice with the api installer's prefix and version.
func (a *APIInstaller) newWebService() *restful.WebService {
	ws := new(restful.WebService)
	// --- 1. 设置根路径 (Root Path) ---
	// `ws.Path(a.prefix)` 是最关键的一步。它定义了这个 WebService 下所有路由的共同前缀。
	// `a.prefix` 通常是 `/apis/<group>/<version>` (例如 `/apis/apps/v1`) 或 `/api/v1`。
	// 这意味着，之后在这个 `ws` 上注册的任何路由，比如针对 "deployments" 的，
	// 它的完整路径会自动变成 `/apis/apps/v1/deployments`。
	ws.Path(a.prefix)
	// a.prefix contains "prefix/group/version"
	// 为这个 WebService 设置一个描述性文档，这会出现在自动生成的 API 文档中。
	ws.Doc("API at " + a.prefix)
	// Backwards compatibility, we accepted objects with empty content-type at V1.
	// If we stop using go-restful, we can default empty content-type to application/json on an
	// endpoint by endpoint basis
	// --- 2. 设置可接受的请求体类型 (Consumes) ---
	// `ws.Consumes("*/*")` 表示这个 WebService 接受任何 Content-Type 的请求。
	// 注释中解释了这是为了向后兼容：在 Kubernetes v1 的早期，API 服务器接受过
	// Content-Type 为空的请求（并将其默认为 application/json）。
	// 虽然现在不推荐这样做，但为了保持兼容性，这里设置为接受所有类型。
	// 如果未来不再使用 go-restful，可以在每个端点上更精确地控制这个行为。
	ws.Consumes("*/*")
	// --- 3. 设置可生成的响应体类型 (Produces) ---
	// `negotiation.MediaTypesForSerializer(a.group.Serializer)` 会根据传入的序列化器（Serializer）
	// 来确定这个 API 版本支持哪些响应格式。
	// `a.group.Serializer` 通常是一个支持多种格式的 `NegotiatedSerializer`。
	// - `mediaTypes`: 一般包含 `application/json` 和 `application/yaml`，以及 Protobuf 格式。
	// - `streamMediaTypes`: 专门用于 Watch 操作的流式响应格式，如 `application/json;stream=watch`。
	mediaTypes, streamMediaTypes := negotiation.MediaTypesForSerializer(a.group.Serializer)

	// `ws.Produces(...)` 告诉客户端这个 WebService 能够生成哪些 Content-Type 的响应。
	// 客户端可以在请求的 `Accept` 头中指定它想要的格式，服务器会根据这个列表进行内容协商。
	ws.Produces(append(mediaTypes, streamMediaTypes...)...)
	// --- 4. 设置 API 版本 ---
	// `ws.ApiVersion(...)` 为这个 WebService 关联一个 API 版本字符串（如 "apps/v1"）。
	// 这个信息同样也会被用于生成 API 文档。
	ws.ApiVersion(a.group.GroupVersion.String())

	return ws
}

// calculate the storage gvk, the gvk objects are converted to before persisted to the etcd.
func getStorageVersionKind(storageVersioner runtime.GroupVersioner, storage rest.Storage, typer runtime.ObjectTyper) (schema.GroupVersionKind, error) {
	object := storage.New()
	fqKinds, _, err := typer.ObjectKinds(object)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	gvk, ok := storageVersioner.KindForGroupVersionKinds(fqKinds)
	if !ok {
		return schema.GroupVersionKind{}, fmt.Errorf("cannot find the storage version kind for %v", reflect.TypeOf(object))
	}
	return gvk, nil
}

// GetResourceKind returns the external group version kind registered for the given storage
// object. If the storage object is a subresource and has an override supplied for it, it returns
// the group version kind supplied in the override.
func GetResourceKind(groupVersion schema.GroupVersion, storage rest.Storage, typer runtime.ObjectTyper) (schema.GroupVersionKind, error) {
	// Let the storage tell us exactly what GVK it has
	if gvkProvider, ok := storage.(rest.GroupVersionKindProvider); ok {
		return gvkProvider.GroupVersionKind(groupVersion), nil
	}

	object := storage.New()
	fqKinds, _, err := typer.ObjectKinds(object)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	// a given go type can have multiple potential fully qualified kinds.  Find the one that corresponds with the group
	// we're trying to register here
	fqKindToRegister := schema.GroupVersionKind{}
	for _, fqKind := range fqKinds {
		if fqKind.Group == groupVersion.Group {
			fqKindToRegister = groupVersion.WithKind(fqKind.Kind)
			break
		}
	}
	if fqKindToRegister.Empty() {
		return schema.GroupVersionKind{}, fmt.Errorf("unable to locate fully qualified kind for %v: found %v when registering for %v", reflect.TypeOf(object), fqKinds, groupVersion)
	}

	// group is guaranteed to match based on the check above
	return fqKindToRegister, nil
}

// registerResourceHandlers 负责为一个资源（如 Pods）注册其所有支持的 RESTful HTTP 端点。
// path: 资源的路径，例如 "pods" 或 "nodes/{name}/status"。
// storage: 实现了资源增删改查逻辑的存储后端接口。
// ws: go-restful 框架的 WebService 对象，所有新创建的路由都会被添加到这里。
// 它返回 APIResource 元数据（用于服务发现）、资源信息和可能出现的错误
func (a *APIInstaller) registerResourceHandlers(path string, storage rest.Storage, ws *restful.WebService) (*metav1.APIResource, *storageversion.ResourceInfo, error) {
	// admit 是一个准入控制器链，用于在创建或更新资源时进行校验。
	admit := a.group.Admit
	// 确定用于处理 options 对象的 API 版本（如 CreateOptions, ListOptions）。
	// 通常与资源本身的版本相同，但可以被覆盖。
	optionsExternalVersion := a.group.GroupVersion
	if a.group.OptionsExternalVersion != nil {
		optionsExternalVersion = *a.group.OptionsExternalVersion
	}
	// 将路径分割成主资源和子资源。例如 "nodes/{name}/status" 会被分割成 resource="nodes" 和 subresource="status"。
	resource, subresource, err := splitSubresource(path)
	if err != nil {
		return nil, nil, err
	}
	// 获取当前处理的 API 组和版本，例如 "v1" 或 "apps/v1"。
	group, version := a.group.GroupVersion.Group, a.group.GroupVersion.Version
	// 获取 storage 对象所代表的资源的确切 GroupVersionKind（GVK）。
	fqKindToRegister, err := GetResourceKind(a.group.GroupVersion, storage, a.group.Typer)
	if err != nil {
		return nil, nil, err
	}
	// 使用 Creater 创建一个该资源类型的空对象实例（例如，一个空的 v1.Pod 对象）。
	// 这在序列化/反序列化和生成文档时非常有用。
	versionedPtr, err := a.group.Creater.New(fqKindToRegister)
	if err != nil {
		return nil, nil, err
	}
	defaultVersionedObject := indirectArbitraryPointer(versionedPtr) // 获取指针指向的实际对象。
	kind := fqKindToRegister.Kind                                    // 从 GVK 中提取 Kind，例如 "Pod"。
	isSubresource := len(subresource) > 0                            // 判断当前路径是否是子资源。

	// If there is a subresource, namespace scoping is defined by the parent resource
	// --- 确定资源是否是命名空间作用域的 ---
	var namespaceScoped bool
	if isSubresource {
		// 如果是子资源，它的作用域由其父资源决定。
		// 例如，Pod 是命名空间的，那么 Pod 的 status 子资源也必须是命名空间的。
		parentStorage, ok := a.group.Storage[resource]
		if !ok {
			return nil, nil, fmt.Errorf("missing parent storage: %q", resource)
		}
		scoper, ok := parentStorage.(rest.Scoper) // 检查父资源是否实现了 Scoper 接口。
		if !ok {
			return nil, nil, fmt.Errorf("%q must implement scoper", resource)
		}
		namespaceScoped = scoper.NamespaceScoped() // 获取父资源的作用域。

	} else {
		// 如果是主资源，直接检查它自己是否实现了 Scoper 接口。
		scoper, ok := storage.(rest.Scoper)
		if !ok {
			return nil, nil, fmt.Errorf("%q must implement scoper", resource)
		}
		namespaceScoped = scoper.NamespaceScoped()
	}
	// --- 核心部分：通过接口断言，检查 storage 对象实现了哪些 REST 行为（Verbs） ---
	// 这是一个非常经典的 Go 语言接口驱动设计的例子。
	// 每个变量（如 isCreater, isLister）都是一个布尔值，代表该 storage 是否支持对应的操作。
	// what verbs are supported by the storage, used to know what verbs we support per path
	creater, isCreater := storage.(rest.Creater)                               // 创建新资源 (POST)
	namedCreater, isNamedCreater := storage.(rest.NamedCreater)                // 创建带名字的资源 (通常不用于 REST)
	lister, isLister := storage.(rest.Lister)                                  // 列出资源集合 (GET /pods)
	getter, isGetter := storage.(rest.Getter)                                  // 获取单个资源 (GET /pods/{name})
	getterWithOptions, isGetterWithOptions := storage.(rest.GetterWithOptions) // 支持带选项的 Get
	gracefulDeleter, isGracefulDeleter := storage.(rest.GracefulDeleter)       // 优雅删除单个资源 (DELETE /pods/{name})
	collectionDeleter, isCollectionDeleter := storage.(rest.CollectionDeleter) // 删除资源集合 (DELETE /pods)
	updater, isUpdater := storage.(rest.Updater)                               // 更新资源 (PUT /pods/{name})
	patcher, isPatcher := storage.(rest.Patcher)                               // 部分更新资源 (PATCH /pods/{name})
	watcher, isWatcher := storage.(rest.Watcher)                               // 监控资源变化 (GET /pods?watch=true)
	connecter, isConnecter := storage.(rest.Connecter)                         // 用于 exec, proxy, port-forward 等长连接
	storageMeta, isMetadata := storage.(rest.StorageMetadata)
	storageVersionProvider, isStorageVersionProvider := storage.(rest.StorageVersionProvider)
	gvAcceptor, _ := storage.(rest.GroupVersionAcceptor)
	if !isMetadata {
		storageMeta = defaultStorageMetadata{} // 如果没实现，使用默认的元数据。
	}

	if isNamedCreater {
		isCreater = true // NamedCreater 也算是一种 Creater。
	}
	// --- 为各种操作准备版本化的辅助对象实例 ---
	// 这些空对象用于 API 文档生成（Swagger/OpenAPI）和请求体的反序列化。
	var versionedList interface{}
	if isLister {
		list := lister.NewList()                            // 获取一个空的 List 对象（如 v1.PodList）
		listGVKs, _, err := a.group.Typer.ObjectKinds(list) // 获取 List 对象的 GVK
		if err != nil {
			return nil, nil, err
		}
		// 创建一个特定版本的 List 对象实例。
		versionedListPtr, err := a.group.Creater.New(a.group.GroupVersion.WithKind(listGVKs[0].Kind))
		if err != nil {
			return nil, nil, err
		}
		versionedList = indirectArbitraryPointer(versionedListPtr)
	}
	// 为各种 Options（如 ListOptions, CreateOptions）创建版本化的空对象实例。
	versionedListOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("ListOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedCreateOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("CreateOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedPatchOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("PatchOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedUpdateOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("UpdateOptions"))
	if err != nil {
		return nil, nil, err
	}

	var versionedDeleteOptions runtime.Object
	var versionedDeleterObject interface{}
	deleteReturnsDeletedObject := false
	if isGracefulDeleter {
		// 创建版本化的 Status 对象实例，用于标准的 API 响应。
		versionedDeleteOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind("DeleteOptions"))
		if err != nil {
			return nil, nil, err
		}
		versionedDeleterObject = indirectArbitraryPointer(versionedDeleteOptions)

		if mayReturnFullObjectDeleter, ok := storage.(rest.MayReturnFullObjectDeleter); ok {
			deleteReturnsDeletedObject = mayReturnFullObjectDeleter.DeleteReturnsDeletedObject()
		}
	}

	versionedStatusPtr, err := a.group.Creater.New(optionsExternalVersion.WithKind("Status"))
	if err != nil {
		return nil, nil, err
	}
	versionedStatus := indirectArbitraryPointer(versionedStatusPtr)
	// --- 处理支持特殊选项的 GET 请求 ---
	var (
		getOptions             runtime.Object
		versionedGetOptions    runtime.Object
		getOptionsInternalKind schema.GroupVersionKind
		getSubpath             bool
	)
	if isGetterWithOptions {
		getOptions, getSubpath, _ = getterWithOptions.NewGetOptions() // 获取 Get 操作支持的选项对象。
		getOptionsInternalKinds, _, err := a.group.Typer.ObjectKinds(getOptions)
		if err != nil {
			return nil, nil, err
		}
		getOptionsInternalKind = getOptionsInternalKinds[0]
		// 创建版本化的 GetOptions 对象。
		versionedGetOptions, err = a.group.Creater.New(a.group.GroupVersion.WithKind(getOptionsInternalKind.Kind))
		if err != nil {
			versionedGetOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind(getOptionsInternalKind.Kind))
			if err != nil {
				return nil, nil, err
			}
		}
		isGetter = true // 如果支持 GetterWithOptions，那么它肯定也支持普通的 Get。
	}
	// 为 Watch 操作创建版本化的 WatchEvent 对象。
	var versionedWatchEvent interface{}
	if isWatcher {
		versionedWatchEventPtr, err := a.group.Creater.New(a.group.GroupVersion.WithKind("WatchEvent"))
		if err != nil {
			return nil, nil, err
		}
		versionedWatchEvent = indirectArbitraryPointer(versionedWatchEventPtr)
	}
	// --- 处理 Connect 请求 (如 exec, attach) ---
	var (
		connectOptions             runtime.Object
		versionedConnectOptions    runtime.Object
		connectOptionsInternalKind schema.GroupVersionKind
		connectSubpath             bool
	)
	if isConnecter {
		connectOptions, connectSubpath, _ = connecter.NewConnectOptions() // 获取 Connect 操作支持的选项对象。
		if connectOptions != nil {
			connectOptionsInternalKinds, _, err := a.group.Typer.ObjectKinds(connectOptions)
			if err != nil {
				return nil, nil, err
			}

			connectOptionsInternalKind = connectOptionsInternalKinds[0]
			// 创建版本化的 ConnectOptions 对象。
			versionedConnectOptions, err = a.group.Creater.New(a.group.GroupVersion.WithKind(connectOptionsInternalKind.Kind))
			if err != nil {
				versionedConnectOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind(connectOptionsInternalKind.Kind))
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
	// 只有当一个资源同时支持 List 和 Watch 时，才允许对列表进行 watch（即 /api/v1/pods?watch=true）。
	allowWatchList := isWatcher && isLister // watching on lists is allowed only for kinds that support both watch and list.
	// 定义通用的路径参数，用于 API 文档。
	nameParam := ws.PathParameter("name", "name of the "+kind).DataType("string")
	pathParam := ws.PathParameter("path", "path to the resource").DataType("string")

	params := []*restful.Parameter{}
	actions := []action{} // actions 列表将收集所有注册的路由及其处理函数。

	var resourceKind string
	kindProvider, ok := storage.(rest.KindProvider)
	if ok {
		resourceKind = kindProvider.Kind()
	} else {
		resourceKind = kind // 默认使用从 GVK 中获取的 Kind。
	}
	// 检查资源是否支持以表格形式输出 (kubectl get ... -o wide)。
	tableProvider, isTableProvider := storage.(rest.TableConvertor)
	if isLister && !isTableProvider {
		// All listers must implement TableProvider
		// 这是一个强制要求：所有支持 List 的资源都必须实现 TableConvertor。
		return nil, nil, fmt.Errorf("%q must implement TableConvertor", resource)
	}

	var apiResource metav1.APIResource
	// 如果启用了 StorageVersionHash 特性门控，并且资源提供了存储版本信息，
	// 则计算并记录其存储版本的哈希值。这用于服务发现和缓存验证。
	if utilfeature.DefaultFeatureGate.Enabled(features.StorageVersionHash) &&
		isStorageVersionProvider &&
		storageVersionProvider.StorageVersion() != nil {
		versioner := storageVersionProvider.StorageVersion()
		gvk, err := getStorageVersionKind(versioner, storage, a.group.Typer)
		if err != nil {
			return nil, nil, err
		}
		apiResource.StorageVersionHash = discovery.StorageVersionHash(gvk.Group, gvk.Version, gvk.Kind)
	}

	// Get the list of actions for the given scope.
	switch {
	// --- Case 1: 处理非命名空间作用域的资源（集群级资源，如 Nodes, PersistentVolumes） ---
	case !namespaceScoped:
		// Handle non-namespace scoped resources like nodes.
		// resourcePath 是指资源集合的路径，例如 "nodes"。
		resourcePath := resource
		resourceParams := params
		// itemPath 是指单个资源的路径，例如 "nodes/{name}"。
		itemPath := resourcePath + "/{name}"
		nameParams := append(params, nameParam)      // 包含 {name} 参数的参数列表。
		proxyParams := append(nameParams, pathParam) // 用于代理请求，包含 {name} 和 {path}。
		// 如果是子资源，路径会更长，例如 "nodes/{name}/status"。
		suffix := ""
		if isSubresource {
			suffix = "/" + subresource
			itemPath = itemPath + suffix
			resourcePath = itemPath
			resourceParams = nameParams
		}
		// 填充用于服务发现的 APIResource 结构体。
		apiResource.Name = path
		apiResource.Namespaced = false // 明确指出这是集群级资源。
		apiResource.Kind = resourceKind
		// 创建一个用于生成资源名称的辅助对象，指定为集群作用域。
		namer := handlers.ContextBasedNaming{
			Namer:         a.group.Namer,
			ClusterScoped: true,
		}

		// Handler for standard REST verbs (GET, PUT, POST and DELETE).
		// Add actions at the resource path: /api/apiVersion/resource
		// --- 开始定义要注册的 actions ---
		// appendIf 是一个辅助函数，只有当最后的布尔条件为 true 时，才会将 action 添加到列表中。
		// 这是一个非常简洁的写法，用来根据 storage 实现的接口动态添加路由。

		// 在资源集合路径上（如 /api/v1/nodes）注册的 actions：
		actions = appendIf(actions, action{request.MethodList, resourcePath, resourceParams, namer, false}, isLister)                        // GET /nodes (如果实现了 Lister)
		actions = appendIf(actions, action{request.MethodPost, resourcePath, resourceParams, namer, false}, isCreater)                       // POST /nodes (如果实现了 Creater)
		actions = appendIf(actions, action{request.MethodDeleteCollection, resourcePath, resourceParams, namer, false}, isCollectionDeleter) // DELETE /nodes (如果实现了 CollectionDeleter)
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{request.MethodWatchList, "watch/" + resourcePath, resourceParams, namer, false}, allowWatchList)

		// Add actions at the item path: /api/apiVersion/resource/{name}
		// 在单个资源路径上（如 /api/v1/nodes/{name}）注册的 actions：
		actions = appendIf(actions, action{request.MethodGet, itemPath, nameParams, namer, false}, isGetter) // GET /nodes/{name} (如果实现了 Getter)

		if getSubpath {
			// 如果支持子路径代理，注册一个更通用的路径，如 /nodes/{name}/{path:*}
			actions = appendIf(actions, action{request.MethodGet, itemPath + "/{path:*}", proxyParams, namer, false}, isGetter)
		}
		actions = appendIf(actions, action{request.MethodPut, itemPath, nameParams, namer, false}, isUpdater)            // PUT /nodes/{name} (如果实现了 Updater)
		actions = appendIf(actions, action{request.MethodPatch, itemPath, nameParams, namer, false}, isPatcher)          // PATCH /nodes/{name} (如果实现了 Patcher)
		actions = appendIf(actions, action{request.MethodDelete, itemPath, nameParams, namer, false}, isGracefulDeleter) // DELETE /nodes/{name} (如果实现了 GracefulDeleter)
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{request.MethodWatch, "watch/" + itemPath, nameParams, namer, false}, isWatcher)
		actions = appendIf(actions, action{request.MethodConnect, itemPath, nameParams, namer, false}, isConnecter)
		actions = appendIf(actions, action{request.MethodConnect, itemPath + "/{path:*}", proxyParams, namer, false}, isConnecter && connectSubpath)
		// --- Case 2: 处理命名空间作用域的资源（如 Pods, Deployments） ---
	default:
		namespaceParamName := "namespaces"
		// Handler for standard REST verbs (GET, PUT, POST and DELETE).
		// 定义一个 {namespace} 路径参数。
		namespaceParam := ws.PathParameter("namespace", "object name and auth scope, such as for teams and projects").DataType("string")
		// namespacedPath 是指特定命名空间下的资源集合路径，例如 "namespaces/{namespace}/pods"。
		namespacedPath := namespaceParamName + "/{namespace}/" + resource
		namespaceParams := []*restful.Parameter{namespaceParam}
		// 同样的逻辑，但所有路径都以 "namespaces/{namespace}/" 为前缀。
		resourcePath := namespacedPath
		resourceParams := namespaceParams
		itemPath := namespacedPath + "/{name}"
		nameParams := append(namespaceParams, nameParam)
		proxyParams := append(nameParams, pathParam)
		itemPathSuffix := ""
		if isSubresource {
			// 子资源路径，例如 "namespaces/{namespace}/pods/{name}/log"。
			itemPathSuffix = "/" + subresource
			itemPath = itemPath + itemPathSuffix
			resourcePath = itemPath
			resourceParams = nameParams
		}
		apiResource.Name = path
		apiResource.Namespaced = true // 明确指出这是命名空间级资源。
		apiResource.Kind = resourceKind
		// 创建一个用于生成资源名称的辅助对象，指定为非集群作用域。
		namer := handlers.ContextBasedNaming{
			Namer:         a.group.Namer,
			ClusterScoped: false,
		}
		// --- 再次定义要注册的 actions，但这次是针对命名空间路径的 ---
		// 在特定命名空间下的资源集合路径上（如 /api/v1/namespaces/{namespace}/pods）注册的 actions：
		actions = appendIf(actions, action{request.MethodList, resourcePath, resourceParams, namer, false}, isLister)                        // GET /namespaces/{ns}/pods
		actions = appendIf(actions, action{request.MethodPost, resourcePath, resourceParams, namer, false}, isCreater)                       // POST /namespaces/{ns}/pods
		actions = appendIf(actions, action{request.MethodDeleteCollection, resourcePath, resourceParams, namer, false}, isCollectionDeleter) // DELETE /namespaces/{ns}/pods
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{request.MethodWatchList, "watch/" + resourcePath, resourceParams, namer, false}, allowWatchList)

		// 在特定命名空间下的单个资源路径上（如 /api/v1/namespaces/{namespace}/pods/{name}）注册的 actions：
		actions = appendIf(actions, action{request.MethodGet, itemPath, nameParams, namer, false}, isGetter)
		if getSubpath {
			actions = appendIf(actions, action{request.MethodGet, itemPath + "/{path:*}", proxyParams, namer, false}, isGetter)
		}
		actions = appendIf(actions, action{request.MethodPut, itemPath, nameParams, namer, false}, isUpdater)            // PUT /namespaces/{ns}/pods/{name}
		actions = appendIf(actions, action{request.MethodPatch, itemPath, nameParams, namer, false}, isPatcher)          // PATCH /namespaces/{ns}/pods/{name}
		actions = appendIf(actions, action{request.MethodDelete, itemPath, nameParams, namer, false}, isGracefulDeleter) // DELETE /namespaces/{ns}/pods/{name}
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{request.MethodWatch, "watch/" + itemPath, nameParams, namer, false}, isWatcher)
		actions = appendIf(actions, action{request.MethodConnect, itemPath, nameParams, namer, false}, isConnecter)
		actions = appendIf(actions, action{request.MethodConnect, itemPath + "/{path:*}", proxyParams, namer, false}, isConnecter && connectSubpath)

		// list or post across namespace.
		// For ex: LIST all pods in all namespaces by sending a LIST request at /api/apiVersion/pods.
		// TODO: more strongly type whether a resource allows these actions on "all namespaces" (bulk delete)
		// --- 特殊处理：为跨所有命名空间的操作注册路由 ---
		// 这对应于 `kubectl get pods -A` 或 `kubectl get pods --all-namespaces`。
		// URL 路径不包含 `namespaces/{namespace}` 部分，例如 /api/v1/pods。
		if !isSubresource {
			// 只有主资源才支持跨命名空间操作，子资源不支持。
			// `allNamespaces` 参数被设为 true，处理函数会据此改变其行为。
			actions = appendIf(actions, action{request.MethodList, resource, params, namer, true}, isLister)
			// DEPRECATED in 1.11
			actions = appendIf(actions, action{request.MethodWatchList, "watch/" + resource, params, namer, true}, allowWatchList)
		}
	}
	// --- 处理 StorageVersionAPI 特性 ---
	// 这部分逻辑用于支持 Kubernetes 的 Storage Version API，这是一个较新的特性，
	// 用于查询和管理 API 对象在 etcd 中的存储版本。
	var resourceInfo *storageversion.ResourceInfo
	if utilfeature.DefaultFeatureGate.Enabled(features.StorageVersionAPI) &&
		utilfeature.DefaultFeatureGate.Enabled(features.APIServerIdentity) &&
		isStorageVersionProvider &&
		storageVersionProvider.StorageVersion() != nil {

		versioner := storageVersionProvider.StorageVersion()
		// 获取资源在 etcd 中实际存储的 GVK（GroupVersionKind）。
		encodingGVK, err := getStorageVersionKind(versioner, storage, a.group.Typer)
		if err != nil {
			return nil, nil, err
		}
		decodableVersions := []schema.GroupVersion{}
		if a.group.ConvertabilityChecker != nil {
			decodableVersions = a.group.ConvertabilityChecker.VersionsForGroupKind(fqKindToRegister.GroupKind())
		}
		// 构造一个 ResourceInfo 对象，包含所有关于存储版本的信息。
		resourceInfo = &storageversion.ResourceInfo{
			GroupResource: schema.GroupResource{
				Group:    a.group.GroupVersion.Group,
				Resource: apiResource.Name,
			},
			EncodingVersion: encodingGVK.GroupVersion().String(), // 记录编码版本。
			// We record EquivalentResourceMapper first instead of calculate
			// DecodableVersions immediately because API installation must
			// be completed first for us to know equivalent APIs
			// EquivalentResourceMapper 用于查找等效的资源（例如 `extensions/v1beta1/deployments` 和 `apps/v1/deployments`）。
			EquivalentResourceMapper: a.group.EquivalentResourceRegistry,
			// 可以直接解码的版本。
			DirectlyDecodableVersions: decodableVersions,
			// 此资源被提供的所有 API 版本。
			ServedVersions: a.group.AllServedVersionsByResource[path],
		}
	}

	// Create Routes for the actions.
	// TODO: Add status documentation using Returns()
	// Errors (see api/errors/errors.go as well as go-restful router):
	// http.StatusNotFound, http.StatusMethodNotAllowed,
	// http.StatusUnsupportedMediaType, http.StatusNotAcceptable,
	// http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
	// http.StatusRequestTimeout, http.StatusConflict, http.StatusPreconditionFailed,
	// http.StatusUnprocessableEntity, http.StatusInternalServerError,
	// http.StatusServiceUnavailable
	// and api error codes
	// Note that if we specify a versioned Status object here, we may need to
	// create one for the tests, also
	// Success:
	// http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent
	//
	// test/integration/auth_test.go is currently the most comprehensive status code test
	// 遍历 group.Serializer 支持的所有媒体类型（如 application/json, application/yaml）。
	for _, s := range a.group.Serializer.SupportedMediaTypes() {
		// 确保每个序列化器都定义了完整的媒体类型。
		if len(s.MediaTypeSubType) == 0 || len(s.MediaTypeType) == 0 {
			return nil, nil, fmt.Errorf("all serializers in the group Serializer must have MediaTypeType and MediaTypeSubType set: %s", s.MediaType)
		}
	}
	// 获取用于 HTTP 内容协商的媒体类型列表。
	mediaTypes, streamMediaTypes := negotiation.MediaTypesForSerializer(a.group.Serializer)
	allMediaTypes := append(mediaTypes, streamMediaTypes...)
	// 告诉 go-restful 这个 WebService 可以产生（Produces）哪些媒体类型的内容。
	ws.Produces(allMediaTypes...)

	kubeVerbs := map[string]struct{}{} // 用于收集该资源支持的所有 Kube Verbs（用于服务发现）。
	// reqScope 是一个核心结构体，它封装了处理一个 API 请求所需的所有上下文和辅助工具。
	// 它会被传递给每个 HTTP 处理函数。
	reqScope := handlers.RequestScope{
		Serializer:      a.group.Serializer,      // 序列化/反序列化器
		ParameterCodec:  a.group.ParameterCodec,  // URL 查询参数编解码器
		Creater:         a.group.Creater,         // 对象创建器
		Convertor:       a.group.Convertor,       // 版本转换器
		Defaulter:       a.group.Defaulter,       // 默认值设置器
		Typer:           a.group.Typer,           // 类型信息获取器
		UnsafeConvertor: a.group.UnsafeConvertor, // 不安全的转换器
		Authorizer:      a.group.Authorizer,      // 授权器

		EquivalentResourceMapper: a.group.EquivalentResourceRegistry, // 等效资源映射器

		// TODO: Check for the interface on storage
		TableConvertor: tableProvider, // 表格转换器

		// TODO: This seems wrong for cross-group subresources. It makes an assumption that a subresource and its parent are in the same group version. Revisit this.
		// 记录当前处理的资源、子资源和 Kind。
		Resource:    a.group.GroupVersion.WithResource(resource),
		Subresource: subresource,
		Kind:        fqKindToRegister,

		AcceptsGroupVersionDelegate: gvAcceptor,

		HubGroupVersion: schema.GroupVersion{Group: fqKindToRegister.Group, Version: runtime.APIVersionInternal},

		MetaGroupVersion: metav1.SchemeGroupVersion, // 元数据对象（如 Status, ListOptions）的版本。

		MaxRequestBodyBytes: a.group.MaxRequestBodyBytes, // 最大请求体大小限制。
	}
	if a.group.MetaGroupVersion != nil {
		reqScope.MetaGroupVersion = *a.group.MetaGroupVersion
	}

	// Strategies may ignore changes to some fields by resetting the field values.
	//
	// For instance, spec resource strategies should reset the status, and status subresource
	// strategies should reset the spec.
	//
	// Strategies that reset fields must report to the field manager which fields are
	// reset by implementing either the ResetFieldsStrategy or the ResetFieldsFilterStrategy
	// interface.
	//
	// For subresources that provide write access to only specific nested fields
	// fieldpath.NewPatternFilter can help create a filter to reset all other fields.
	// --- 处理 Server-Side Apply (SSA) 的字段管理逻辑 ---
	var resetFieldsFilter map[fieldpath.APIVersion]fieldpath.Filter
	resetFieldsStrategy, isResetFieldsStrategy := storage.(rest.ResetFieldsStrategy)
	if isResetFieldsStrategy {
		resetFieldsFilter = fieldpath.NewExcludeFilterSetMap(resetFieldsStrategy.GetResetFields())
	}
	if resetFieldsStrategy, isResetFieldsFilterStrategy := storage.(rest.ResetFieldsFilterStrategy); isResetFieldsFilterStrategy {
		if isResetFieldsStrategy {
			return nil, nil, fmt.Errorf("may not implement both ResetFieldsStrategy and ResetFieldsFilterStrategy")
		}
		resetFieldsFilter = resetFieldsStrategy.GetResetFieldsFilter()
	}

	reqScope.FieldManager, err = managedfields.NewDefaultFieldManager(
		a.group.TypeConverter,
		a.group.UnsafeConvertor,
		a.group.Defaulter,
		a.group.Creater,
		fqKindToRegister,
		reqScope.HubGroupVersion,
		subresource,
		resetFieldsFilter,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create field manager: %v", err)
	}
	// =========================================================================
	// === 核心循环：遍历之前收集的所有 action，为每个 action 创建并注册路由 ===
	// =========================================================================
	for _, action := range actions {
		producedObject := storageMeta.ProducesObject(action.Verb)
		if producedObject == nil {
			producedObject = defaultVersionedObject
		}
		reqScope.Namer = action.Namer
		// --- 根据 action 的属性，确定用于监控指标的标签 ---
		requestScope := "cluster"
		var namespaced string
		var operationSuffix string
		if apiResource.Namespaced { // 命名空间级
			requestScope = "namespace"
			namespaced = "Namespaced"
		}
		if strings.HasSuffix(action.Path, "/{path:*}") { // 带子路径
			requestScope = "resource"
			operationSuffix = operationSuffix + "WithPath"
		}
		if strings.Contains(action.Path, "/{name}") || action.Verb == request.MethodPost {
			requestScope = "resource"
		}
		if action.AllNamespaces {
			requestScope = "cluster"
			operationSuffix = operationSuffix + "ForAllNamespaces"
			namespaced = ""
		}
		// 将 action 的动词转换为服务发现使用的 Kube Verb（如 "get", "list", "create"）。
		if kubeVerb, found := toDiscoveryKubeVerb[action.Verb]; found {
			if len(kubeVerb) != 0 {
				kubeVerbs[kubeVerb] = struct{}{}
			}
		} else {
			return nil, nil, fmt.Errorf("unknown action verb for discovery: %s", action.Verb)
		}

		routes := []*restful.RouteBuilder{}

		// If there is a subresource, kind should be the parent's kind.
		if isSubresource {
			parentStorage, ok := a.group.Storage[resource]
			if !ok {
				return nil, nil, fmt.Errorf("missing parent storage: %q", resource)
			}

			fqParentKind, err := GetResourceKind(a.group.GroupVersion, parentStorage, a.group.Typer)
			if err != nil {
				return nil, nil, err
			}
			kind = fqParentKind.Kind
		}

		verbOverrider, needOverride := storage.(StorageMetricsOverride)

		// accumulate endpoint-level warnings
		var (
			warnings       []string
			deprecated     bool
			removedRelease string
		)

		{
			versionedPtrWithGVK := versionedPtr.DeepCopyObject()
			versionedPtrWithGVK.GetObjectKind().SetGroupVersionKind(fqKindToRegister)
			currentMajor, currentMinor, _ := deprecation.MajorMinor(versioninfo.Get())
			deprecated = deprecation.IsDeprecated(versionedPtrWithGVK, currentMajor, currentMinor)
			if deprecated {
				removedRelease = deprecation.RemovedRelease(versionedPtrWithGVK)
				warnings = append(warnings, deprecation.WarningMessage(versionedPtrWithGVK))
			}
		}
		// ============================================================
		// === 巨大的 switch 语句，为每种 action.Verb 创建路由 ===
		// ============================================================
		switch action.Verb {
		case request.MethodGet: // Get a resource.  // 处理 GET 请求 (获取单个资源)
			var handler restful.RouteFunction
			if isGetterWithOptions {
				// 如果 storage 支持带选项的 Get，使用 restfulGetResourceWithOptions 工厂函数。
				handler = restfulGetResourceWithOptions(getterWithOptions, reqScope, isSubresource)
			} else {
				handler = restfulGetResource(getter, reqScope)
			}

			if needOverride {
				// need change the reported verb
				handler = metrics.InstrumentRouteFunc(verbOverrider.OverrideMetricsVerb(action.Verb), group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			} else {
				handler = metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			}
			handler = utilwarning.AddWarningsHandler(handler, warnings)

			doc := "read the specified " + kind

			if isSubresource {
				doc = "read " + subresource + " of the specified " + kind
			}
			// --- 使用链式调用构建一个完整的 go-restful Route ---
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("read"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				Writes(producedObject)
			if isGetterWithOptions {
				if err := AddObjectParams(ws, route, versionedGetOptions); err != nil {
					return nil, nil, err
				}
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodList: // List all resources of a kind.
			doc := "list objects of kind " + kind
			if isSubresource {
				doc = "list " + subresource + " of objects of kind " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, false, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("list"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), allMediaTypes...)...).
				Returns(http.StatusOK, "OK", versionedList).
				Writes(versionedList)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			switch {
			case isLister && isWatcher:
				doc := "list or watch objects of kind " + kind
				if isSubresource {
					doc = "list or watch " + subresource + " of objects of kind " + kind
				}
				route.Doc(doc)
			case isWatcher:
				doc := "watch objects of kind " + kind
				if isSubresource {
					doc = "watch " + subresource + "of objects of kind " + kind
				}
				route.Doc(doc)
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodPut: // Update a resource.
			doc := "replace the specified " + kind
			if isSubresource {
				doc = "replace " + subresource + " of the specified " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulUpdateResource(updater, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.PUT(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("replace"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// TODO: in some cases, the API may return a v1.Status instead of the versioned object
				// but currently go-restful can't handle multiple different objects being returned.
				Returns(http.StatusCreated, "Created", producedObject).
				Reads(defaultVersionedObject).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedUpdateOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodPatch: // Partially update a resource
			doc := "partially update the specified " + kind
			if isSubresource {
				doc = "partially update " + subresource + " of the specified " + kind
			}
			supportedTypes := []string{
				string(types.JSONPatchType),
				string(types.MergePatchType),
				string(types.StrategicMergePatchType),
				string(types.ApplyYAMLPatchType),
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
				supportedTypes = append(supportedTypes, string(types.ApplyCBORPatchType))
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulPatchResource(patcher, reqScope, admit, supportedTypes))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.PATCH(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Consumes(supportedTypes...).
				Operation("patch"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// Patch can return 201 when a server side apply is requested
				Returns(http.StatusCreated, "Created", producedObject).
				Reads(metav1.Patch{}).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedPatchOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodPost: // Create a resource.
			var handler restful.RouteFunction
			if isNamedCreater {
				handler = restfulCreateNamedResource(namedCreater, reqScope, admit)
			} else {
				handler = restfulCreateResource(creater, reqScope, admit)
			}
			handler = metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			article := GetArticleForNoun(kind, " ")
			doc := "create" + article + kind
			if isSubresource {
				doc = "create " + subresource + " of" + article + kind
			}
			route := ws.POST(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("create"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// TODO: in some cases, the API may return a v1.Status instead of the versioned object
				// but currently go-restful can't handle multiple different objects being returned.
				Returns(http.StatusCreated, "Created", producedObject).
				Returns(http.StatusAccepted, "Accepted", producedObject).
				Reads(defaultVersionedObject).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedCreateOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodDelete: // Delete a resource.
			article := GetArticleForNoun(kind, " ")
			doc := "delete" + article + kind
			if isSubresource {
				doc = "delete " + subresource + " of" + article + kind
			}
			deleteReturnType := versionedStatus
			if deleteReturnsDeletedObject {
				deleteReturnType = producedObject
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulDeleteResource(gracefulDeleter, isGracefulDeleter, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.DELETE(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("delete"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Writes(deleteReturnType).
				Returns(http.StatusOK, "OK", deleteReturnType).
				Returns(http.StatusAccepted, "Accepted", deleteReturnType)
			if isGracefulDeleter {
				route.Reads(versionedDeleterObject)
				route.ParameterNamed("body").Required(false)
				if err := AddObjectParams(ws, route, versionedDeleteOptions); err != nil {
					return nil, nil, err
				}
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodDeleteCollection:
			doc := "delete collection of " + kind
			if isSubresource {
				doc = "delete collection of " + subresource + " of a " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulDeleteCollection(collectionDeleter, isCollectionDeleter, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.DELETE(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("deletecollection"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Writes(versionedStatus).
				Returns(http.StatusOK, "OK", versionedStatus)
			if isCollectionDeleter {
				route.Reads(versionedDeleterObject)
				route.ParameterNamed("body").Required(false)
				if err := AddObjectParams(ws, route, versionedDeleteOptions); err != nil {
					return nil, nil, err
				}
			}
			if err := AddObjectParams(ws, route, versionedListOptions, "watch", "allowWatchBookmarks"); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		// deprecated in 1.11
		case request.MethodWatch: // Watch a resource.
			doc := "watch changes to an object of kind " + kind
			if isSubresource {
				doc = "watch changes to " + subresource + " of an object of kind " + kind
			}
			doc += ". deprecated: use the 'watch' parameter with a list operation instead, filtered to a single item with the 'fieldSelector' parameter."
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, true, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("watch"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(allMediaTypes...).
				Returns(http.StatusOK, "OK", versionedWatchEvent).
				Writes(versionedWatchEvent)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		// deprecated in 1.11
		case request.MethodWatchList: // Watch all resources of a kind.
			doc := "watch individual changes to a list of " + kind
			if isSubresource {
				doc = "watch individual changes to a list of " + subresource + " of " + kind
			}
			doc += ". deprecated: use the 'watch' parameter with a list operation instead."
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, true, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("watch"+namespaced+kind+strings.Title(subresource)+"List"+operationSuffix).
				Produces(allMediaTypes...).
				Returns(http.StatusOK, "OK", versionedWatchEvent).
				Writes(versionedWatchEvent)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case request.MethodConnect:
			for _, method := range connecter.ConnectMethods() {
				connectProducedObject := storageMeta.ProducesObject(method)
				if connectProducedObject == nil {
					connectProducedObject = "string"
				}
				doc := "connect " + method + " requests to " + kind
				if isSubresource {
					doc = "connect " + method + " requests to " + subresource + " of " + kind
				}
				handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulConnectResource(connecter, reqScope, admit, path, isSubresource))
				handler = utilwarning.AddWarningsHandler(handler, warnings)
				route := ws.Method(method).Path(action.Path).
					To(handler).
					Doc(doc).
					Operation("connect" + strings.Title(strings.ToLower(method)) + namespaced + kind + strings.Title(subresource) + operationSuffix).
					Produces("*/*").
					Consumes("*/*").
					Writes(connectProducedObject)
				if versionedConnectOptions != nil {
					if err := AddObjectParams(ws, route, versionedConnectOptions); err != nil {
						return nil, nil, err
					}
				}
				addParams(route, action.Params)
				routes = append(routes, route)

				// transform ConnectMethods to kube verbs
				if kubeVerb, found := toDiscoveryKubeVerb[method]; found {
					if len(kubeVerb) != 0 {
						kubeVerbs[kubeVerb] = struct{}{}
					}
				}
			}
		default:
			return nil, nil, fmt.Errorf("unrecognized action verb: %s", action.Verb)
		}
		for _, route := range routes {
			route.Metadata(RouteMetaGVK, metav1.GroupVersionKind{
				Group:   reqScope.Kind.Group,
				Version: reqScope.Kind.Version,
				Kind:    reqScope.Kind.Kind,
			})
			route.Metadata(RouteMetaAction, strings.ToLower(action.Verb))
			ws.Route(route)
		}
		// Note: update GetAuthorizerAttributes() when adding a custom handler.
	}

	apiResource.Verbs = make([]string, 0, len(kubeVerbs))
	for kubeVerb := range kubeVerbs {
		apiResource.Verbs = append(apiResource.Verbs, kubeVerb)
	}
	sort.Strings(apiResource.Verbs)

	if shortNamesProvider, ok := storage.(rest.ShortNamesProvider); ok {
		apiResource.ShortNames = shortNamesProvider.ShortNames()
	}
	if categoriesProvider, ok := storage.(rest.CategoriesProvider); ok {
		apiResource.Categories = categoriesProvider.Categories()
	}
	if !isSubresource {
		singularNameProvider, ok := storage.(rest.SingularNameProvider)
		if !ok {
			return nil, nil, fmt.Errorf("resource %s must implement SingularNameProvider", resource)
		}
		apiResource.SingularName = singularNameProvider.GetSingularName()
	}

	if gvkProvider, ok := storage.(rest.GroupVersionKindProvider); ok {
		gvk := gvkProvider.GroupVersionKind(a.group.GroupVersion)
		apiResource.Group = gvk.Group
		apiResource.Version = gvk.Version
		apiResource.Kind = gvk.Kind
	}

	// Record the existence of the GVR and the corresponding GVK
	a.group.EquivalentResourceRegistry.RegisterKindFor(reqScope.Resource, reqScope.Subresource, fqKindToRegister)

	return &apiResource, resourceInfo, nil
}

// indirectArbitraryPointer returns *ptrToObject for an arbitrary pointer
func indirectArbitraryPointer(ptrToObject interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(ptrToObject)).Interface()
}

func appendIf(actions []action, a action, shouldAppend bool) []action {
	if shouldAppend {
		actions = append(actions, a)
	}
	return actions
}

func addParams(route *restful.RouteBuilder, params []*restful.Parameter) {
	for _, param := range params {
		route.Param(param)
	}
}

// AddObjectParams converts a runtime.Object into a set of go-restful Param() definitions on the route.
// The object must be a pointer to a struct; only fields at the top level of the struct that are not
// themselves interfaces or structs are used; only fields with a json tag that is non empty (the standard
// Go JSON behavior for omitting a field) become query parameters. The name of the query parameter is
// the JSON field name. If a description struct tag is set on the field, that description is used on the
// query parameter. In essence, it converts a standard JSON top level object into a query param schema.
func AddObjectParams(ws *restful.WebService, route *restful.RouteBuilder, obj interface{}, excludedNames ...string) error {
	sv, err := conversion.EnforcePtr(obj)
	if err != nil {
		return err
	}
	st := sv.Type()
	excludedNameSet := sets.NewString(excludedNames...)
	switch st.Kind() {
	case reflect.Struct:
		for i := 0; i < st.NumField(); i++ {
			name := st.Field(i).Name
			sf, ok := st.FieldByName(name)
			if !ok {
				continue
			}
			switch sf.Type.Kind() {
			case reflect.Interface, reflect.Struct:
			case reflect.Pointer:
				// TODO: This is a hack to let metav1.Time through. This needs to be fixed in a more generic way eventually. bug #36191
				if (sf.Type.Elem().Kind() == reflect.Interface || sf.Type.Elem().Kind() == reflect.Struct) && strings.TrimPrefix(sf.Type.String(), "*") != "metav1.Time" {
					continue
				}
				fallthrough
			default:
				jsonTag := sf.Tag.Get("json")
				if len(jsonTag) == 0 {
					continue
				}
				jsonName := strings.SplitN(jsonTag, ",", 2)[0]
				if len(jsonName) == 0 {
					continue
				}
				if excludedNameSet.Has(jsonName) {
					continue
				}
				var desc string
				if docable, ok := obj.(documentable); ok {
					desc = docable.SwaggerDoc()[jsonName]
				}
				route.Param(ws.QueryParameter(jsonName, desc).DataType(typeToJSON(sf.Type.String())))
			}
		}
	}
	return nil
}

// TODO: this is incomplete, expand as needed.
// Convert the name of a golang type to the name of a JSON type
func typeToJSON(typeName string) string {
	switch typeName {
	case "bool", "*bool":
		return "boolean"
	case "uint8", "*uint8", "int", "*int", "int32", "*int32", "int64", "*int64", "uint32", "*uint32", "uint64", "*uint64":
		return "integer"
	case "float64", "*float64", "float32", "*float32":
		return "number"
	case "metav1.Time", "*metav1.Time":
		return "string"
	case "byte", "*byte":
		return "string"
	case "v1.DeletionPropagation", "*v1.DeletionPropagation":
		return "string"
	case "v1.ResourceVersionMatch", "*v1.ResourceVersionMatch":
		return "string"
	case "v1.IncludeObjectPolicy", "*v1.IncludeObjectPolicy":
		return "string"
	case "*string":
		return "string"

	// TODO: Fix these when go-restful supports a way to specify an array query param:
	// https://github.com/emicklei/go-restful/issues/225
	case "[]string", "[]*string":
		return "string"
	case "[]int32", "[]*int32":
		return "integer"

	default:
		return typeName
	}
}

// defaultStorageMetadata provides default answers to rest.StorageMetadata.
type defaultStorageMetadata struct{}

// defaultStorageMetadata implements rest.StorageMetadata
var _ rest.StorageMetadata = defaultStorageMetadata{}

func (defaultStorageMetadata) ProducesMIMETypes(verb string) []string {
	return nil
}

func (defaultStorageMetadata) ProducesObject(verb string) interface{} {
	return nil
}

// splitSubresource checks if the given storage path is the path of a subresource and returns
// the resource and subresource components.
func splitSubresource(path string) (string, string, error) {
	var resource, subresource string
	switch parts := strings.Split(path, "/"); len(parts) {
	case 2:
		resource, subresource = parts[0], parts[1]
	case 1:
		resource = parts[0]
	default:
		// TODO: support deeper paths
		return "", "", fmt.Errorf("api_installer allows only one or two segment paths (resource or resource/subresource)")
	}
	return resource, subresource, nil
}

// GetArticleForNoun returns the article needed for the given noun.
func GetArticleForNoun(noun string, padding string) string {
	if !strings.HasSuffix(noun, "ss") && strings.HasSuffix(noun, "s") {
		// Plurals don't have an article.
		// Don't catch words like class
		return fmt.Sprintf("%v", padding)
	}

	article := "a"
	if isVowel(rune(noun[0])) {
		article = "an"
	}

	return fmt.Sprintf("%s%s%s", padding, article, padding)
}

// isVowel returns true if the rune is a vowel (case insensitive).
func isVowel(c rune) bool {
	vowels := []rune{'a', 'e', 'i', 'o', 'u'}
	for _, value := range vowels {
		if value == unicode.ToLower(c) {
			return true
		}
	}
	return false
}

func restfulListResource(r rest.Lister, rw rest.Watcher, scope handlers.RequestScope, forceWatch bool, minRequestTimeout time.Duration) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.ListResource(r, rw, &scope, forceWatch, minRequestTimeout)(res.ResponseWriter, req.Request)
	}
}

func restfulCreateNamedResource(r rest.NamedCreater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.CreateNamedResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulCreateResource(r rest.Creater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.CreateResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulDeleteResource(r rest.GracefulDeleter, allowsOptions bool, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.DeleteResource(r, allowsOptions, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulDeleteCollection(r rest.CollectionDeleter, checkBody bool, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.DeleteCollection(r, checkBody, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulUpdateResource(r rest.Updater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.UpdateResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulPatchResource(r rest.Patcher, scope handlers.RequestScope, admit admission.Interface, supportedTypes []string) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.PatchResource(r, &scope, admit, supportedTypes)(res.ResponseWriter, req.Request)
	}
}

func restfulGetResource(r rest.Getter, scope handlers.RequestScope) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.GetResource(r, &scope)(res.ResponseWriter, req.Request)
	}
}

func restfulGetResourceWithOptions(r rest.GetterWithOptions, scope handlers.RequestScope, isSubresource bool) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.GetResourceWithOptions(r, &scope, isSubresource)(res.ResponseWriter, req.Request)
	}
}

func restfulConnectResource(connecter rest.Connecter, scope handlers.RequestScope, admit admission.Interface, restPath string, isSubresource bool) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.ConnectResource(connecter, &scope, admit, restPath, isSubresource)(res.ResponseWriter, req.Request)
	}
}
