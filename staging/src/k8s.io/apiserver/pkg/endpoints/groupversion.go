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

package endpoints

import (
	"path"
	"time"

	restful "github.com/emicklei/go-restful/v3"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storageversion"
)

// ConvertabilityChecker indicates what versions a GroupKind is available in.
type ConvertabilityChecker interface {
	// VersionsForGroupKind indicates what versions are available to convert a group kind. This determines
	// what our decoding abilities are.
	VersionsForGroupKind(gk schema.GroupKind) []schema.GroupVersion
}

// APIGroupVersion is a helper for exposing rest.Storage objects as http.Handlers via go-restful
// It handles URLs of the form:
// /${storage_key}[/${object_name}]
// Where 'storage_key' points to a rest.Storage object stored in storage.
// This object should contain all parameterization necessary for running a particular API version
type APIGroupVersion struct {
	Storage map[string]rest.Storage

	Root string

	// GroupVersion is the external group version
	GroupVersion schema.GroupVersion

	// AllServedVersionsByResource is indexed by resource and maps to a list of versions that resource exists in.
	// This was created so that StorageVersion for APIs can include a list of all version that are served for each
	// GroupResource tuple.
	AllServedVersionsByResource map[string][]string

	// OptionsExternalVersion controls the Kubernetes APIVersion used for common objects in the apiserver
	// schema like api.Status, api.DeleteOptions, and metav1.ListOptions. Other implementors may
	// define a version "v1beta1" but want to use the Kubernetes "v1" internal objects. If
	// empty, defaults to GroupVersion.
	OptionsExternalVersion *schema.GroupVersion
	// MetaGroupVersion defaults to "meta.k8s.io/v1" and is the scheme group version used to decode
	// common API implementations like ListOptions. Future changes will allow this to vary by group
	// version (for when the inevitable meta/v2 group emerges).
	MetaGroupVersion *schema.GroupVersion

	// Serializer is used to determine how to convert responses from API methods into bytes to send over
	// the wire.
	Serializer     runtime.NegotiatedSerializer
	ParameterCodec runtime.ParameterCodec

	Typer                 runtime.ObjectTyper
	Creater               runtime.ObjectCreater
	Convertor             runtime.ObjectConvertor
	ConvertabilityChecker ConvertabilityChecker
	Defaulter             runtime.ObjectDefaulter
	Namer                 runtime.Namer
	UnsafeConvertor       runtime.ObjectConvertor
	TypeConverter         managedfields.TypeConverter

	EquivalentResourceRegistry runtime.EquivalentResourceRegistry

	// Authorizer determines whether a user is allowed to make a certain request. The Handler does a preliminary
	// authorization check using the request URI but it may be necessary to make additional checks, such as in
	// the create-on-update case
	Authorizer authorizer.Authorizer

	Admit admission.Interface

	MinRequestTimeout time.Duration

	// The limit on the request body size that would be accepted and decoded in a write request.
	// 0 means no limit.
	MaxRequestBodyBytes int64
}

// InstallREST registers the REST handlers (storage, watch, proxy and redirect) into a restful Container.
// It is expected that the provided path root prefix will serve all operations. Root MUST NOT end
// in a slash.
// InstallREST 负责将 REST 处理器（存储、watch、代理、重定向等）注册到一个 restful.Container 中。
// 它期望提供的路径根前缀将为所有操作提供服务。Root 不能以斜杠结尾。
func (g *APIGroupVersion) InstallREST(container *restful.Container) ([]apidiscoveryv2.APIResourceDiscovery, []*storageversion.ResourceInfo, error) {
	// --- 1. 创建并配置 APIInstaller ---

	// `path.Join` 用于安全地构建这个 API 版本的基础 URL 路径。
	// 例如: g.Root="/apis", g.GroupVersion.Group="apps", g.GroupVersion.Version="v1"
	// 结果 `prefix` 就是 "/apis/apps/v1"。
	prefix := path.Join(g.Root, g.GroupVersion.Group, g.GroupVersion.Version)
	// 实例化一个 APIInstaller。这是实际执行安装工作的“工人”。
	// 将 APIGroupVersion 自身 (`g`)、计算出的 URL 前缀 (`prefix`) 和其他配置传递给它。
	installer := &APIInstaller{
		group:             g,
		prefix:            prefix,
		minRequestTimeout: g.MinRequestTimeout,
	}
	// --- 2. 执行安装并收集结果 ---

	// 调用 installer.Install() 来执行所有具体的路由安装工作。
	// 这个调用会返回：
	// - `apiResources`: 用于 v1 服务发现的资源列表。
	// - `resourceInfos`: 用于 StorageVersion API 的资源信息列表。
	// - `ws`: 一个包含了所有已安装路由的 `restful.WebService` 对象。
	// - `registrationErrors`: 安装过程中可能出现的错误列表。
	apiResources, resourceInfos, ws, registrationErrors := installer.Install()
	// --- 3. 安装版本级别的服务发现端点 ---

	// 创建一个 `APIVersionHandler`，它负责处理对该版本自身的服务发现请求。
	// 即，当客户端访问 `/apis/apps/v1` 时，这个处理器会被调用。
	// `staticLister{apiResources}` 提供了一个静态列表，告诉处理器这个版本下有哪些资源。
	versionDiscoveryHandler := discovery.NewAPIVersionHandler(g.Serializer, g.GroupVersion, staticLister{apiResources})
	// `AddToWebService` 会将版本发现路由（通常是 `GET /`，相对于 WebService 的根路径）添加到 `ws` 中。
	versionDiscoveryHandler.AddToWebService(ws)
	// --- 4. 将配置好的 WebService 添加到主容器 ---
	// `container.Add(ws)` 是将这个包含了所有路由（资源路由 + 版本发现路由）的 WebService
	// 正式添加到 APIServer 的主 `restful.Container` 中的关键一步。
	// 至此，所有路由都已准备好，可以对外提供服务了。
	container.Add(ws)

	// --- 5. 准备并返回结果 ---

	// 将 v1 版本的服务发现资源列表转换为 v2beta1 版本。
	// 这是为了支持新的、更丰富的 `discovery.k8s.io` API。
	aggregatedDiscoveryResources, err := ConvertGroupVersionIntoToDiscovery(apiResources)

	if err != nil {
		registrationErrors = append(registrationErrors, err)
	}
	// `removeNonPersistedResources` 会从 `resourceInfos` 列表中移除那些非持久化的资源
	// (例如 "pods/proxy")，因为 StorageVersion API 只关心持久化到 etcd 的资源。
	// `utilerrors.NewAggregate` 将所有收集到的错误合并成一个单一的 error 对象。
	return aggregatedDiscoveryResources, removeNonPersistedResources(resourceInfos), utilerrors.NewAggregate(registrationErrors)
}

func removeNonPersistedResources(infos []*storageversion.ResourceInfo) []*storageversion.ResourceInfo {
	var filtered []*storageversion.ResourceInfo
	for _, info := range infos {
		// if EncodingVersion is empty, then the apiserver does not
		// need to register this resource via the storage version API,
		// thus we can remove it.
		if info != nil && len(info.EncodingVersion) > 0 {
			filtered = append(filtered, info)
		}
	}
	return filtered
}

// staticLister implements the APIResourceLister interface
type staticLister struct {
	list []metav1.APIResource
}

func (s staticLister) ListAPIResources() []metav1.APIResource {
	return s.list
}

var _ discovery.APIResourceLister = &staticLister{}
