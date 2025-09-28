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
	"io"
	"net"
	"net/url"

	"github.com/spf13/pflag"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"

	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver"
	generatedopenapi "k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured/unstructuredscheme"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/features"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	storagevalue "k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/apiserver/pkg/util/openapi"
	"k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/apiserver/pkg/util/webhook"
	scheme "k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/listers/core/v1"
	netutils "k8s.io/utils/net"
)

const defaultEtcdPathPrefix = "/registry/apiextensions.kubernetes.io"

// CustomResourceDefinitionsServerOptions describes the runtime options of an apiextensions-apiserver.
type CustomResourceDefinitionsServerOptions struct {
	ServerRunOptions   *genericoptions.ServerRunOptions
	RecommendedOptions *genericoptions.RecommendedOptions
	APIEnablement      *genericoptions.APIEnablementOptions

	StdOut io.Writer
	StdErr io.Writer
}

// NewCustomResourceDefinitionsServerOptions creates default options of an apiextensions-apiserver.
func NewCustomResourceDefinitionsServerOptions(out, errOut io.Writer) *CustomResourceDefinitionsServerOptions {
	o := &CustomResourceDefinitionsServerOptions{
		ServerRunOptions: genericoptions.NewServerRunOptions(),
		RecommendedOptions: genericoptions.NewRecommendedOptions(
			defaultEtcdPathPrefix,
			apiserver.Codecs.LegacyCodec(v1beta1.SchemeGroupVersion, v1.SchemeGroupVersion),
		),
		APIEnablement: genericoptions.NewAPIEnablementOptions(),

		StdOut: out,
		StdErr: errOut,
	}

	return o
}

// AddFlags adds the apiextensions-apiserver flags to the flagset.
func (o CustomResourceDefinitionsServerOptions) AddFlags(fs *pflag.FlagSet) {
	o.ServerRunOptions.AddUniversalFlags(fs)
	o.RecommendedOptions.AddFlags(fs)
	o.APIEnablement.AddFlags(fs)
}

// Validate validates the apiextensions-apiserver options.
func (o CustomResourceDefinitionsServerOptions) Validate() error {
	errors := []error{}
	errors = append(errors, o.ServerRunOptions.Validate()...)
	errors = append(errors, o.RecommendedOptions.Validate()...)
	errors = append(errors, o.APIEnablement.Validate(apiserver.Scheme)...)
	return utilerrors.NewAggregate(errors)
}

// Complete fills in missing options.
func (o *CustomResourceDefinitionsServerOptions) Complete() error {
	return o.ServerRunOptions.Complete()
}

// Config returns an apiextensions-apiserver configuration.
func (o CustomResourceDefinitionsServerOptions) Config() (*apiserver.Config, error) {
	// TODO have a "real" external address
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	serverConfig := genericapiserver.NewRecommendedConfig(apiserver.Codecs)
	if err := o.ServerRunOptions.ApplyTo(&serverConfig.Config); err != nil {
		return nil, err
	}
	if err := o.RecommendedOptions.ApplyTo(serverConfig); err != nil {
		return nil, err
	}
	if err := o.APIEnablement.ApplyTo(&serverConfig.Config, apiserver.DefaultAPIResourceConfigSource(), apiserver.Scheme); err != nil {
		return nil, err
	}

	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(openapi.GetOpenAPIDefinitionsWithoutDisabledFeatures(generatedopenapi.GetOpenAPIDefinitions), openapinamer.NewDefinitionNamer(apiserver.Scheme, scheme.Scheme))
	config := &apiserver.Config{
		GenericConfig: serverConfig,
		ExtraConfig: apiserver.ExtraConfig{
			CRDRESTOptionsGetter: NewCRDRESTOptionsGetter(*o.RecommendedOptions.Etcd, serverConfig.ResourceTransformers, serverConfig.StorageObjectCountTracker),
			ServiceResolver:      &serviceResolver{serverConfig.SharedInformerFactory.Core().V1().Services().Lister()},
			AuthResolverWrapper:  webhook.NewDefaultAuthenticationInfoResolverWrapper(nil, nil, serverConfig.LoopbackClientConfig, noopoteltrace.NewTracerProvider()),
		},
	}
	return config, nil
}

// NewCRDRESTOptionsGetter create a RESTOptionsGetter for CustomResources.
//
// Avoid messing with anything outside of changes to StorageConfig as that
// may lead to unexpected behavior when the options are applied.
// NewCRDRESTOptionsGetter 函数创建并返回一个 genericregistry.RESTOptionsGetter。
// RESTOptionsGetter 是一个非常重要的接口，它的核心作用是为某个特定的 API 资源（在这里是所有的 Custom Resource）
// 提供一套与后端存储（Etcd）交互的完整配置，这套配置被称为 genericregistry.RESTOptions。
// RESTOptions 包含了诸如编解码器(Codec)、存储装饰器(Decorator)等关键信息。
//
// 简而言之，这个函数就是为所有 "kubectl get my-crd-instance" 这样的操作，定制其在 Etcd 层面上的读写行为。
func NewCRDRESTOptionsGetter(etcdOptions genericoptions.EtcdOptions, resourceTransformers storagevalue.ResourceTransformers, tracker flowcontrolrequest.StorageObjectCountTracker) genericregistry.RESTOptionsGetter {
	// 1. 准备 CBOR 序列化器。
	// CBOR (Concise Binary Object Representation) 是一种二进制序列化格式，类似于 JSON，但更紧凑、解析更快。
	// 这里创建了一个专门用于序列化和反序列化 unstructured.Unstructured 对象的 CBOR 序列化器。
	// unstructured.Unstructured 是 Kubernetes 中用来表示所有 CR (Custom Resource) 的通用数据结构，本质上是一个 map[string]interface{}。
	ucbor := cbor.NewSerializer(unstructuredscheme.NewUnstructuredCreator(), unstructuredscheme.NewUnstructuredObjectTyper())
	// 2. 根据特性门控 (Feature Gate) 决定默认的编码器。
	// encoder 将被用于将内存中的对象序列化成二进制数据，以便存入 Etcd。
	encoder := unstructured.UnstructuredJSONScheme
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		encoder = ucbor
	}
	// 3. 定制化为 CRD 专用的 Etcd 存储配置。
	// 创建 etcdOptions 的一个副本，以避免修改原始配置。
	etcdOptionsCopy := etcdOptions
	// 关键步骤：为 CRD 存储配置一个特殊的编解码器 (Codec)。
	etcdOptionsCopy.StorageConfig.Codec = runtime.NewCodec(
		encoder, // 编码器：使用我们上面根据特性门控选择的 encoder (JSON 或 CBOR)。
		// Whether the feature gate is enabled or disabled, the decoder must be able to
		// recognize any resources stored using the CBOR encoder.
		// 解码器：这里使用了一个 "recognizer" (识别器)。
		// 这是一个非常重要的健壮性设计！
		// 无论 CBORServingAndStorage 特性门控是开启还是关闭，解码器都必须有能力同时识别和解析 CBOR 和 JSON 两种格式。
		// 这是因为 Etcd 中可能同时存在新旧两种格式的数据（比如，在特性门控状态变更期间）。
		// recognizer 会自动检测输入数据的格式，然后选择正确的解码器 (ucbor 或 UnstructuredJSONScheme) 来进行解析。
		recognizer.NewDecoder(
			ucbor,
			unstructured.UnstructuredJSONScheme,
		),
	)
	// 设置存储对象数量追踪器。
	etcdOptionsCopy.StorageConfig.StorageObjectCountTracker = tracker
	// 清空 WatchCacheSizes 配置。
	// 对于 CRD，Kubernetes 不提供单独控制其 Watch 缓存大小的选项，所以这里将其设置为 nil，
	// 以确保 CRD 使用的是全局默认的 Watch 缓存配置。

	etcdOptionsCopy.WatchCacheSizes = nil // this control is not provided for custom resources
	// 4. 创建并返回最终的 RESTOptionsGetter。
	// etcdOptions.CreateRESTOptionsGetter 是一个工厂方法，它接收一个存储工厂 (StorageFactory)
	// 和资源转换器 (resourceTransformers)，然后生成一个闭包函数，这个闭包就是 RESTOptionsGetter。
	// 当 apiserver 需要为某个 CRD 资源构建存储后端时，就会调用这个返回的闭包函数，
	// 闭包函数内部会使用我们在这里精心定制的 `etcdOptionsCopy.StorageConfig` 来创建与 Etcd 交互所需的所有选项。
	return etcdOptions.CreateRESTOptionsGetter(&genericoptions.SimpleStorageFactory{StorageConfig: etcdOptionsCopy.StorageConfig}, resourceTransformers)
}

type serviceResolver struct {
	services corev1.ServiceLister
}

func (r *serviceResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	return proxy.ResolveCluster(r.services, namespace, name, port)
}
