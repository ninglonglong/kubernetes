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

package authenticatorfactory

import (
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/group"
	"k8s.io/apiserver/pkg/authentication/request/anonymous"
	"k8s.io/apiserver/pkg/authentication/request/bearertoken"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	unionauth "k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authentication/request/websocket"
	"k8s.io/apiserver/pkg/authentication/request/x509"
	"k8s.io/apiserver/pkg/authentication/token/cache"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	webhooktoken "k8s.io/apiserver/plugin/pkg/authenticator/token/webhook"
	authenticationclient "k8s.io/client-go/kubernetes/typed/authentication/v1"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// DelegatingAuthenticatorConfig is the minimal configuration needed to create an authenticator
// built to delegate authentication to a kube API server
type DelegatingAuthenticatorConfig struct {
	Anonymous *apiserver.AnonymousAuthConfig

	// TokenAccessReviewClient is a client to do token review. It can be nil. Then every token is ignored.
	TokenAccessReviewClient authenticationclient.AuthenticationV1Interface

	// TokenAccessReviewTimeout specifies a time limit for requests made by the authorization webhook client.
	TokenAccessReviewTimeout time.Duration

	// WebhookRetryBackoff specifies the backoff parameters for the authentication webhook retry logic.
	// This allows us to configure the sleep time at each iteration and the maximum number of retries allowed
	// before we fail the webhook call in order to limit the fan out that ensues when the system is degraded.
	WebhookRetryBackoff *wait.Backoff

	// CacheTTL is the length of time that a token authentication answer will be cached.
	CacheTTL time.Duration

	// CAContentProvider are the options for verifying incoming connections using mTLS and directly assigning to users.
	// Generally this is the CA bundle file used to authenticate client certificates
	// If this is nil, then mTLS will not be used.
	ClientCertificateCAContentProvider dynamiccertificates.CAContentProvider

	APIAudiences authenticator.Audiences

	RequestHeaderConfig *RequestHeaderConfig
}

// New 是一个工厂方法，它根据 DelegatingAuthenticatorConfig 的配置，
// 创建并组装出一个完整的、多层次的认证器。
//
// 返回值:
//  1. authenticator.Request: 最终组合好的、可用于处理 HTTP 请求的认证器接口。
//  2. *spec.SecurityDefinitions: 用于生成 OpenAPI/Swagger API 文档的安全定义部分。
//  3. error: 如果配置有误，则返回一个错误。
func (c DelegatingAuthenticatorConfig) New() (authenticator.Request, *spec.SecurityDefinitions, error) {
	// 初始化一个空的认证器切片。我们将按照优先级顺序，把各种认证器添加到这个列表里。
	// 这就是“责任链模式”中“链”的本体。
	authenticators := []authenticator.Request{}
	// 初始化用于 OpenAPI 文档的安全定义。
	securityDefinitions := spec.SecurityDefinitions{}

	// front-proxy first, then remote
	// Add the front proxy authenticator if requested
	// 顺序至关重要：首先处理前置代理（front-proxy），然后是其他认证方式。

	// 1. 添加前置代理认证器（Request Header Authenticator）
	// 这个认证器用于 "聚合API服务器" (Aggregated API Server) 场景。
	// 它信任一个可信的前置代理（如 kube-proxy），并通过检查代理设置的 HTTP-Header 来确定用户身份。
	// 由 --requestheader-client-ca-file, --requestheader-username-headers 等参数开启。
	if c.RequestHeaderConfig != nil {
		requestHeaderAuthenticator := headerrequest.NewDynamicVerifyOptionsSecure(
			c.RequestHeaderConfig.CAContentProvider.VerifyOptions, // CA 用于验证代理的客户端证书
			c.RequestHeaderConfig.AllowedClientNames,              // 允许的代理客户端通用名称(CN)
			c.RequestHeaderConfig.UsernameHeaders,                 // 从哪个 Header 获取用户名
			c.RequestHeaderConfig.UIDHeaders,                      // 从哪个 Header 获取用户 UID
			c.RequestHeaderConfig.GroupHeaders,                    // 从哪个 Header 获取用户组
			c.RequestHeaderConfig.ExtraHeaderPrefixes,             // Header 中用于获取额外信息的前缀
		)
		authenticators = append(authenticators, requestHeaderAuthenticator)
	}

	// x509 client cert auth
	// 2. 添加 X.509 客户端证书认证器
	// 这是标准的认证方式之一，例如 kubectl 使用 kubeconfig 中的客户端证书来认证。
	// 由 --client-ca-file 参数开启。
	if c.ClientCertificateCAContentProvider != nil {
		authenticators = append(authenticators, x509.NewDynamic(c.ClientCertificateCAContentProvider.VerifyOptions, x509.CommonNameUserConversion))
	}

	// 3. 添加 Token 认证器 (这里主要是 Webhook Token 认证器)
	// 如果配置了 TokenAccessReviewClient，意味着启用了 Token Webhook 认证。
	// APIServer 会将收到的 Bearer Token 发送给一个外部服务（Webhook），由该服务来判断 Token 是否有效。
	// 由 --authentication-token-webhook-config-file 参数开启。
	if c.TokenAccessReviewClient != nil {
		// 安全检查：如果使用 webhook，必须配置重试回退策略，以应对 webhook 服务临时不可用的情况。
		if c.WebhookRetryBackoff == nil {
			return nil, nil, errors.New("retry backoff parameters for delegating authentication webhook has not been specified")
		}
		// 创建实际调用 webhook 的认证器实例。
		tokenAuth, err := webhooktoken.NewFromInterface(c.TokenAccessReviewClient, c.APIAudiences, *c.WebhookRetryBackoff, c.TokenAccessReviewTimeout, webhooktoken.AuthenticatorMetrics{
			RecordRequestTotal:   RecordRequestTotal,
			RecordRequestLatency: RecordRequestLatency,
		})
		if err != nil {
			return nil, nil, err
		}
		// 性能优化：在 webhook 认证器外层包裹一个缓存层。
		// 对于同一个 Token，在缓存有效期（TTL）内，无需每次都调用远程 webhook，直接从缓存返回结果。
		cachingTokenAuth := cache.New(tokenAuth, false, c.CacheTTL, c.CacheTTL)
		// 将支持 Token 的认证器添加到责任链中。这里添加了两个：
		// a) bearertoken.New: 用于处理标准的 HTTP "Authorization: Bearer <token>" 头。
		// b) websocket.NewProtocolAuthenticator: 用于处理通过 WebSocket 协议传递的 token（例如 kubectl exec/logs/port-forward）。
		authenticators = append(authenticators, bearertoken.New(cachingTokenAuth), websocket.NewProtocolAuthenticator(cachingTokenAuth))
		// 更新 OpenAPI 定义，告诉 API 客户端本服务支持 "BearerToken" 认证。

		securityDefinitions["BearerToken"] = &spec.SecurityScheme{
			SecuritySchemeProps: spec.SecuritySchemeProps{
				Type:        "apiKey",
				Name:        "authorization",
				In:          "header",
				Description: "Bearer Token authentication",
			},
		}
	}
	// 安全检查：如果经过上述配置，一个认证方式都-没有启用，则需要做出判断。
	if len(authenticators) == 0 {
		// 如果没有其他认证方式，但配置了允许匿名访问。
		if c.Anonymous != nil && c.Anonymous.Enabled {
			// 那么只返回一个匿名认证器。所有请求都将被视为匿名用户。
			return anonymous.NewAuthenticator(c.Anonymous.Conditions), &securityDefinitions, nil
		}
		// 如果连匿名访问都禁止，那么这是一个无效的配置，APIServer 无法启动。
		return nil, nil, errors.New("no authentication method configured")
	}
	// --- 最终的组装 ---

	// 1. 使用 union.New 将列表中的所有认证器组合成一个“联合认证器”。
	//    这个联合认证器会按照 `authenticators` 列表的顺序（代理头 -> X.509 -> Token）依次尝试。
	//    只要有一个成功，整个认证就成功。
	// 2. 使用 group.NewAuthenticatedGroupAdder 在联合认证器外再包裹一层。
	//    它的作用是：任何通过联合认证器成功认证的用户，都会被自动添加到一个名为 "system:authenticated" 的用户组中。
	//    这对于编写针对“所有已认证用户”的授权策略非常方便。
	authenticator := group.NewAuthenticatedGroupAdder(unionauth.New(authenticators...))
	// 如果配置了允许匿名访问，则进行最后一次包裹。
	if c.Anonymous != nil && c.Anonymous.Enabled {
		// 使用 union.NewFailOnError 将“主认证器”和“匿名认证器”组合起来。
		// 它的逻辑是：
		// a) 首先尝试 `authenticator`（即包含代理、X.509、Token 的责任链）。
		// b) 如果 `authenticator` 明确返回了一个错误（比如 Token 格式错误），则整个认证失败。
		// c) 如果 `authenticator` 只是没有找到匹配的用户（但没有出错），则继续尝试 `anonymous.NewAuthenticator()`。
		// 这就实现了“如果所有认证方式都失败了，就将请求视为匿名用户”的逻辑。
		authenticator = unionauth.NewFailOnError(authenticator, anonymous.NewAuthenticator(c.Anonymous.Conditions))
	}
	// 返回最终构建完成的、层次分明的认证器，以及 OpenAPI 安全定义。
	return authenticator, &securityDefinitions, nil
}
