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
	"time"

	"github.com/google/uuid"

	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
)

type SecureServingOptionsWithLoopback struct {
	*SecureServingOptions
}

func (o *SecureServingOptions) WithLoopback() *SecureServingOptionsWithLoopback {
	return &SecureServingOptionsWithLoopback{o}
}

// ApplyTo fills up serving information in the server configuration.
// ApplyTo 将 SecureServingOptionsWithLoopback 的配置应用到服务器配置中。
// 这个函数有两个主要职责：
// 1. 调用内嵌的 SecureServingOptions.ApplyTo，完成所有常规的服务器端 HTTPS 配置。
// 2. 创建一个专门用于“环回”调用的自签名证书和客户端配置。
//
// 参数解释：
//
//	secureServingInfo: 是一个指向 server.SecureServingInfo 指针的指针 (二级指针)。
//	                   函数将把配置好的安全服务信息填充到这里。
//	loopbackClientConfig: 也是一个二级指针，函数将把创建好的环回客户端配置填充到这里
func (s *SecureServingOptionsWithLoopback) ApplyTo(secureServingInfo **server.SecureServingInfo, loopbackClientConfig **rest.Config) error {
	// --- 第一部分：基础校验和常规配置 ---

	// 防御性编程：如果传入的结构体或指针是空的，直接返回，什么也不做。
	if s == nil || s.SecureServingOptions == nil || secureServingInfo == nil {
		return nil
	}

	// 调用内嵌的 SecureServingOptions 的 ApplyTo 方法。
	// 这一步会处理所有通用的 HTTPS 服务器配置，比如绑定地址、端口、主服务证书等。
	// 如果这一步失败了，整个流程就无法继续。
	if err := s.SecureServingOptions.ApplyTo(secureServingInfo); err != nil {
		return err
	}
	// 再次校验：如果经过上一步常规配置后，secureServingInfo 仍然是空的（意味着安全服务被禁用了），
	// 或者我们不需要填充环回配置，那就直接返回。
	if *secureServingInfo == nil || loopbackClientConfig == nil {
		return nil
	}

	// Set a validity period of approximately 3 years for the loopback certificate
	// to avoid kube-apiserver disruptions due to certificate expiration.
	// When this certificate expires, restarting kube-apiserver will automatically
	// regenerate a new certificate with fresh validity dates.

	// --- 第二部分：创建专门用于环回的自签名证书 ---

	// 为环回证书设置一个大约 3 年的有效期。
	// 这是为了避免证书过期导致 kube-apiserver 无法与自己通信，从而造成服务中断。
	// 当这个证书过期时，只需要重启 kube-apiserver，它就会自动重新生成一个新证书。
	maxAge := (3*365 + 1) * 24 * time.Hour

	// create self-signed cert+key with the fake server.LoopbackClientServerNameOverride and
	// let the server return it when the loopback client connects.
	// 使用一个特殊的服务器名 `server.LoopbackClientServerNameOverride` (通常是 "kube-apiserver-loopback")
	// 来生成一个自签名的证书和私钥对。这个证书是专门给环回客户端使用的。
	certPem, keyPem, err := certutil.GenerateSelfSignedCertKeyWithOptions(certutil.SelfSignedCertKeyOptions{
		Host:   server.LoopbackClientServerNameOverride,
		MaxAge: maxAge,
	})
	if err != nil {
		return fmt.Errorf("failed to generate self-signed certificate for loopback connection: %v", err)
	}
	// 将刚刚生成的证书和私钥包装成一个 "SNI 证书提供者"。
	// SNI (Server Name Indication) 是一种技术，允许服务器根据客户端请求的域名来提供不同的证书。
	certProvider, err := dynamiccertificates.NewStaticSNICertKeyContent("self-signed loopback", certPem, keyPem, server.LoopbackClientServerNameOverride)
	if err != nil {
		return fmt.Errorf("failed to generate self-signed certificate for loopback connection: %v", err)
	}

	// Write to the front of SNICerts so that this overrides any other certs with the same name
	// 这一步非常关键：将这个新的证书提供者“插入”到 SNI 证书列表的【最前面】。
	// 这样做是为了确保：当一个客户端使用 "kube-apiserver-loopback" 这个主机名来连接时，
	// 服务器会优先使用我们刚刚生成的这个专用证书，而不是通用的主服务证书。
	(*secureServingInfo).SNICerts = append([]dynamiccertificates.SNICertKeyContentProvider{certProvider}, (*secureServingInfo).SNICerts...)

	// --- 第三部分：创建环回客户端配置 ---

	// 使用服务器信息和刚刚生成的证书，创建一个新的环回客户端配置 (rest.Config)。
	// 这个配置包含了连接到服务器所需的所有信息：API 服务器地址、CA 证书、客户端证书等。
	// uuid.New().String() 用作生成临时 kubeconfig 文件时的唯一标识。
	secureLoopbackClientConfig, err := (*secureServingInfo).NewLoopbackClientConfig(uuid.New().String(), certPem)
	// 这里处理创建环回配置可能出现的成功或失败情况。
	switch {
	// if we failed and there's no fallback loopback client config, we need to fail
	// 情况一：创建失败了，并且之前也没有任何备用的（比如不安全的）环回配置。
	// 这是一个致命错误，因为 apiserver 将无法与自己通信。
	case err != nil && *loopbackClientConfig == nil:
		// 清理现场：把我们刚刚添加的 SNI 证书移除，因为它已经没用了。
		(*secureServingInfo).SNICerts = (*secureServingInfo).SNICerts[1:]
		return err

	// if we failed, but we already have a fallback loopback client config (usually insecure), allow it
	// 情况二：创建失败了，但是我们已经有了一个备用的环回配置。
	// 这种情况允许 apiserver 降级使用那个备用配置（即使它可能是不安全的），保证启动成功。
	// 所以这里什么也不做，直接跳过。
	case err != nil && *loopbackClientConfig != nil:
	// 情况三（默认）：创建成功了！
	// 将新创建的安全环回配置赋值给传入的二级指针，这样调用者就能拿到这个配置了。
	default:
		*loopbackClientConfig = secureLoopbackClientConfig
	}

	return nil
}
