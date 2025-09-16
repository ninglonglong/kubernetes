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

// Package options contains flags and options for initializing kube-apiserver
package options

import (
	genericoptions "k8s.io/apiserver/pkg/server/options"
	netutils "k8s.io/utils/net"
)

// NewSecureServingOptions gives default values for the kube-apiserver which are not the options wanted by
// "normal" API servers running on the platform
// NewSecureServingOptions 创建并返回一个带有推荐默认值的 SecureServingOptionsWithLoopback 对象。
// 这个函数专门为 kube-apiserver 这类需要与自身进行安全通信的组件设计
func NewSecureServingOptions() *genericoptions.SecureServingOptionsWithLoopback {
	// 创建一个 SecureServingOptions 结构体实例，并填充默认值。
	o := genericoptions.SecureServingOptions{
		// BindAddress 设置服务器监听的 IP 地址。
		// "0.0.0.0" 是一个特殊地址，表示监听本机上所有可用的网络接口（IPv4）。
		// 这使得 API 服务器不仅能被本地访问，也能被网络中的其他机器访问。
		BindAddress: netutils.ParseIPSloppy("0.0.0.0"),
		// BindPort 设置服务器监听的端口号。
		// 6443 是 Kubernetes API 服务器约定俗成的默认安全端口 (HTTPS)。
		BindPort: 6443,
		// Required 标志表示安全服务是必需的。
		// 如果设置为 true，那么在启动时必须成功配置并启动 HTTPS 服务，否则组件将启动失败。
		Required: true,
		// ServerCert 配置服务器证书。
		// GeneratableKeyCert 类型表示这个证书和私钥对是“可生成的”。
		// 如果在指定的路径下找不到证书，服务器会尝试自动生成一个自签名的证书。
		ServerCert: genericoptions.GeneratableKeyCert{
			PairName:      "apiserver",
			CertDirectory: "/var/run/kubernetes",
		},
	}
	return o.WithLoopback()
}
