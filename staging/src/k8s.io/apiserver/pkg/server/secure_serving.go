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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
)

const (
	defaultKeepAlivePeriod = 3 * time.Minute
)

// tlsConfig produces the tls.Config to serve with.
func (s *SecureServingInfo) tlsConfig(stopCh <-chan struct{}) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		// Can't use SSLv3 because of POODLE and BEAST
		// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
		// Can't use TLSv1.1 because of RC4 cipher usage
		MinVersion: tls.VersionTLS12,
		// enable HTTP2 for go's 1.7 HTTP Server
		NextProtos: []string{"h2", "http/1.1"},
	}

	// these are static aspects of the tls.Config
	if s.DisableHTTP2 {
		klog.Info("Forcing use of http/1.1 only")
		tlsConfig.NextProtos = []string{"http/1.1"}
	}
	if s.MinTLSVersion > 0 {
		tlsConfig.MinVersion = s.MinTLSVersion
	}
	if len(s.CipherSuites) > 0 {
		tlsConfig.CipherSuites = s.CipherSuites
		insecureCiphers := flag.InsecureTLSCiphers()
		for i := 0; i < len(s.CipherSuites); i++ {
			for cipherName, cipherID := range insecureCiphers {
				if s.CipherSuites[i] == cipherID {
					klog.Warningf("Use of insecure cipher '%s' detected.", cipherName)
				}
			}
		}
	}

	if s.ClientCA != nil {
		// Populate PeerCertificates in requests, but don't reject connections without certificates
		// This allows certificates to be validated by authenticators, while still allowing other auth types
		tlsConfig.ClientAuth = tls.RequestClientCert
	}

	if s.ClientCA != nil || s.Cert != nil || len(s.SNICerts) > 0 {
		dynamicCertificateController := dynamiccertificates.NewDynamicServingCertificateController(
			tlsConfig,
			s.ClientCA,
			s.Cert,
			s.SNICerts,
			nil, // TODO see how to plumb an event recorder down in here. For now this results in simply klog messages.
		)

		if s.ClientCA != nil {
			s.ClientCA.AddListener(dynamicCertificateController)
		}
		if s.Cert != nil {
			s.Cert.AddListener(dynamicCertificateController)
		}
		// generate a context from stopCh. This is to avoid modifying files which are relying on apiserver
		// TODO: See if we can pass ctx to the current method
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-stopCh:
				cancel() // stopCh closed, so cancel our context
			case <-ctx.Done():
			}
		}()
		// start controllers if possible
		if controller, ok := s.ClientCA.(dynamiccertificates.ControllerRunner); ok {
			// runonce to try to prime data.  If this fails, it's ok because we fail closed.
			// Files are required to be populated already, so this is for convenience.
			if err := controller.RunOnce(ctx); err != nil {
				klog.Warningf("Initial population of client CA failed: %v", err)
			}

			go controller.Run(ctx, 1)
		}
		if controller, ok := s.Cert.(dynamiccertificates.ControllerRunner); ok {
			// runonce to try to prime data.  If this fails, it's ok because we fail closed.
			// Files are required to be populated already, so this is for convenience.
			if err := controller.RunOnce(ctx); err != nil {
				klog.Warningf("Initial population of default serving certificate failed: %v", err)
			}

			go controller.Run(ctx, 1)
		}
		for _, sniCert := range s.SNICerts {
			sniCert.AddListener(dynamicCertificateController)
			if controller, ok := sniCert.(dynamiccertificates.ControllerRunner); ok {
				// runonce to try to prime data.  If this fails, it's ok because we fail closed.
				// Files are required to be populated already, so this is for convenience.
				if err := controller.RunOnce(ctx); err != nil {
					klog.Warningf("Initial population of SNI serving certificate failed: %v", err)
				}

				go controller.Run(ctx, 1)
			}
		}

		// runonce to try to prime data.  If this fails, it's ok because we fail closed.
		// Files are required to be populated already, so this is for convenience.
		if err := dynamicCertificateController.RunOnce(); err != nil {
			klog.Warningf("Initial population of dynamic certificates failed: %v", err)
		}
		go dynamicCertificateController.Run(1, stopCh)

		tlsConfig.GetConfigForClient = dynamicCertificateController.GetConfigForClient
	}

	return tlsConfig, nil
}

// Serve runs the secure http server. It fails only if certificates cannot be loaded or the initial listen call fails.
// The actual server loop (stoppable by closing stopCh) runs in a go routine, i.e. Serve does not block.
// It returns a stoppedCh that is closed when all non-hijacked active requests have been processed.
// It returns a listenerStoppedCh that is closed when the underlying http Server has stopped listening.
// Serve 方法的作用是：根据 SecureServingInfo 中的配置，创建一个功能完备、经过优化的 http.Server，
// 然后调用 RunServer 来以非阻塞的方式启动它。
//
// 参数:
//   - handler:         要服务的根 http.Handler (通常是 apiserver 的主 Mux)。
//   - shutdownTimeout: 优雅关闭时，等待已有连接处理完毕的超时时间。
//   - stopCh:          一个 channel，当它被关闭时，服务器将开始优雅关闭流程。
func (s *SecureServingInfo) Serve(handler http.Handler, shutdownTimeout time.Duration, stopCh <-chan struct{}) (<-chan struct{}, <-chan struct{}, error) {
	klog.V(2).InfoS("Preparing to serve on secure port")
	// 步骤 1: 基本检查
	// 确保监听器 (Listener) 已经被创建。Listener 是在之前的 PrepareRun 阶段通过 `net.Listen` 创建的，
	// 它已经绑定了 apiserver 的安全端口（如 6443）。
	if s.Listener == nil {
		return nil, nil, fmt.Errorf("listener must not be nil")
	}
	// 步骤 2: 配置 TLS
	// `s.tlsConfig()` 会根据配置（如 --tls-cert-file, --tls-private-key-file, --client-ca-file）
	// 创建一个 `*tls.Config` 对象。这个对象包含了所有用于 HTTPS 的证书、密钥和客户端认证策略。
	// 它还会设置动态证书重载的逻辑。
	tlsConfig, err := s.tlsConfig(stopCh)
	if err != nil {
		return nil, nil, err
	}
	// 步骤 3: 创建并配置 http.Server 实例
	klog.V(4).InfoS("Creating and configuring http.Server")
	secureServer := &http.Server{
		// Addr: 服务器的监听地址，主要用于日志记录。
		Addr: s.Listener.Addr().String(),
		// Handler: 整个 apiserver 的请求分发器。
		Handler: handler,
		// MaxHeaderBytes: 限制 HTTP Header 的最大大小，防止 Header 攻击。
		MaxHeaderBytes: 1 << 20,
		// TLSConfig: 应用上一步创建的 TLS 配置。
		TLSConfig: tlsConfig,
		// IdleTimeout: 连接空闲超时。如果一个 keep-alive 连接在这个时间内没有任何活动，服务器会关闭它。
		// 90 秒与 Go 默认的 http.DefaultTransport 的 keep-alive 超时匹配，有助于回收空闲连接。
		IdleTimeout: 90 * time.Second, // matches http.DefaultTransport keep-alive timeout
		// ReadHeaderTimeout: 读取 HTTP Header 的超时时间。防止慢客户端攻击（Slowloris）。
		// 32 秒比 apiserver 的默认请求超时（30秒）稍长。
		ReadHeaderTimeout: 32 * time.Second, // just shy of requestTimeoutUpperBound
	}
	// 步骤 4: 专门为 HTTP/2 进行性能优化
	// kube-apiserver 内部组件之间的通信（Loopback）以及一些现代客户端（如 gRPC-proxies）会使用 HTTP/2。
	// 这里对 HTTP/2 的参数进行了精细的调优。
	if !s.DisableHTTP2 {
		// At least 99% of serialized resources in surveyed clusters were smaller than 256kb.
		// This should be big enough to accommodate most API POST requests in a single frame,
		// and small enough to allow a per connection buffer of this size multiplied by `MaxConcurrentStreams`.
		klog.V(4).InfoS("Configuring HTTP/2 options")

		// 调研发现，99% 的 K8s 资源对象序列化后小于 256KB。
		// 这个值被用作基准。
		const resourceBody99Percentile = 256 * 1024

		http2Options := &http2.Server{
			IdleTimeout: 90 * time.Second, // matches http.DefaultTransport keep-alive timeout
			// shrink the per-stream buffer and max framesize from the 1MB default while still accommodating most API POST requests in a single frame

			// 优化缓冲区和帧大小：
			// 默认的 1MB 缓冲区对于大量小请求来说可能过于浪费。
			// 将它们缩小到 256KB，既能容纳绝大多数单个的 POST 请求，又能节省内存。
			MaxUploadBufferPerStream: resourceBody99Percentile,
			MaxReadFrameSize:         resourceBody99Percentile,
		}

		// use the overridden concurrent streams setting or make the default of 250 explicit so we can size MaxUploadBufferPerConnection appropriately
		// 配置每个连接的最大并发流数量。
		if s.HTTP2MaxStreamsPerConnection > 0 {
			// 如果用户通过 flag 自定义了，就使用用户的值。
			http2Options.MaxConcurrentStreams = uint32(s.HTTP2MaxStreamsPerConnection)
		} else {
			// match http2.initialMaxConcurrentStreams used by clients
			// this makes it so that a malicious client can only open 400 streams before we forcibly close the connection
			// https://github.com/golang/net/commit/b225e7ca6dde1ef5a5ae5ce922861bda011cfabd
			// 否则，使用一个合理的默认值 100。
			// 这可以防止恶意客户端通过打开大量并发流来耗尽服务器资源。
			http2Options.MaxConcurrentStreams = 100
		}

		// 根据并发流数量，计算每个连接的总上传缓冲区大小。
		// 这是为了确保在最坏情况下（所有并发流都在上传数据），服务器有足够的缓冲区。
		// increase the connection buffer size from the 1MB default to handle the specified number of concurrent streams
		http2Options.MaxUploadBufferPerConnection = http2Options.MaxUploadBufferPerStream * int32(http2Options.MaxConcurrentStreams)
		// apply settings to the server
		// 将这些优化后的 HTTP/2 设置应用到 `secureServer` 上。

		if err := http2.ConfigureServer(secureServer, http2Options); err != nil {
			return nil, nil, fmt.Errorf("error configuring http2: %v", err)
		}
	}
	// 步骤 5: 配置错误日志记录器
	// 默认的 http.Server 会将 TLS 握手错误打印到标准错误输出（os.Stderr），
	// 这可能会与 apiserver 的结构化日志（klog）混在一起，造成混乱。
	// 这里创建一个自定义的 writer 和 logger，专门用于捕获和记录 TLS 握手错误，
	// 使日志输出更干净。
	// use tlsHandshakeErrorWriter to handle messages of tls handshake error
	tlsErrorWriter := &tlsHandshakeErrorWriter{os.Stderr}
	tlsErrorLogger := log.New(tlsErrorWriter, "", 0)
	secureServer.ErrorLog = tlsErrorLogger
	// 步骤 6: 委托给 RunServer 来启动服务器
	// 所有准备工作都已完成，现在 `secureServer` 是一个完全配置好的实例。
	// `RunServer` 是一个辅助函数，它会负责启动 `secureServer` 的 goroutine 并处理关闭逻辑。
	klog.Infof("Serving securely on %s", secureServer.Addr)
	return RunServer(secureServer, s.Listener, shutdownTimeout, stopCh)
}

// RunServer spawns a go-routine continuously serving until the stopCh is
// closed.
// It returns a stoppedCh that is closed when all non-hijacked active requests
// have been processed.
// This function does not block
// TODO: make private when insecure serving is gone from the kube-apiserver
// RunServer 函数是一个通用的辅助函数，用于以非阻塞的方式启动一个 http.Server，
// 并处理其优雅关闭的逻辑。
//
// 参数:
//   - server:          已经完全配置好的 *http.Server 实例。
//   - ln:              服务器要监听的 net.Listener。
//   - shutDownTimeout: 调用 server.Shutdown() 时的超时时间。
//   - stopCh:          一个 channel，当它被关闭时，将触发服务器的关闭流程。
//
// 返回值:
//   - <-chan struct{}: 第一个 channel (`serverShutdownCh`)，当 server.Shutdown() 完成后，它会被关闭。
//   - <-chan struct{}: 第二个 channel (`listenerStoppedCh`)，当 server.Serve() 返回（即监听停止）后，它会被关闭。
//   - error:           如果传入的 listener 为 nil，则返回错误。
func RunServer(
	server *http.Server,
	ln net.Listener,
	shutDownTimeout time.Duration,
	stopCh <-chan struct{},
) (<-chan struct{}, <-chan struct{}, error) {
	// 步骤 1: 基本检查
	if ln == nil {
		return nil, nil, fmt.Errorf("listener must not be nil")
	}

	// Shutdown server gracefully.
	// 步骤 2: 创建用于监控关闭状态的 channel
	serverShutdownCh, listenerStoppedCh := make(chan struct{}), make(chan struct{})
	// ----------------------------------------------------------------------------------
	// Goroutine 1: "关闭监控者"
	// ----------------------------------------------------------------------------------
	// 启动一个 goroutine，它的唯一职责就是等待关闭信号，并执行优雅关闭。
	go func() {
		// 当这个 goroutine 退出时，关闭 serverShutdownCh，通知上层调用者“关闭操作已完成”。
		defer close(serverShutdownCh)
		// **阻塞**，直到 stopCh 被关闭（即收到了全局的关闭命令）。
		<-stopCh
		klog.V(2).InfoS("Shutdown signal received, calling server.Shutdown")
		// 创建一个带有超时的 context，用于 `server.Shutdown()`。
		// 这确保了优雅关闭过程本身不会无限期地阻塞。
		ctx, cancel := context.WithTimeout(context.Background(), shutDownTimeout)
		defer cancel()
		// **执行优雅关闭**。
		// `server.Shutdown()` 会：
		// 1. 立即关闭所有空闲的 keep-alive 连接。
		// 2. 停止接受新的连接。
		// 3. 等待所有正在处理的请求完成，直到传入的 context 超时。
		err := server.Shutdown(ctx)
		if err != nil {
			klog.Errorf("Failed to shutdown server: %v", err)
		}
	}()
	// ----------------------------------------------------------------------------------
	// Goroutine 2: "服务器运行者"
	// ----------------------------------------------------------------------------------
	// 启动另一个 goroutine，它的职责是真正地运行 HTTP 服务器。
	go func() {
		// 使用 `defer HandleCrash()` 来捕获这个 goroutine 中可能发生的 panic，防止整个程序崩溃。
		defer utilruntime.HandleCrash()
		// 当这个 goroutine 退出时（无论是正常关闭还是出错），关闭 listenerStoppedCh，
		// 通知上层调用者“服务器已停止监听”
		defer close(listenerStoppedCh)
		// 对原始的 net.Listener 进行包装。
		var listener net.Listener
		// 包装一层 `tcpKeepAliveListener`，它会为接受的每个 TCP 连接都设置 TCP Keep-Alive。
		// 这有助于检测和清理半开的、僵死的连接。
		listener = tcpKeepAliveListener{ln}
		// 如果是 HTTPS 服务 (TLSConfig 不为 nil)，再包装一层 `tls.NewListener`。
		// 这个包装器会在原始的 TCP 连接上执行 TLS 握手，将一个普通的 TCP listener 变成一个加密的 TLS listener。
		if server.TLSConfig != nil {
			listener = tls.NewListener(listener, server.TLSConfig)
		}
		klog.V(4).InfoS("Starting server.Serve on listener", "address", ln.Addr().String())
		// **核心启动调用！**
		// `server.Serve(listener)` 是一个**阻塞**调用。它会开始在一个循环中接受和处理连接，
		// 直到 `listener` 关闭，或者 `server.Shutdown()` 被调用，或者发生不可恢复的错误。
		err := server.Serve(listener)
		// `server.Serve` 返回后，记录一条日志。
		msg := fmt.Sprintf("Stopped listening on %s", ln.Addr().String())
		// 判断 `server.Serve` 是如何退出的。
		select {
		case <-stopCh:
			klog.Info(msg)
		default:
			panic(fmt.Sprintf("%s due to error: %v", msg, err))
		}
	}()
	// 步骤 3: 立即返回 channel
	// 函数不等待任何 goroutine 完成，而是立即返回这两个 channel。
	// 这使得 `RunServer` 成为一个非阻塞的启动函数。
	// 上层调用者可以通过这两个 channel 来异步地监控服务器的关闭状态。
	return serverShutdownCh, listenerStoppedCh, nil
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
//
// Copied from Go 1.7.2 net/http/server.go
type tcpKeepAliveListener struct {
	net.Listener
}

func (ln tcpKeepAliveListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(defaultKeepAlivePeriod)
	}
	return c, nil
}

// tlsHandshakeErrorWriter writes TLS handshake errors to klog with
// trace level - V(5), to avoid flooding of tls handshake errors.
type tlsHandshakeErrorWriter struct {
	out io.Writer
}

const tlsHandshakeErrorPrefix = "http: TLS handshake error"

func (w *tlsHandshakeErrorWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), tlsHandshakeErrorPrefix) {
		klog.V(5).Info(string(p))
		metrics.TLSHandshakeErrors.Inc()
		return len(p), nil
	}

	// for non tls handshake error, log it as usual
	return w.out.Write(p)
}
