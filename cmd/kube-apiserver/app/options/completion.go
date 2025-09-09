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

package options

import (
	"context"
	"fmt"
	"net"
	"strings"

	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
	netutils "k8s.io/utils/net"

	cp "k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	"k8s.io/kubernetes/pkg/kubeapiserver"
)

// completedOptions is a private wrapper that enforces a call of Complete() before Run can be invoked.
type completedOptions struct {
	cp.CompletedOptions

	Extra
}

type CompletedOptions struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedOptions
}

// Complete set default ServerRunOptions.
// Should be called after kube-apiserver flags parsed.
func (s *ServerRunOptions) Complete(ctx context.Context) (CompletedOptions, error) {
	if s == nil {
		return CompletedOptions{completedOptions: &completedOptions{}}, nil
	}

	// process s.ServiceClusterIPRange from list to Primary and Secondary
	// we process secondary only if provided by user
	// 步骤 2: 解析 Service Cluster IP 地址范围
	apiServerServiceIP, primaryServiceIPRange, secondaryServiceIPRange, err := getServiceIPAndRanges(s.ServiceClusterIPRanges)
	if err != nil {
		return CompletedOptions{}, err
	}
	// 步骤 3: 调用内嵌的 GenericAPIServer Options 的 Complete 方法
	// s.Options 是一个通用的 apiserver 配置，它包含了像认证、授权、安全端口等通用设置。
	// 这里调用它的 Complete 方法，完成这些通用配置的补全。
	// []string{"kubernetes.default.svc", ...} 是 apiserver 的服务名称，用于生成自签名证书。
	// []net.IP{apiServerServiceIP} 是证书需要包含的 SANs (Subject Alternative Names)。
	controlplane, err := s.Options.Complete(ctx, []string{"kubernetes.default.svc", "kubernetes.default", "kubernetes"}, []net.IP{apiServerServiceIP})
	if err != nil {
		return CompletedOptions{}, err
	}

	// 步骤 4: 组装最终的 completedOptions 对象
	// 创建一个 kube-apiserver 特有的 completedOptions 结构体。
	completed := completedOptions{
		CompletedOptions: controlplane,

		Extra: s.Extra,
	}

	completed.PrimaryServiceClusterIPRange = primaryServiceIPRange
	completed.SecondaryServiceClusterIPRange = secondaryServiceIPRange
	completed.APIServerServiceIP = apiServerServiceIP

	if completed.Etcd != nil && completed.Etcd.EnableWatchCache {
		sizes := kubeapiserver.DefaultWatchCacheSizes()
		// Ensure that overrides parse correctly.
		userSpecified, err := apiserveroptions.ParseWatchCacheSizes(completed.Etcd.WatchCacheSizes)
		if err != nil {
			return CompletedOptions{}, err
		}
		for resource, size := range userSpecified {
			sizes[resource] = size
		}
		completed.Etcd.WatchCacheSizes, err = apiserveroptions.WriteWatchCacheSizes(sizes)
		if err != nil {
			return CompletedOptions{}, err
		}
	}

	return CompletedOptions{
		completedOptions: &completed,
	}, nil
}

func getServiceIPAndRanges(serviceClusterIPRanges string) (net.IP, net.IPNet, net.IPNet, error) {
	serviceClusterIPRangeList := []string{}
	if serviceClusterIPRanges != "" {
		serviceClusterIPRangeList = strings.Split(serviceClusterIPRanges, ",")
	}

	var apiServerServiceIP net.IP
	var primaryServiceIPRange net.IPNet
	var secondaryServiceIPRange net.IPNet
	var err error
	// nothing provided by user, use default range (only applies to the Primary)
	if len(serviceClusterIPRangeList) == 0 {
		var primaryServiceClusterCIDR net.IPNet
		primaryServiceIPRange, apiServerServiceIP, err = cp.ServiceIPRange(primaryServiceClusterCIDR)
		if err != nil {
			return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("error determining service IP ranges: %v", err)
		}
		return apiServerServiceIP, primaryServiceIPRange, net.IPNet{}, nil
	}

	_, primaryServiceClusterCIDR, err := netutils.ParseCIDRSloppy(serviceClusterIPRangeList[0])
	if err != nil {
		return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("service-cluster-ip-range[0] is not a valid cidr")
	}

	primaryServiceIPRange, apiServerServiceIP, err = cp.ServiceIPRange(*primaryServiceClusterCIDR)
	if err != nil {
		return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("error determining service IP ranges for primary service cidr: %v", err)
	}

	// user provided at least two entries
	// note: validation asserts that the list is max of two dual stack entries
	if len(serviceClusterIPRangeList) > 1 {
		_, secondaryServiceClusterCIDR, err := netutils.ParseCIDRSloppy(serviceClusterIPRangeList[1])
		if err != nil {
			return net.IP{}, net.IPNet{}, net.IPNet{}, fmt.Errorf("service-cluster-ip-range[1] is not an ip net")
		}
		secondaryServiceIPRange = *secondaryServiceClusterCIDR
	}
	return apiServerServiceIP, primaryServiceIPRange, secondaryServiceIPRange, nil
}
