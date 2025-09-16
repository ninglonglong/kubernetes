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

package options

import (
	"fmt"
	"strings"
	"time"

	authorizationcel "k8s.io/apiserver/pkg/authorization/cel"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	authzconfig "k8s.io/apiserver/pkg/apis/apiserver"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	versionedinformers "k8s.io/client-go/informers"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer"
	authzmodes "k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
)

const (
	defaultWebhookName                      = "default"
	authorizationModeFlag                   = "authorization-mode"
	authorizationWebhookConfigFileFlag      = "authorization-webhook-config-file"
	authorizationWebhookVersionFlag         = "authorization-webhook-version"
	authorizationWebhookAuthorizedTTLFlag   = "authorization-webhook-cache-authorized-ttl"
	authorizationWebhookUnauthorizedTTLFlag = "authorization-webhook-cache-unauthorized-ttl"
	authorizationPolicyFileFlag             = "authorization-policy-file"
	authorizationConfigFlag                 = "authorization-config"
)

// BuiltInAuthorizationOptions contains all build-in authorization options for API Server
type BuiltInAuthorizationOptions struct {
	Modes                       []string
	PolicyFile                  string
	WebhookConfigFile           string
	WebhookVersion              string
	WebhookCacheAuthorizedTTL   time.Duration
	WebhookCacheUnauthorizedTTL time.Duration
	// WebhookRetryBackoff specifies the backoff parameters for the authorization webhook retry logic.
	// This allows us to configure the sleep time at each iteration and the maximum number of retries allowed
	// before we fail the webhook call in order to limit the fan out that ensues when the system is degraded.
	WebhookRetryBackoff *wait.Backoff

	// AuthorizationConfigurationFile is mutually exclusive with all of:
	//	- Modes
	//	- WebhookConfigFile
	//	- WebHookVersion
	//	- WebhookCacheAuthorizedTTL
	//	- WebhookCacheUnauthorizedTTL
	AuthorizationConfigurationFile string

	AreLegacyFlagsSet func() bool
}

// NewBuiltInAuthorizationOptions create a BuiltInAuthorizationOptions with default value
func NewBuiltInAuthorizationOptions() *BuiltInAuthorizationOptions {
	return &BuiltInAuthorizationOptions{
		Modes:                       []string{},
		WebhookVersion:              "v1beta1",
		WebhookCacheAuthorizedTTL:   5 * time.Minute,
		WebhookCacheUnauthorizedTTL: 30 * time.Second,
		WebhookRetryBackoff:         genericoptions.DefaultAuthWebhookRetryBackoff(),
	}
}

// Complete modifies authorization options
// Complete 修改授权选项，为其填充默认值。

func (o *BuiltInAuthorizationOptions) Complete() []error {
	// 防御性编程：如果选项对象本身是 nil，则直接返回，什么也不做。

	if o == nil {
		return nil
	}

	// 这是整个函数的核心逻辑：检查用户是否配置了任何授权模式。
	//
	// `len(o.AuthorizationConfigurationFile) == 0`: 检查用户是否提供了授权配置文件。
	//                                              在现代版本中，这主要用于 Webhook 模式。
	// `len(o.Modes) == 0`: 检查用户是否通过 `--authorization-mode` 参数指定了任何授权模式。
	//
	// 如果两个条件【同时】成立，意味着用户完全没有配置任何授权机制。
	if len(o.AuthorizationConfigurationFile) == 0 && len(o.Modes) == 0 {
		// 在这种“零配置”的情况下，为了让服务器能够启动并响应请求（尤其是在测试或极简环境中），
		// 系统会默认启用 "AlwaysAllow" 模式。
		//
		// `authzmodes.ModeAlwaysAllow` 的值就是 "AlwaysAllow"。
		// 这个授权模式会对所有请求都说“是”，即允许所有操作。
		//
		// 警告：这在生产环境中是极其不安全的！
		o.Modes = []string{authzmodes.ModeAlwaysAllow}
	}
	// 如果用户配置了任何一种授权模式（例如 `--authorization-mode=RBAC`），
	// `if` 条件就不会满足，此函数将什么也不做，完全尊重用户的配置。
	return nil
}

// Validate checks invalid config combination
func (o *BuiltInAuthorizationOptions) Validate() []error {
	if o == nil {
		return nil
	}
	var allErrors []error

	// if --authorization-config is set, check if
	// 	- the feature flag is set
	//	- legacyFlags are not set
	//	- the config file can be loaded
	//	- the config file represents a valid configuration
	if o.AuthorizationConfigurationFile != "" {
		if !utilfeature.DefaultFeatureGate.Enabled(genericfeatures.StructuredAuthorizationConfiguration) {
			return append(allErrors, fmt.Errorf("--%s cannot be used without enabling StructuredAuthorizationConfiguration feature flag", authorizationConfigFlag))
		}

		// error out if legacy flags are defined
		if o.AreLegacyFlagsSet != nil && o.AreLegacyFlagsSet() {
			return append(allErrors, fmt.Errorf("--%s can not be specified when --%s or --authorization-webhook-* flags are defined", authorizationConfigFlag, authorizationModeFlag))
		}

		// load/validate kube-apiserver authz config with no opinion about required modes
		_, _, err := authorizer.LoadAndValidateFile(o.AuthorizationConfigurationFile, authorizationcel.NewDefaultCompiler(), nil)
		if err != nil {
			return append(allErrors, err)
		}

		return allErrors
	}

	// validate the legacy flags using the legacy mode if --authorization-config is not passed
	if len(o.Modes) == 0 {
		allErrors = append(allErrors, fmt.Errorf("at least one authorization-mode must be passed"))
	}

	modes := sets.NewString(o.Modes...)
	for _, mode := range o.Modes {
		if !authzmodes.IsValidAuthorizationMode(mode) {
			allErrors = append(allErrors, fmt.Errorf("authorization-mode %q is not a valid mode", mode))
		}
		if mode == authzmodes.ModeABAC && o.PolicyFile == "" {
			allErrors = append(allErrors, fmt.Errorf("authorization-mode ABAC's authorization policy file not passed"))
		}
		if mode == authzmodes.ModeWebhook && o.WebhookConfigFile == "" {
			allErrors = append(allErrors, fmt.Errorf("authorization-mode Webhook's authorization config file not passed"))
		}
	}

	if o.PolicyFile != "" && !modes.Has(authzmodes.ModeABAC) {
		allErrors = append(allErrors, fmt.Errorf("cannot specify --authorization-policy-file without mode ABAC"))
	}

	if o.WebhookConfigFile != "" && !modes.Has(authzmodes.ModeWebhook) {
		allErrors = append(allErrors, fmt.Errorf("cannot specify --authorization-webhook-config-file without mode Webhook"))
	}

	if len(o.Modes) != modes.Len() {
		allErrors = append(allErrors, fmt.Errorf("authorization-mode %q has mode specified more than once", o.Modes))
	}

	if o.WebhookRetryBackoff != nil && o.WebhookRetryBackoff.Steps <= 0 {
		allErrors = append(allErrors, fmt.Errorf("number of webhook retry attempts must be greater than 0, but is: %d", o.WebhookRetryBackoff.Steps))
	}

	return allErrors
}

// AddFlags returns flags of authorization for a API Server
func (o *BuiltInAuthorizationOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.StringSliceVar(&o.Modes, authorizationModeFlag, o.Modes, ""+
		"Ordered list of plug-ins to do authorization on secure port. Defaults to AlwaysAllow if --authorization-config is not used. Comma-delimited list of: "+
		strings.Join(authzmodes.AuthorizationModeChoices, ",")+".")

	fs.StringVar(&o.PolicyFile, authorizationPolicyFileFlag, o.PolicyFile, ""+
		"File with authorization policy in json line by line format, used with --authorization-mode=ABAC, on the secure port.")

	fs.StringVar(&o.WebhookConfigFile, authorizationWebhookConfigFileFlag, o.WebhookConfigFile, ""+
		"File with webhook configuration in kubeconfig format, used with --authorization-mode=Webhook. "+
		"The API server will query the remote service to determine access on the API server's secure port.")

	fs.StringVar(&o.WebhookVersion, authorizationWebhookVersionFlag, o.WebhookVersion, ""+
		"The API version of the authorization.k8s.io SubjectAccessReview to send to and expect from the webhook.")

	fs.DurationVar(&o.WebhookCacheAuthorizedTTL, authorizationWebhookAuthorizedTTLFlag,
		o.WebhookCacheAuthorizedTTL,
		"The duration to cache 'authorized' responses from the webhook authorizer.")

	fs.DurationVar(&o.WebhookCacheUnauthorizedTTL,
		authorizationWebhookUnauthorizedTTLFlag, o.WebhookCacheUnauthorizedTTL,
		"The duration to cache 'unauthorized' responses from the webhook authorizer.")

	fs.StringVar(&o.AuthorizationConfigurationFile, authorizationConfigFlag, o.AuthorizationConfigurationFile, ""+
		"File with Authorization Configuration to configure the authorizer chain. "+
		"Requires feature gate StructuredAuthorizationConfiguration. "+
		"This flag is mutually exclusive with the other --authorization-mode and --authorization-webhook-* flags.")

	// preserves compatibility with any method set during initialization
	oldAreLegacyFlagsSet := o.AreLegacyFlagsSet
	o.AreLegacyFlagsSet = func() bool {
		if oldAreLegacyFlagsSet != nil && oldAreLegacyFlagsSet() {
			return true
		}

		return fs.Changed(authorizationModeFlag) ||
			fs.Changed(authorizationWebhookConfigFileFlag) ||
			fs.Changed(authorizationWebhookVersionFlag) ||
			fs.Changed(authorizationWebhookAuthorizedTTLFlag) ||
			fs.Changed(authorizationWebhookUnauthorizedTTLFlag)
	}
}

// ToAuthorizationConfig convert BuiltInAuthorizationOptions to authorizer.Config
func (o *BuiltInAuthorizationOptions) ToAuthorizationConfig(versionedInformerFactory versionedinformers.SharedInformerFactory) (*authorizer.Config, error) {
	if o == nil {
		return nil, nil
	}

	var authorizationConfiguration *authzconfig.AuthorizationConfiguration
	var err error
	var authorizationConfigData string

	// if --authorization-config is set, check if
	// 	- the feature flag is set
	//	- legacyFlags are not set
	//	- the config file can be loaded
	//	- the config file represents a valid configuration
	// else,
	//	- build the AuthorizationConfig from the legacy flags
	if o.AuthorizationConfigurationFile != "" {
		if !utilfeature.DefaultFeatureGate.Enabled(genericfeatures.StructuredAuthorizationConfiguration) {
			return nil, fmt.Errorf("--%s cannot be used without enabling StructuredAuthorizationConfiguration feature flag", authorizationConfigFlag)
		}
		// error out if legacy flags are defined
		if o.AreLegacyFlagsSet != nil && o.AreLegacyFlagsSet() {
			return nil, fmt.Errorf("--%s can not be specified when --%s or --authorization-webhook-* flags are defined", authorizationConfigFlag, authorizationModeFlag)
		}
		// load/validate kube-apiserver authz config with no opinion about required modes
		authorizationConfiguration, authorizationConfigData, err = authorizer.LoadAndValidateFile(o.AuthorizationConfigurationFile, authorizationcel.NewDefaultCompiler(), nil)
		if err != nil {
			return nil, err
		}
	} else {
		authorizationConfiguration, err = o.buildAuthorizationConfiguration()
		if err != nil {
			return nil, fmt.Errorf("failed to build authorization config: %s", err)
		}
	}

	return &authorizer.Config{
		PolicyFile:               o.PolicyFile,
		VersionedInformerFactory: versionedInformerFactory,
		WebhookRetryBackoff:      o.WebhookRetryBackoff,

		ReloadFile:                            o.AuthorizationConfigurationFile,
		AuthorizationConfiguration:            authorizationConfiguration,
		InitialAuthorizationConfigurationData: authorizationConfigData,
	}, nil
}

// buildAuthorizationConfiguration converts existing flags to the AuthorizationConfiguration format
func (o *BuiltInAuthorizationOptions) buildAuthorizationConfiguration() (*authzconfig.AuthorizationConfiguration, error) {
	var authorizers []authzconfig.AuthorizerConfiguration

	if len(o.Modes) != sets.NewString(o.Modes...).Len() {
		return nil, fmt.Errorf("modes should not be repeated in --authorization-mode")
	}

	for _, mode := range o.Modes {
		switch mode {
		case authzmodes.ModeWebhook:
			authorizers = append(authorizers, authzconfig.AuthorizerConfiguration{
				Type: authzconfig.TypeWebhook,
				Name: defaultWebhookName,
				Webhook: &authzconfig.WebhookConfiguration{
					AuthorizedTTL:             metav1.Duration{Duration: o.WebhookCacheAuthorizedTTL},
					CacheAuthorizedRequests:   o.WebhookCacheAuthorizedTTL != 0,
					UnauthorizedTTL:           metav1.Duration{Duration: o.WebhookCacheUnauthorizedTTL},
					CacheUnauthorizedRequests: o.WebhookCacheUnauthorizedTTL != 0,
					// Timeout and FailurePolicy are required for the new configuration.
					// Setting these two implicitly to preserve backward compatibility.
					Timeout:                    metav1.Duration{Duration: 30 * time.Second},
					FailurePolicy:              authzconfig.FailurePolicyNoOpinion,
					SubjectAccessReviewVersion: o.WebhookVersion,
					ConnectionInfo: authzconfig.WebhookConnectionInfo{
						Type:           authzconfig.AuthorizationWebhookConnectionInfoTypeKubeConfigFile,
						KubeConfigFile: &o.WebhookConfigFile,
					},
				},
			})
		default:
			authorizers = append(authorizers, authzconfig.AuthorizerConfiguration{
				Type: authzconfig.AuthorizerType(mode),
				Name: authorizer.GetNameForAuthorizerMode(mode),
			})
		}
	}

	return &authzconfig.AuthorizationConfiguration{Authorizers: authorizers}, nil
}
