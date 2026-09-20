package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type workspaceProvider struct {
	Args             []string                `json:"args,omitempty"`
	Name             string                  `json:"name"`
	Image            string                  `json:"image"`
	ProviderName     string                  `json:"providerName"`
	Resource         metav1.GroupVersionKind `json:"resource"`
	RoleRules        []rbacv1.PolicyRule     `json:"roleRules,omitempty"`
	ClusterRoleRules []rbacv1.PolicyRule     `json:"clusterRoleRules,omitempty"`
}

func loadWorkspaceProviders(path string) ([]workspaceProvider, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read kcp service-provider configuration: %w", err)
	}

	providers := []workspaceProvider{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&providers); err != nil {
		return nil, fmt.Errorf("decode kcp service-provider configuration: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return nil, fmt.Errorf("decode kcp service-provider configuration: %w", err)
	}

	names := map[string]struct{}{}
	providerNames := map[string]struct{}{}
	for i := range providers {
		provider := &providers[i]
		if problems := validation.IsDNS1123Subdomain(provider.Name); len(problems) > 0 {
			return nil, fmt.Errorf("service provider %d has invalid name %q: %s", i, provider.Name, problems[0])
		}
		if _, exists := names[provider.Name]; exists {
			return nil, fmt.Errorf("service provider name %q is duplicated", provider.Name)
		}
		names[provider.Name] = struct{}{}
		if provider.Image == "" {
			return nil, fmt.Errorf("service provider %q has no image", provider.Name)
		}
		if problems := validation.IsDNS1123Subdomain(provider.ProviderName); len(problems) > 0 {
			return nil, fmt.Errorf("service provider %q has invalid providerName %q: %s", provider.Name, provider.ProviderName, problems[0])
		}
		if _, exists := providerNames[provider.ProviderName]; exists {
			return nil, fmt.Errorf("service provider providerName %q is duplicated", provider.ProviderName)
		}
		providerNames[provider.ProviderName] = struct{}{}
		if provider.Resource.Group == "" || provider.Resource.Version == "" || provider.Resource.Kind == "" {
			return nil, fmt.Errorf("service provider %q has an incomplete resource GVK", provider.Name)
		}
		for field, rules := range map[string][]rbacv1.PolicyRule{
			"roleRules":        provider.RoleRules,
			"clusterRoleRules": provider.ClusterRoleRules,
		} {
			for ruleIndex, rule := range rules {
				if len(rule.APIGroups) == 0 || len(rule.Resources) == 0 || len(rule.Verbs) == 0 {
					return nil, fmt.Errorf("service provider %q %s[%d] must set apiGroups, resources, and verbs", provider.Name, field, ruleIndex)
				}
			}
		}
	}
	return providers, nil
}
