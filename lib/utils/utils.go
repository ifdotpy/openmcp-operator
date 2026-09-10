package utils

import (
	"context"
	"fmt"

	"github.com/openmcp-project/controller-utils/pkg/controller"
)

const (
	prefixOnboarding = "ob-"
	oldPrefixMCP     = "mcp-"
	prefixWorkload   = "wl-"
	prefixMCP        = "mcp--"
)

// StableRequestNamespace returns a stable namespace for ClusterRequests and AccessRequests,
// that can be created/used on the platform cluster.
// onboardingNamespace is the namespace of the reconciled resource on the onboarding cluster.
//
// Deprecated: Use StableMCPNamespace instead.
func StableRequestNamespace(onboardingNamespace string) string {
	return fmt.Sprint(prefixOnboarding, onboardingNamespace)
}

// StableRequestNameMCP returns a stable name for MCP requests that can be used in a request namespace on the platform cluster.
// onboardingName is the name of the reconciled resource on the onboarding cluster.
// controllerName is the name of the controller that is reconciling the resource.
//
// Deprecated: Since every MCP now has its own namespace on the platform cluster, producing unique names this way is not required anymore for namespace-scoped resources. Use StableMCPIdentifier for cluster-scoped resources.
func StableRequestNameMCP(onboardingName, controllerName string) string {
	return fmt.Sprint(oldPrefixMCP, controller.K8sNameHash(onboardingName, controllerName))
}

// StableRequestNameWorkload returns a stable name for Workload requests that can be used in a request namespace on the platform cluster.
// onboardingName is the name of the reconciled resource on the onboarding cluster.
// controllerName is the name of the controller that is reconciling the resource.
//
// Deprecated: Since each MCP now has its own namespace, the controller can choose the request name relatively freely (just avoid conflicts with other controllers).
func StableRequestNameWorkload(onboardingName, controllerName string) string {
	return fmt.Sprint(prefixWorkload, controller.K8sNameHash(onboardingName, controllerName))
}

// StableRequestNameOnboarding returns a stable name for Onboarding requests that can be used in a request namespace on the platform cluster.
// onboardingName is the name of the reconciled resource on the onboarding cluster.
// controllerName is the name of the controller that is reconciling the resource.
//
// Deprecated: Since each MCP now has its own namespace, the controller can choose the request name relatively freely (just avoid conflicts with other controllers).
func StableRequestNameOnboarding(onboardingName, controllerName string) string {
	return fmt.Sprint(prefixOnboarding, controller.K8sNameHash(onboardingName, controllerName))
}

// StableMCPNamespace computes the namespace on the platform cluster that belongs to the given MCP.
// onboardingName and onboardingNamespace are name and namespace of the MCP resource on the onboarding cluster.
func StableMCPNamespace(onboardingName, onboardingNamespace string) (string, error) {
	res, err := controller.K8sNameUUID(onboardingNamespace, onboardingName)
	if err != nil {
		return res, fmt.Errorf("error computing MCP namespace on platform cluster: %w", err)
	}
	return prefixMCP + res, nil
}

// StableMCPIdentifier computes a string that is stable for the given MCP name and namespace and at the same time unique across all MCPs.
// This can be used to name cluster-scoped resources on the platform cluster. It is recommended to add a prefix or suffix to it.
// For namespaced resources, use the MCP-specific namespace (get the name via StableMCPNamespace).
// onboardingName and onboardingNamespace are name and namespace of the MCP resource on the onboarding cluster.
func StableMCPIdentifier(onboardingName, onboardingNamespace string) (string, error) {
	res, err := controller.K8sNameUUID(onboardingNamespace, onboardingName)
	if err != nil {
		return res, fmt.Errorf("error computing MCP identifier for platform cluster: %w", err)
	}
	return res, nil
}

// WebhookSecretName computes the name the secret containing the webhook TLS certificate is expected to have, based on the provider's name.
func WebhookSecretName(providerName string) (string, error) {
	suffix := "-webhook-tls"
	base, err := controller.ShortenToXCharacters(providerName, controller.K8sMaxNameLength-len(suffix))
	if err != nil {
		return "", fmt.Errorf("error computing webhook secret name: %w", err)
	}
	return base + suffix, nil
}

// tenantClusterCtxKey carries the logical (tenant) cluster name through the
// context in the multicluster deployment mode.
type tenantClusterCtxKey struct{}

// WithTenantCluster returns a context carrying the tenant cluster name.
func WithTenantCluster(ctx context.Context, cluster string) context.Context {
	return context.WithValue(ctx, tenantClusterCtxKey{}, cluster)
}

// TenantClusterFrom returns the tenant cluster name from the context, or "".
func TenantClusterFrom(ctx context.Context) string {
	if v, ok := ctx.Value(tenantClusterCtxKey{}).(string); ok {
		return v
	}
	return ""
}

// StableMCPNamespaceCtx is StableMCPNamespace with the tenant cluster from the
// context folded into the namespace input. In the classic single-onboarding
// mode (no tenant cluster in the context) it is identical to
// StableMCPNamespace, so existing namespaces stay stable.
func StableMCPNamespaceCtx(ctx context.Context, onboardingName, onboardingNamespace string) (string, error) {
	if c := TenantClusterFrom(ctx); c != "" {
		onboardingNamespace = c + "_" + onboardingNamespace
	}
	return StableMCPNamespace(onboardingName, onboardingNamespace)
}
