package mcp

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
)

const registrationAccessRequestAnnotation = "openmcp.cloud/onboarding-access-request"

// retainedProviderManagers discovers retired services from persisted descriptors.
// A terminating service still needs its provider and access to finish deletion.
func (r *workspaceRuntime) retainedProviderManagers(ctx context.Context, workspace client.Client) (map[string]bool, error) {
	configured := map[string]bool{}
	for _, provider := range r.providers {
		configured[provider.ProviderName] = true
	}
	providers := &providerv1alpha1.ServiceProviderList{}
	if err := r.platform.Client().List(ctx, providers, client.MatchingLabels{workspaceProviderLabel: labelValueTrue}); err != nil {
		return nil, err
	}
	retained := map[string]bool{}
	for _, provider := range providers.Items {
		if configured[provider.Name] {
			continue
		}
		if len(provider.Status.Resources) == 0 {
			return nil, fmt.Errorf("retired provider %q has no resource descriptors; restore its configuration before cleanup", provider.Name)
		}
		for _, resource := range provider.Status.Resources {
			gvk := schema.GroupVersionKind(resource)
			if gvk.Group == "" || gvk.Version == "" || gvk.Kind == "" {
				return nil, fmt.Errorf("retired provider %q has an incomplete resource descriptor", provider.Name)
			}
			services := &unstructured.UnstructuredList{}
			services.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
			if err := workspace.List(ctx, services, client.Limit(1)); err != nil {
				return nil, fmt.Errorf("check retired %s services: %w", gvk.Kind, err)
			}
			if len(services.Items) > 0 {
				retained[gvk.Kind] = true
				retained[strings.ToLower(gvk.Kind)+"."+gvk.Group] = true
			}
		}
	}
	return retained, nil
}

// hasRetiredRequests preserves runtime artifacts while other finalizers remain.
func (r *workspaceRuntime) hasRetiredRequests(ctx context.Context, namespace string) (bool, error) {
	requests := &clustersv1alpha1.AccessRequestList{}
	if err := r.platform.Client().List(ctx, requests, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range requests.Items {
		request := &requests.Items[i]
		owned := controllerutil.ContainsFinalizer(request, workspaceAccessFinalizer) || request.Labels[clustersv1alpha1.ProviderLabel] == workspaceProviderName || isWorkspaceOnboardingRequest(request)
		if owned && !r.ownsWorkspaceRequest(request.Labels[apiconst.ManagedByLabel]) {
			return true, nil
		}
	}
	clusters := &clustersv1alpha1.ClusterRequestList{}
	if err := r.platform.Client().List(ctx, clusters, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range clusters.Items {
		request := &clusters.Items[i]
		ref := request.Status.Cluster
		owned := controllerutil.ContainsFinalizer(request, workspaceRequestFinalizer) || (ref != nil && ref.Name == workspaceClusterName && ref.Namespace == namespace)
		if owned && !r.ownsWorkspaceRequest(request.Labels[apiconst.ManagedByLabel]) {
			return true, nil
		}
	}
	return false, nil
}

func (r *workspaceRuntime) pruneConfiguredProviderRuntime(ctx context.Context, namespace string) error {
	deployments, rbac := map[string]struct{}{}, map[string]struct{}{}
	for _, provider := range r.providers {
		if provider.RegistrationNamespace == "" {
			deployments[provider.Name] = struct{}{}
		}
		rbac[providerRuntimeRBACName(namespace, provider.Name)] = struct{}{}
	}
	return r.pruneProviderRuntime(ctx, namespace, deployments, rbac)
}

// refreshRetiredRegistrations keeps existing shared providers connected during
// service deletion. It never creates a registration for an unconfigured provider.
func (r *workspaceRuntime) refreshRetiredRegistrations(ctx context.Context, owner *corev1.Namespace, onboardingNamespace string) error {
	c := r.platform.Client()
	registrations := &corev1.SecretList{}
	if err := c.List(ctx, registrations, client.MatchingLabels{sharedRegistrationLabel: labelValueTrue, workspaceRuntimeLabel: owner.Name}); err != nil {
		return err
	}
	configured := map[string]bool{}
	for _, provider := range r.providers {
		configured[provider.RegistrationNamespace] = true
	}
	for i := range registrations.Items {
		registration := &registrations.Items[i]
		if configured[registration.Namespace] || registration.Name != onboardingNamespace || !metav1.IsControlledBy(registration, owner) {
			continue
		}
		requestName, err := r.registrationAccessRequest(ctx, registration, owner.Name)
		if err != nil {
			return err
		}
		request := &clustersv1alpha1.AccessRequest{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: owner.Name, Name: requestName}, request); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !request.DeletionTimestamp.IsZero() || request.Status.SecretRef == nil {
			continue
		}
		if !controllerutil.ContainsFinalizer(request, workspaceAccessFinalizer) {
			return fmt.Errorf("registration %s has no owned AccessRequest", registration.Name)
		}
		source := &corev1.Secret{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: owner.Name, Name: request.Status.SecretRef.Name}, source); err != nil {
			return err
		}
		if !metav1.IsControlledBy(source, request) {
			return fmt.Errorf("registration credential is not owned by AccessRequest %s", request.Name)
		}
		if len(source.Data[clustersv1alpha1.SecretKeyKubeconfig]) == 0 {
			return fmt.Errorf("registration credential for %s has no kubeconfig", request.Name)
		}
		old := registration.DeepCopy()
		if registration.Annotations == nil {
			registration.Annotations = map[string]string{}
		}
		registration.Annotations[registrationAccessRequestAnnotation] = requestName
		registration.Data = map[string][]byte{"kubeconfig": source.Data[clustersv1alpha1.SecretKeyKubeconfig]}
		if err := c.Patch(ctx, registration, client.MergeFrom(old)); err != nil {
			return err
		}
	}
	return nil
}

// Legacy registrations did not record their AccessRequest. Their owned RoleBinding
// identifies the shared provider ServiceAccount and its registration namespace.
func (r *workspaceRuntime) registrationAccessRequest(ctx context.Context, registration *corev1.Secret, namespace string) (string, error) {
	if name := registration.Annotations[registrationAccessRequestAnnotation]; name != "" {
		return name, nil
	}
	bindings := &rbacv1.RoleBindingList{}
	if err := r.platform.Client().List(ctx, bindings, client.InNamespace(namespace), client.MatchingLabels{workspaceRuntimeLabel: namespace}); err != nil {
		return "", err
	}
	name := ""
	for _, binding := range bindings.Items {
		provider := binding.Labels[appNameLabel]
		if provider == "" {
			continue
		}
		for _, subject := range binding.Subjects {
			if subject.Kind != serviceAccountKind || subject.Name != provider || subject.Namespace != registration.Namespace {
				continue
			}
			candidate := provider + "-onboarding"
			if name != "" && name != candidate {
				return "", fmt.Errorf("registration %s has ambiguous provider ownership", registration.Name)
			}
			name = candidate
		}
	}
	if name == "" {
		return "", fmt.Errorf("registration %s has no provider ownership metadata; restore its configuration", registration.Name)
	}
	return name, nil
}
