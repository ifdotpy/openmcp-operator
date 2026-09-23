package mcp

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
)

const sharedRegistrationLabel = "openmcp.cloud/onboarding-kubeconfig"

func (r *workspaceRuntime) ensureSharedProviderAccess(ctx context.Context, namespace string) error {
	for _, provider := range r.providers {
		if provider.RegistrationNamespace == "" {
			continue
		}
		ar := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: provider.Name + "-onboarding", Namespace: namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.platform.Client(), ar, func() error {
			if ar.Labels == nil {
				ar.Labels = map[string]string{}
			}
			if owner := ar.Labels[workspaceProviderLabel]; ar.ResourceVersion != "" && owner != provider.Name {
				return fmt.Errorf("refusing to adopt access request %s", ar.Name)
			}
			if ar.DeletionTimestamp.IsZero() {
				controllerutil.AddFinalizer(ar, workspaceAccessFinalizer)
			}
			ar.Labels[workspaceProviderLabel] = provider.Name
			ar.Labels[apiconst.ManagedByLabel] = provider.Resource.Kind
			ar.Spec.ClusterRef = &commonapi.ObjectReference{Name: workspaceClusterName, Namespace: namespace}
			ar.Spec.Token = &clustersv1alpha1.TokenConfig{Permissions: []clustersv1alpha1.PermissionsRequest{{Rules: []rbacv1.PolicyRule{{APIGroups: []string{provider.Resource.Group}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}}}}}}
			return nil
		})
		if err != nil {
			return fmt.Errorf("ensure shared provider access: %w", err)
		}
	}
	return nil
}

func (r *workspaceRuntime) publishSharedProviderRegistrations(ctx context.Context, namespace, onboardingNamespace string, preserveRetired bool) error {
	c := r.platform.Client()
	owner := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, owner); err != nil {
		return err
	}
	for _, provider := range r.providers {
		if provider.RegistrationNamespace == "" {
			continue
		}
		ar := &clustersv1alpha1.AccessRequest{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: provider.Name + "-onboarding"}, ar); err != nil {
			return err
		}
		if !ar.Status.IsGranted() || ar.Status.SecretRef == nil {
			continue
		}
		source := &corev1.Secret{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ar.Status.SecretRef.Name}, source); err != nil {
			return err
		}
		if len(source.Data[clustersv1alpha1.SecretKeyKubeconfig]) == 0 {
			return fmt.Errorf("onboarding credential has no kubeconfig")
		}
		target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: provider.RegistrationNamespace, Name: onboardingNamespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, c, target, func() error {
			if target.ResourceVersion != "" && !metav1.IsControlledBy(target, owner) {
				return fmt.Errorf("refusing to adopt onboarding registration %s", target.Name)
			}
			if err := controllerutil.SetControllerReference(owner, target, c.Scheme()); err != nil {
				return err
			}
			target.Labels = map[string]string{sharedRegistrationLabel: "true", workspaceRuntimeLabel: namespace}
			if target.Annotations == nil {
				target.Annotations = map[string]string{}
			}
			target.Annotations[registrationAccessRequestAnnotation] = ar.Name
			target.Data = map[string][]byte{"kubeconfig": source.Data[clustersv1alpha1.SecretKeyKubeconfig]}
			return nil
		})
		if err != nil {
			return fmt.Errorf("publish onboarding registration: %w", err)
		}
	}
	if preserveRetired {
		return r.refreshRetiredRegistrations(ctx, owner, onboardingNamespace)
	}
	return r.pruneSharedProviderRegistrations(ctx, owner, onboardingNamespace)
}

// Remove only registrations owned by this runtime when provider configuration changes.
func (r *workspaceRuntime) pruneSharedProviderRegistrations(ctx context.Context, owner *corev1.Namespace, onboardingNamespace string) error {
	desired := map[client.ObjectKey]bool{}
	for _, provider := range r.providers {
		if provider.RegistrationNamespace != "" {
			desired[client.ObjectKey{Namespace: provider.RegistrationNamespace, Name: onboardingNamespace}] = true
		}
	}
	registrations := &corev1.SecretList{}
	c := r.platform.Client()
	if err := c.List(ctx, registrations, client.MatchingLabels{sharedRegistrationLabel: "true", workspaceRuntimeLabel: owner.Name}); err != nil {
		return err
	}
	for i := range registrations.Items {
		registration := &registrations.Items[i]
		if desired[client.ObjectKeyFromObject(registration)] || !metav1.IsControlledBy(registration, owner) {
			continue
		}
		if err := c.Delete(ctx, registration); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// isWorkspaceOnboardingRequest identifies the request created for a shared provider.
// The reserved label, exact name, and cluster reference survive a restart before
// older operator versions added the access finalizer.
func isWorkspaceOnboardingRequest(ar *clustersv1alpha1.AccessRequest) bool {
	provider := ar.Labels[workspaceProviderLabel]
	return provider != "" && ar.Name == provider+"-onboarding" && ar.Spec.ClusterRef != nil &&
		ar.Spec.ClusterRef.Name == workspaceClusterName && ar.Spec.ClusterRef.Namespace == ar.Namespace
}
