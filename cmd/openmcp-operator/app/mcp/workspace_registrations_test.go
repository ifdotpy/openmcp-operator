package mcp

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
)

func TestSharedProviderHasNoTenantDeployment(t *testing.T) {
	r, platform, _ := testRuntime(t)
	r.providers = []workspaceProvider{{Name: "example", RegistrationNamespace: "shared", Resource: metav1.GroupVersionKind{Group: "example.io", Kind: "Example"}}}
	ctx := context.Background()
	if err := r.ensureWorkspaceProviders(ctx, "tenant"); err != nil {
		t.Fatal(err)
	}
	deployments := &appsv1.DeploymentList{}
	if err := platform.List(ctx, deployments); err != nil {
		t.Fatal(err)
	}
	if len(deployments.Items) != 0 {
		t.Fatal("shared mode created tenant deployments")
	}
}

func TestSharedRegistrationRotationAndOwnership(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	r.providers = []workspaceProvider{{Name: "example", RegistrationNamespace: "shared", Resource: metav1.GroupVersionKind{Group: "example.io", Kind: "Example"}}}
	owner := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant", UID: "tenant-uid"}}
	if err := platform.Create(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureSharedProviderAccess(ctx, "tenant"); err != nil {
		t.Fatal(err)
	}
	ar := &clustersv1alpha1.AccessRequest{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: "tenant", Name: "example-onboarding"}, ar); err != nil {
		t.Fatal(err)
	}
	if got := ar.Spec.Token.Permissions[0].Rules[0].APIGroups; len(got) != 1 || got[0] != "example.io" {
		t.Fatalf("unexpected permissions: %v", got)
	}
	ar.Status.Phase = clustersv1alpha1.AccessRequestGranted
	ar.Status.SecretRef = &commonapi.LocalObjectReference{Name: "credential"}
	if err := platform.Status().Update(ctx, ar); err != nil {
		t.Fatal(err)
	}
	source := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "credential"}, Data: map[string][]byte{clustersv1alpha1.SecretKeyKubeconfig: []byte("first")}}
	if err := platform.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first", "rotated"} {
		source.Data[clustersv1alpha1.SecretKeyKubeconfig] = []byte(value)
		if err := platform.Update(ctx, source); err != nil {
			t.Fatal(err)
		}
		if err := r.publishSharedProviderRegistrations(ctx, "tenant", "onboarding", false); err != nil {
			t.Fatal(err)
		}
		target := &corev1.Secret{}
		if err := platform.Get(ctx, client.ObjectKey{Namespace: "shared", Name: "onboarding"}, target); err != nil {
			t.Fatal(err)
		}
		if string(target.Data["kubeconfig"]) != value || !metav1.IsControlledBy(target, owner) {
			t.Fatal("credential rotation or cleanup ownership missing")
		}
	}
	r.providers = nil
	if err := r.publishSharedProviderRegistrations(ctx, "tenant", "onboarding", false); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: "shared", Name: "onboarding"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale registration remains: %v", err)
	}
}

func TestSharedProviderRejectsForeignAccessRequest(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	r.providers = []workspaceProvider{{Name: "example", RegistrationNamespace: "shared"}}
	foreign := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "example-onboarding", Namespace: "tenant"}}
	if err := platform.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureSharedProviderAccess(ctx, "tenant"); err == nil {
		t.Fatal("adopted foreign access request")
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
		t.Fatal(err)
	}
	if foreign.Spec.Token != nil || foreign.Spec.ClusterRef != nil {
		t.Fatal("changed foreign permissions")
	}
}
