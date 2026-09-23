package mcp

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
)

func TestRemovedProviderRevokesAccess(t *testing.T) {
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	name := multicluster.ClusterName("tenant")
	ns, err := runtimeNamespaceForWorkspace(name)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &rest.Config{Host: "https://kcp.example/clusters/tenant"}
	r.providers = []workspaceProvider{{Name: "example", RegistrationNamespace: "shared", Resource: metav1.GroupVersionKind{Group: "example.io", Kind: "Example"}}}
	binding := &kcpapisv1alpha1.APIBinding{ObjectMeta: metav1.ObjectMeta{Name: "services", UID: "binding-1"}, Spec: kcpapisv1alpha1.APIBindingSpec{Reference: kcpapisv1alpha1.BindingReference{Export: &r.bindingExport}}}
	if err := workspace.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureWorkspaceRuntime(ctx, name, ns, cfg.Host); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureSharedProviderAccess(ctx, ns); err != nil {
		t.Fatal(err)
	}
	ar := &clustersv1alpha1.AccessRequest{}
	key := client.ObjectKey{Namespace: ns, Name: "example-onboarding"}
	if err := platform.Get(ctx, key, ar); err != nil {
		t.Fatal(err)
	}
	ar.UID = "provider-access-uid"
	if err := platform.Update(ctx, ar); err != nil {
		t.Fatal(err)
	}
	credential := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: ar.Name + "-kubeconfig", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ar, clustersv1alpha1.GroupVersion.WithKind("AccessRequest"))}}, Data: map[string][]byte{clustersv1alpha1.SecretKeyKubeconfig: []byte("test-credential"), clustersv1alpha1.SecretKeyExpirationTimestamp: []byte(time.Now().Add(time.Hour).UTC().Format(time.RFC3339))}}
	if err := platform.Create(ctx, credential); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcile(ctx, name, workspace, cfg); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, key, ar); err != nil {
		t.Fatal(err)
	}
	if !ar.Status.IsGranted() {
		t.Fatal("fixture access was not granted")
	}
	grants := &rbacv1.ClusterRoleBindingList{}
	if err := workspace.List(ctx, grants, client.MatchingLabels{workspaceAccessOwnerLabel: string(ar.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(grants.Items) == 0 {
		t.Fatal("fixture created no grants")
	}

	r.providers = nil
	if err := r.reconcile(ctx, name, workspace, cfg); err != nil {
		t.Fatal(err)
	}
	registrationKey := client.ObjectKey{Namespace: "shared", Name: workspaceControlPlaneNamespace(name)}
	if err := platform.Get(ctx, registrationKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("registration was not pruned: %v", err)
	}
	if err := platform.Get(ctx, key, ar); !apierrors.IsNotFound(err) {
		t.Fatalf("stale AccessRequest remains: %v", err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(credential), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale credential remains: %v", err)
	}
	if err := workspace.List(ctx, grants, client.MatchingLabels{workspaceAccessOwnerLabel: string(ar.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(grants.Items) != 0 {
		t.Fatal("tenant access remains after provider removal")
	}
	if err := r.reconcile(ctx, name, workspace, cfg); err != nil {
		t.Fatalf("cleanup is not idempotent: %v", err)
	}
}

func TestRemovedProviderReleasesClusterRequests(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	provider := workspaceProvider{Name: "example", ProviderName: "example", Resource: metav1.GroupVersionKind{Group: "example.io", Kind: "Example"}}
	r.providers = []workspaceProvider{provider}
	ns := "tenant"
	if err := r.ensureWorkspaceRuntime(ctx, "tenant", ns, "https://kcp.example/clusters/tenant"); err != nil {
		t.Fatal(err)
	}
	stale := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "removed", Namespace: ns, UID: "removed", Labels: map[string]string{apiconst.ManagedByLabel: provider.Resource.Kind}}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_ONBOARDING}}
	active := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: ns, UID: "active", Labels: managedLabels()}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP}}
	foreign := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: ns, UID: "foreign"}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP}}
	for _, obj := range []client.Object{stale, active, foreign} {
		if err := platform.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.reconcileClusterRequests(ctx, ns, nil); err != nil {
		t.Fatal(err)
	}
	r.providers = nil
	for i := 0; i < 2; i++ {
		if err := r.reconcileClusterRequests(ctx, ns, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(stale), stale); !apierrors.IsNotFound(err) {
		t.Fatalf("stale request remains: %v", err)
	}
	for _, obj := range []client.Object{active, foreign} {
		if err := platform.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &clustersv1alpha1.Cluster{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: ns, Name: workspaceClusterName}, cluster); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(cluster.Finalizers, stale.FinalizerForCluster()) {
		t.Fatal("removed request still holds the cluster")
	}
	if !slices.Contains(cluster.Finalizers, active.FinalizerForCluster()) {
		t.Fatal("active request lost its cluster protection")
	}
}

func TestRemovedProviderRetriesFailedRevocation(t *testing.T) {
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	ar := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "removed", Namespace: "tenant", UID: "removed", Finalizers: []string{workspaceAccessFinalizer}, Labels: map[string]string{apiconst.ManagedByLabel: "Example"}}}
	foreign := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "tenant", Labels: map[string]string{apiconst.ManagedByLabel: "Example"}}}
	for _, obj := range []client.Object{ar, foreign} {
		if err := platform.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	injected := errors.New("list denied")
	failing := interceptor.NewClient(workspace.(client.WithWatch), interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
		return injected
	}})
	if err := r.reconcileAccessRequests(ctx, "tenant", failing, &rest.Config{}, testBindingOwner(), nil); !errors.Is(err, injected) {
		t.Fatalf("lost cleanup error: %v", err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(ar), ar); err != nil {
		t.Fatal(err)
	}
	if ar.DeletionTimestamp.IsZero() || !slices.Contains(ar.Finalizers, workspaceAccessFinalizer) {
		t.Fatal("failed cleanup lost its finalizer")
	}
	if err := r.reconcileAccessRequests(ctx, "tenant", workspace, &rest.Config{}, testBindingOwner(), nil); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(ar), ar); !apierrors.IsNotFound(err) {
		t.Fatalf("retry did not finish deletion: %v", err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
		t.Fatal("foreign request changed", err)
	}
}

func TestAccessCleanupPreservesForeignCredential(t *testing.T) {
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	ar := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "removed", Namespace: "tenant", UID: "removed", Finalizers: []string{workspaceAccessFinalizer}}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ar.Name + "-kubeconfig", Namespace: ar.Namespace, UID: "foreign"}, Data: map[string][]byte{"value": []byte("untouched")}}
	for _, obj := range []client.Object{ar, foreign} {
		if err := platform.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.reconcileAccessRequests(ctx, ar.Namespace, workspace, &rest.Config{}, testBindingOwner(), nil); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
		t.Fatal(err)
	}
	if string(foreign.Data["value"]) != "untouched" {
		t.Fatal("foreign credential changed")
	}
}
