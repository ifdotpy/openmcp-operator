package mcp

import (
	"context"
	"testing"

	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func finishCleanupService(t *testing.T, f *cleanupFixture) {
	t.Helper()
	ctx := context.Background()
	if err := f.account.Delete(ctx, f.service); err != nil {
		t.Fatal(err)
	}
	scoped := f.scoped()
	old := f.service.DeepCopy()
	f.service.Object["status"] = map[string]interface{}{"phase": "Deleted"}
	if err := scoped.Status().Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
		t.Fatal(err)
	}
	old = f.service.DeepCopy()
	f.service.SetFinalizers(nil)
	if err := scoped.Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
		t.Fatal(err)
	}
}

func assertCleanupIdentityRevoked(t *testing.T, f *cleanupFixture) {
	t.Helper()
	ctx := context.Background()
	for _, obj := range []client.Object{&corev1.Secret{}, &corev1.ServiceAccount{}} {
		if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), obj); !apierrors.IsNotFound(err) {
			t.Fatalf("external cleanup state remains (%T): %v", obj, err)
		}
	}
	for _, obj := range []client.Object{&rbacv1.ClusterRole{}, &rbacv1.ClusterRoleBinding{}} {
		if err := f.account.Get(ctx, client.ObjectKey{Name: f.record.Name}, obj); !apierrors.IsNotFound(err) {
			t.Fatalf("account cleanup grant remains (%T): %v", obj, err)
		}
	}
}

func TestCleanupWorkspaceKeepsExternalGrantsUntilBrokerRevokes(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.r.cleanupWorkspace(ctx, "tenant", f.account); err != nil {
		t.Fatal(err)
	}
	if err := f.account.Get(ctx, client.ObjectKey{Name: f.record.Name}, &rbacv1.ClusterRoleBinding{}); err != nil {
		t.Fatalf("workspace cleanup revoked provider before broker completion: %v", err)
	}
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatalf("broker cannot complete after workspace cleanup: %v", err)
	}
	assertCleanupIdentityRevoked(t, f)
}

func TestCleanupReplacementRequestRevokesOldIdentity(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil {
		t.Fatal(err)
	}
	f.ar.UID = "replacement"
	if err := f.platform.Update(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatalf("broker cannot revoke old identity after request replacement: %v", err)
	}
	assertCleanupIdentityRevoked(t, f)
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil || f.ar.UID != "replacement" {
		t.Fatalf("old cleanup changed replacement request: %v", err)
	}
}

func removeCleanupAccountContent(t *testing.T, f *cleanupFixture) {
	t.Helper()
	ctx := context.Background()
	bindings := &rbacv1.ClusterRoleBindingList{}
	if err := f.account.List(ctx, bindings); err != nil {
		t.Fatal(err)
	}
	for i := range bindings.Items {
		if err := f.account.Delete(ctx, &bindings.Items[i]); err != nil {
			t.Fatal(err)
		}
	}
	roles := &rbacv1.ClusterRoleList{}
	if err := f.account.List(ctx, roles); err != nil {
		t.Fatal(err)
	}
	for i := range roles.Items {
		if err := f.account.Delete(ctx, &roles.Items[i]); err != nil {
			t.Fatal(err)
		}
	}
	logical := &kcpcorev1alpha1.LogicalCluster{}
	if err := f.account.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical); err != nil {
		t.Fatal(err)
	}
	if err := f.account.Delete(ctx, logical); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupNormalAccountDeletionRevokesExternalIdentity(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.r.cleanupWorkspace(ctx, "tenant", f.account); err != nil {
		t.Fatal(err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), f.record); err != nil {
		t.Fatal(err)
	}
	record, err := readCleanupRecord(f.record)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := record.Phase
	// KCP removes account RBAC and the LogicalCluster after content deletion.
	// Native access then returns Forbidden, even for a LogicalCluster GET.
	removeCleanupAccountContent(t, f)
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatalf("completed account deletion kept external identity after native denial: %v", err)
	}
	if checkpoint != "services-final" {
		t.Fatalf("runtime teardown did not persist service completion: %q", checkpoint)
	}
	assertCleanupIdentityRevoked(t, f)
}

func TestCleanupAccountDenialWithoutCompletionRetainsIdentity(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	// No runtime observation or checkpoint exists. Denial cannot prove completion.
	removeCleanupAccountContent(t, f)
	if err := f.r.reconcileCleanupRecords(ctx); !apierrors.IsForbidden(err) {
		t.Fatalf("expected native denial without completion marker: %v", err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.Secret{}); err != nil {
		t.Fatal("unconfirmed completion discarded durable record", err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.ServiceAccount{}); err != nil {
		t.Fatal("unconfirmed completion discarded identity", err)
	}
}

func TestCleanupRuntimeConfirmsCompletionBeforeRequestFinalization(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	removeCleanupAccountContent(t, f)
	// The runtime can observe bound service absence through its workspace client.
	// It must save completion before finalizing the pending AccessRequest.
	if err := f.r.cleanupWorkspace(ctx, "tenant", f.account); err != nil {
		t.Fatalf("runtime finalized access before saving service completion: %v", err)
	}
	assertCleanupIdentityRevoked(t, f)
}

func TestCleanupCompletedRecordDoesNotRevokeReplacementAccountGrants(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.r.cleanupWorkspace(ctx, "tenant", f.account); err != nil {
		t.Fatal(err)
	}
	logical := &kcpcorev1alpha1.LogicalCluster{}
	if err := f.account.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical); err != nil {
		t.Fatal(err)
	}
	logical.UID = "replacement-account"
	if err := f.account.Update(ctx, logical); err != nil {
		t.Fatal(err)
	}
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.account.Get(ctx, client.ObjectKey{Name: f.record.Name}, &rbacv1.ClusterRoleBinding{}); err != nil {
		t.Fatal("old record changed replacement account grants", err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("replacement account left old external record: %v", err)
	}
}
