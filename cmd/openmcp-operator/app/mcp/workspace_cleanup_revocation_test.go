package mcp

import (
	"context"
	"fmt"
	"testing"

	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
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

func TestCleanupLiveAccountGrantDenialKeepsTokenFreeRetry(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.r.markCleanupServicesFinal(ctx, f.ar.Namespace, f.account); err != nil {
		t.Fatal(err)
	}
	original := f.account
	deny := true
	f.account = interceptor.NewClient(original.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if deny {
			return apierrors.NewForbidden(schema.GroupResource{Group: rbacv1.GroupName, Resource: "clusterrolebindings"}, obj.GetName(), fmt.Errorf("grant deletion denied in live account"))
		}
		if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.ServiceAccount{}); !apierrors.IsNotFound(err) {
			t.Fatalf("grant deletion preceded identity deletion: %v", err)
		}
		return c.Delete(ctx, obj, opts...)
	}})
	before := len(f.tokens)
	err := f.r.reconcileCleanupRecords(ctx)
	if err != nil && !apierrors.IsForbidden(err) {
		t.Fatal(err)
	}
	for _, obj := range []client.Object{&rbacv1.ClusterRole{}, &rbacv1.ClusterRoleBinding{}} {
		if err := original.Get(ctx, client.ObjectKey{Name: f.record.Name}, obj); err != nil {
			t.Fatalf("denial removed account grant (%T): %v", obj, err)
		}
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.ServiceAccount{}); !apierrors.IsNotFound(err) {
		t.Fatalf("denied grant deletion retained the external identity: %v", err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), f.record); err != nil {
		t.Fatalf("live account grant has no retry record: %v", err)
	}
	record, err := readCleanupRecord(f.record)
	if err != nil {
		t.Fatal(err)
	}
	if record.Phase != "grant-retry" || len(f.record.Data) != 1 {
		t.Fatal("grant retry record contains credentials or lost its phase")
	}
	deny = false
	fresh, _, _ := testRuntime(t)
	fresh.platform = f.r.platform
	fresh.cleanupClient, fresh.cleanupConfig, fresh.cleanupCluster = f.issuer, f.r.cleanupConfig, "provider"
	fresh.cleanupNewClient = f.r.cleanupNewClient
	if err := fresh.reconcileCleanupRecords(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.tokens) != before {
		t.Fatal("grant retry renewed the revoked identity")
	}
	assertCleanupIdentityRevoked(t, f)
}

func TestCleanupCompletedDenialReleasesAccessRequest(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	rbacLists := 0
	workspace := interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		switch list.(type) {
		case *rbacv1.RoleList, *rbacv1.RoleBindingList, *rbacv1.ClusterRoleList, *rbacv1.ClusterRoleBindingList:
			rbacLists++
			return apierrors.NewForbidden(schema.GroupResource{Group: rbacv1.GroupName, Resource: "roles"}, "", fmt.Errorf("account RBAC claims withdrawn"))
		}
		return c.List(ctx, list, opts...)
	}})
	if err := f.r.cleanupWorkspace(ctx, "tenant", workspace); err != nil {
		t.Fatalf("completed cleanup blocks request finalizer and runtime: %v", err)
	}
	if rbacLists != 0 {
		t.Fatalf("proven broker cleanup called the legacy sweep %d times", rbacLists)
	}
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); !apierrors.IsNotFound(err) {
		t.Fatalf("completed cleanup retained the AccessRequest: %v", err)
	}
	if err := f.platform.Get(ctx, client.ObjectKey{Name: f.ar.Namespace}, &corev1.Namespace{}); !apierrors.IsNotFound(err) {
		t.Fatalf("completed cleanup retained the runtime: %v", err)
	}
	assertCleanupIdentityRevoked(t, f)
}

func TestCleanupMixedRequestsKeepLegacySweepDenial(t *testing.T) {
	for _, terminating := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy-terminating-%t", terminating), func(t *testing.T) {
			f := newCleanupFixture(t)
			ctx := context.Background()
			finishCleanupService(t, f)
			legacy := f.ar.DeepCopy()
			legacy.Name, legacy.UID, legacy.ResourceVersion = "legacy", "legacy-uid", ""
			legacy.Finalizers = []string{workspaceAccessFinalizer}
			legacy.Status = clustersv1alpha1.AccessRequestStatus{}
			legacy.Spec.Token.Permissions[0].Namespace = "one-namespace"
			if err := f.platform.Create(ctx, legacy); err != nil {
				t.Fatal(err)
			}
			if terminating {
				if err := f.platform.Delete(ctx, legacy); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.platform.Delete(ctx, f.ar); err != nil {
				t.Fatal(err)
			}
			rbacLists := 0
			workspace := interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				switch list.(type) {
				case *rbacv1.RoleList, *rbacv1.RoleBindingList, *rbacv1.ClusterRoleList, *rbacv1.ClusterRoleBindingList:
					rbacLists++
					return apierrors.NewForbidden(schema.GroupResource{Group: rbacv1.GroupName, Resource: "roles"}, "", fmt.Errorf("account RBAC claims withdrawn"))
				}
				return c.List(ctx, list, opts...)
			}})
			if err := f.r.cleanupWorkspace(ctx, "tenant", workspace); !apierrors.IsForbidden(err) {
				t.Fatalf("mixed cleanup bypassed legacy denial: %v", err)
			}
			if rbacLists == 0 {
				t.Fatal("mixed cleanup skipped the legacy sweep")
			}
			if terminating {
				if err := f.platform.Get(ctx, client.ObjectKeyFromObject(legacy), legacy); err != nil || len(legacy.Finalizers) == 0 {
					t.Fatalf("legacy denial released the request: %v", err)
				}
			}
		})
	}
}

func TestCleanupBrokerCompletionSurvivesBeforeRequestFinalization(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := f.r.markCleanupServicesFinal(ctx, f.ar.Namespace, f.account); err != nil {
		t.Fatal(err)
	}
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatal(err)
	}
	assertCleanupIdentityRevoked(t, f)
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil {
		t.Fatal(err)
	}
	denied := interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
		t.Fatal("completed request called legacy account cleanup")
		return nil
	}})
	fresh, _, _ := testRuntime(t)
	fresh.platform, fresh.cleanupClient = f.r.platform, f.issuer
	if err := fresh.finalizeWorkspaceAccess(ctx, denied, f.ar); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); !apierrors.IsNotFound(err) {
		t.Fatalf("broker completion was lost before finalizer release: %v", err)
	}
}

func TestCleanupMissingCompletionKeepsLegacyFinalization(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.issuer.Delete(ctx, f.record); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil {
		t.Fatal(err)
	}
	denied := interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: rbacv1.GroupName, Resource: "roles"}, "", fmt.Errorf("legacy cleanup denied"))
	}})
	if err := f.r.finalizeWorkspaceAccess(ctx, denied, f.ar); !apierrors.IsForbidden(err) {
		t.Fatalf("missing broker completion bypassed legacy denial: %v", err)
	}
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil || len(f.ar.Finalizers) == 0 {
		t.Fatalf("unconfirmed request lost its finalizer: %v", err)
	}
}
