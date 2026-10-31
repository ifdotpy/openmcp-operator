package mcp

import (
	"context"
	"fmt"
	"testing"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCleanupMigratedRequestRevokesLegacyIdentity(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := ensureWorkspaceAccess(ctx, f.account, f.ar, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	legacy, err := workspaceGrantObjects(f.ar)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := directWorkspaceConfig(f.r.cleanupConfig, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.r.reconcileCleanupAccess(ctx, f.account, cfg, f.ar, f.r.providers[0], testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	finishCleanupService(t, f)
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	if err := f.r.cleanupWorkspace(ctx, "tenant", f.account); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{controllerName, credentialIssuerName} {
		if err := f.account.Get(ctx, client.ObjectKey{Namespace: workspaceAccessNamespace(f.ar), Name: name}, &corev1.ServiceAccount{}); !apierrors.IsNotFound(err) {
			t.Fatalf("migrated request retained legacy ServiceAccount %s: %v", name, err)
		}
	}
	for _, obj := range append(legacy, &corev1.Secret{}, &corev1.Namespace{}) {
		key := client.ObjectKeyFromObject(obj)
		switch obj.(type) {
		case *corev1.Secret:
			key = client.ObjectKey{Namespace: workspaceAccessNamespace(f.ar), Name: workspaceIssuerSecretName(f.ar)}
		case *corev1.Namespace:
			key = client.ObjectKey{Name: workspaceAccessNamespace(f.ar)}
		}
		if err := f.account.Get(ctx, key, obj); !apierrors.IsNotFound(err) {
			t.Fatalf("migrated request retained legacy object %T %s: %v", obj, key, err)
		}
	}
	assertCleanupIdentityRevoked(t, f)
	if err := f.account.Get(ctx, client.ObjectKey{Name: testBindingOwner().Name}, &kcpapisv1alpha1.APIBinding{}); err != nil {
		t.Fatalf("migration test lost the active APIBinding: %v", err)
	}
}

func TestCleanupUncheckedMigrationFinishesDuringTeardown(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := ensureWorkspaceAccess(ctx, f.account, f.ar, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), f.record); err != nil {
		t.Fatal(err)
	}
	record, err := readCleanupRecord(f.record)
	if err != nil {
		t.Fatal(err)
	}
	// This record was written before migration state existed.
	record.LegacyState = ""
	if err := f.r.saveCleanupRecord(ctx, f.record, record); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	if brokerOnly, err := f.r.cleanupRequestsUseBroker(ctx, f.ar.Namespace); err != nil || brokerOnly {
		t.Fatalf("unchecked migration counted as broker-only: %t, %v", brokerOnly, err)
	}
	if err := f.r.cleanupWorkspace(ctx, "tenant", f.account); err != nil {
		t.Fatal(err)
	}
	if err := f.account.Get(ctx, client.ObjectKey{Namespace: workspaceAccessNamespace(f.ar), Name: controllerName}, &corev1.ServiceAccount{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unchecked migration retained the old ServiceAccount: %v", err)
	}
	assertCleanupIdentityRevoked(t, f)
}

func TestCleanupMigrationDenialRetainsPendingWork(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := ensureWorkspaceAccess(ctx, f.account, f.ar, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	cfg, err := directWorkspaceConfig(f.r.cleanupConfig, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	denied := interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if _, ok := obj.(*corev1.Secret); ok {
			return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, obj.GetName(), fmt.Errorf("legacy issuer deletion denied"))
		}
		return c.Delete(ctx, obj, opts...)
	}})
	if err := f.r.reconcileCleanupAccess(ctx, denied, cfg, f.ar, f.r.providers[0], testBindingOwner()); !apierrors.IsForbidden(err) {
		t.Fatalf("legacy deletion denial was lost: %v", err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), f.record); err != nil {
		t.Fatal(err)
	}
	record, err := readCleanupRecord(f.record)
	if err != nil || record.LegacyState != cleanupLegacyPending {
		t.Fatalf("migration did not retain legacy-pending: %v", err)
	}
	finishCleanupService(t, f)
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	if brokerOnly, err := f.r.cleanupRequestsUseBroker(ctx, f.ar.Namespace); err != nil || brokerOnly {
		t.Fatalf("pending legacy migration counted as broker-only: %t, %v", brokerOnly, err)
	}
	if err := f.r.cleanupWorkspace(ctx, "tenant", denied); !apierrors.IsForbidden(err) {
		t.Fatalf("legacy denial did not block runtime teardown: %v", err)
	}
	if err := f.r.reconcileCleanupRecords(ctx); err == nil {
		t.Fatal("independent broker completed unresolved legacy migration")
	}
	for _, obj := range []client.Object{&corev1.Secret{}, &corev1.ServiceAccount{}} {
		if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), obj); err != nil {
			t.Fatalf("pending migration discarded external access (%T): %v", obj, err)
		}
	}
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil || len(f.ar.Finalizers) == 0 {
		t.Fatalf("pending migration released the request finalizer: %v", err)
	}
	fresh, _, _ := testRuntime(t)
	fresh.platform, fresh.providers = f.r.platform, f.r.providers
	fresh.cleanupClient, fresh.cleanupConfig, fresh.cleanupCluster = f.issuer, f.r.cleanupConfig, "provider"
	fresh.cleanupNewClient = f.r.cleanupNewClient
	if err := fresh.cleanupWorkspace(ctx, "tenant", f.account); err != nil {
		t.Fatalf("restart with restored account access could not finish migration: %v", err)
	}
	assertCleanupIdentityRevoked(t, f)
	if err := f.account.Get(ctx, client.ObjectKey{Namespace: workspaceAccessNamespace(f.ar), Name: workspaceIssuerSecretName(f.ar)}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("migration retry retained the issuer Secret: %v", err)
	}
}

func TestCleanupMigrationDoesNotDeleteReplacementAccountIdentity(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	finishCleanupService(t, f)
	if err := ensureWorkspaceAccess(ctx, f.account, f.ar, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), f.record); err != nil {
		t.Fatal(err)
	}
	record, err := readCleanupRecord(f.record)
	if err != nil {
		t.Fatal(err)
	}
	record.LegacyState = cleanupLegacyPending
	if err := f.r.saveCleanupRecord(ctx, f.record, record); err != nil {
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
	if err := f.r.cleanupWorkspace(ctx, "tenant", f.account); err == nil {
		t.Fatal("old migration completed against a replacement account")
	}
	if err := f.account.Get(ctx, client.ObjectKey{Namespace: workspaceAccessNamespace(f.ar), Name: controllerName}, &corev1.ServiceAccount{}); err != nil {
		t.Fatalf("old migration removed the replacement account identity: %v", err)
	}
}
