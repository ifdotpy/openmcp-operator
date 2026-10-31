package mcp

import (
	"context"
	"fmt"

	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// An old record without migration state must verify legacy revocation once.
// Later active reconciles detect an account-local issuer restored by an upgrade.
func (r *workspaceRuntime) observeCleanupLegacyAccess(ctx context.Context, workspace client.Client, secret *corev1.Secret, record *cleanupRecord) error {
	if record.LegacyState == cleanupLegacyRevoked {
		err := workspace.Get(ctx, client.ObjectKey{Name: workspaceAccessNamespace(&record.Request)}, &corev1.Namespace{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	if record.LegacyState == cleanupLegacyPending {
		return nil
	}
	record.LegacyState = cleanupLegacyPending
	return r.saveCleanupRecord(ctx, secret, record)
}

// Use the engaged account client. The permanent broker has no legacy issuer
// permissions. Persist pending work before deleting any legacy credential.
func (r *workspaceRuntime) revokeCleanupLegacyAccess(ctx context.Context, workspace client.Client, secret *corev1.Secret, record *cleanupRecord) error {
	if record.LegacyState == cleanupLegacyRevoked {
		return nil
	}
	if record.LegacyState != cleanupLegacyPending {
		record.LegacyState = cleanupLegacyPending
		if err := r.saveCleanupRecord(ctx, secret, record); err != nil {
			return err
		}
	}
	account, err := r.cleanupAccountClient(record, nil)
	if err != nil {
		return err
	}
	logical := &kcpcorev1alpha1.LogicalCluster{}
	if err := account.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical); err != nil {
		return err
	}
	if logical.UID != record.AccountUID {
		return fmt.Errorf("legacy cleanup account UID changed; retain migration state")
	}
	ar := &record.Request
	namespace := workspaceAccessNamespace(ar)
	objects := []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: controllerName}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: workspaceIssuerSecretName(ar)}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: credentialIssuerName}},
	}
	for _, obj := range objects {
		if err := deleteCleanupLegacyObject(ctx, workspace, obj, string(ar.UID)); err != nil {
			return err
		}
	}
	keep := map[string]bool{}
	for _, obj := range cleanupGrants(record, r.cleanupCluster) {
		keep[workspaceObjectID(obj)] = true
	}
	if err := pruneWorkspaceGrants(ctx, workspace, string(ar.UID), keep); err != nil {
		return err
	}
	if err := deleteCleanupLegacyObject(ctx, workspace, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, string(ar.UID)); err != nil {
		return err
	}
	record.LegacyState = cleanupLegacyRevoked
	return r.saveCleanupRecord(ctx, secret, record)
}

func deleteCleanupLegacyObject(ctx context.Context, workspace client.Client, obj client.Object, owner string) error {
	if err := workspace.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if obj.GetLabels()[workspaceAccessOwnerLabel] != owner {
		return fmt.Errorf("refusing to revoke foreign legacy object %s", workspaceObjectID(obj))
	}
	return client.IgnoreNotFound(workspace.Delete(ctx, obj))
}
