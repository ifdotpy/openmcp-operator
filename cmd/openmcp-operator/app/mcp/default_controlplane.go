package mcp

import (
	"context"
	"crypto/sha256"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
)

const (
	defaultControlPlaneName = "default"
	// bootstrapMarkerSecret records that the default ControlPlane was created
	// once for a workspace, so a deliberately deleted default is not recreated.
	bootstrapMarkerSecret = "openmcp-workspace-bootstrap"
)

func workspaceControlPlaneNamespace(name multicluster.ClusterName) string {
	sum := sha256.Sum256([]byte(name))
	return fmt.Sprintf("openmcp-%x", sum[:8])
}

// defaultControlPlaneBootstrapper creates one ControlPlane named "default" in
// a KCP workspace. The marker Secret preserves a deliberate deletion.
type defaultControlPlaneBootstrapper struct {
	log logging.Logger
}

func (b *defaultControlPlaneBootstrapper) ensureDefault(ctx context.Context, c client.Client, namespace string, owner metav1.OwnerReference) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, ns, func() error {
		if owner.UID != "" {
			ns.OwnerReferences = []metav1.OwnerReference{owner}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("ensuring namespace %q: %w", namespace, err)
	}
	marker := &corev1.Secret{}
	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: bootstrapMarkerSecret}, marker)
	if err == nil {
		return nil // already bootstrapped once
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("reading bootstrap marker: %w", err)
	}

	cp := &corev2alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultControlPlaneName,
			Namespace: namespace,
			Annotations: map[string]string{
				"openmcp.cloud/created-by": "kcp-api-binding",
			},
		},
		Spec: corev2alpha1.ControlPlaneSpec{
			IAM: corev2alpha1.IAMConfig{
				Tokens: []corev2alpha1.TokenConfig{{
					Name: "admin",
					TokenConfig: clustersv1alpha1.TokenConfig{
						RoleRefs: []commonapi.RoleRef{{Kind: clusterRoleKind, Name: "cluster-admin"}},
					},
				}},
			},
		},
	}
	if err := c.Create(ctx, cp); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating default ControlPlane: %w", err)
	}
	b.log.Info("Ensured default ControlPlane for enabled workspace", "namespace", cp.Namespace, "name", cp.Name)

	marker = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: bootstrapMarkerSecret, Namespace: namespace},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{"bootstrapped": labelValueTrue},
	}
	if err := c.Create(ctx, marker); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating bootstrap marker: %w", err)
	}
	return nil
}
