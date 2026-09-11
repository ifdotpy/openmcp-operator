package mcp

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
)

const (
	defaultControlPlaneName      = "default"
	defaultControlPlaneNamespace = "default"
	// bootstrapMarkerSecret records that the default ControlPlane was created
	// once for a workspace, so a deliberately deleted default is not recreated.
	bootstrapMarkerSecret = "opencontrolplane-bootstrap"
)

// defaultControlPlaneBootstrapper creates one ControlPlane named "default" in
// every tenant workspace that binds the ControlPlane API. In the kcp mode the
// multicluster provider engages a workspace exactly when its APIBinding becomes
// ready, i.e. when the account enabled the service in the marketplace, so the
// engagement is the natural "enabled" hook.
//
// The default is created only once per workspace (marker Secret); deleting it
// afterwards is respected.
type defaultControlPlaneBootstrapper struct {
	log logging.Logger
}

var _ multicluster.Aware = (*defaultControlPlaneBootstrapper)(nil)

// Start satisfies manager.Runnable; all work happens per engaged cluster.
func (b *defaultControlPlaneBootstrapper) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Engage is called by the multicluster manager for every engaged workspace.
func (b *defaultControlPlaneBootstrapper) Engage(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	log := b.log.WithValues("cluster", string(name))
	go func() {
		if err := b.ensureDefault(ctx, cl.GetClient()); err != nil {
			// Never fail the engagement: the regular reconciler must keep running.
			log.Error(err, "default ControlPlane bootstrap failed")
		}
	}()
	return nil
}

func (b *defaultControlPlaneBootstrapper) ensureDefault(ctx context.Context, c client.Client) error {
	marker := &corev1.Secret{}
	err := c.Get(ctx, client.ObjectKey{Namespace: defaultControlPlaneNamespace, Name: bootstrapMarkerSecret}, marker)
	if err == nil {
		return nil // already bootstrapped once
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("reading bootstrap marker: %w", err)
	}

	cps := &corev2alpha1.ControlPlaneList{}
	if err := c.List(ctx, cps); err != nil {
		return fmt.Errorf("listing ControlPlanes: %w", err)
	}
	if len(cps.Items) == 0 {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: defaultControlPlaneNamespace}}
		if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating namespace %q: %w", defaultControlPlaneNamespace, err)
		}
		cp := &corev2alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      defaultControlPlaneName,
				Namespace: defaultControlPlaneNamespace,
				Annotations: map[string]string{
					"open-control-plane.io/created-by": "enable",
				},
			},
			Spec: corev2alpha1.ControlPlaneSpec{
				IAM: corev2alpha1.IAMConfig{
					Tokens: []corev2alpha1.TokenConfig{{
						Name: "admin",
						TokenConfig: clustersv1alpha1.TokenConfig{
							RoleRefs: []commonapi.RoleRef{{Kind: "ClusterRole", Name: "cluster-admin"}},
						},
					}},
				},
			},
		}
		if err := c.Create(ctx, cp); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating default ControlPlane: %w", err)
		}
		b.log.Info("Created default ControlPlane for newly enabled workspace", "namespace", cp.Namespace, "name", cp.Name)
	}

	marker = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: bootstrapMarkerSecret, Namespace: defaultControlPlaneNamespace},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{"bootstrapped": "true"},
	}
	if err := c.Create(ctx, marker); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating bootstrap marker: %w", err)
	}
	return nil
}
