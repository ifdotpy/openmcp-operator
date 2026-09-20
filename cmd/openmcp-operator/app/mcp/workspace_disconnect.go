package mcp

import (
	"context"
	"fmt"
	"net/url"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"

	"github.com/openmcp-project/openmcp-operator/internal/disconnectguard"
)

type workspaceDisconnectGuard struct {
	url      string
	caBundle []byte
}

func (r *workspaceRuntime) ensureDisconnectWebhook(ctx context.Context, name multicluster.ClusterName, c client.Client, binding *kcpapisv1alpha1.APIBinding) error {
	guard := r.disconnectGuard
	parsed, err := url.Parse(guard.url)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("disconnect guard requires an HTTPS URL without credentials, query, or fragment")
	}
	if binding.UID == "" {
		return fmt.Errorf("protected APIBinding has no UID")
	}
	failure, effects, timeout := admissionv1.Fail, admissionv1.SideEffectClassNone, int32(10)
	webhook := &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "openmcp-disconnect-" + string(binding.UID)}}
	_, err = controllerutil.CreateOrUpdate(ctx, c, webhook, func() error {
		webhook.Labels = map[string]string{workspaceRuntimeLabel: workspaceControlPlaneNamespace(name)}
		webhook.OwnerReferences = []metav1.OwnerReference{{APIVersion: kcpapisv1alpha1.SchemeGroupVersion.String(), Kind: "APIBinding", Name: binding.Name, UID: binding.UID}}
		webhook.Webhooks = []admissionv1.ValidatingWebhook{{
			Name:          "disconnect.openmcp.cloud",
			ClientConfig:  admissionv1.WebhookClientConfig{URL: &guard.url, CABundle: guard.caBundle},
			FailurePolicy: &failure, SideEffects: &effects, TimeoutSeconds: &timeout,
			AdmissionReviewVersions: []string{"v1"},
			MatchConditions:         []admissionv1.MatchCondition{{Name: "protected-binding", Expression: fmt.Sprintf("request.name == %q", binding.Name)}},
			Rules:                   []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Delete}, Rule: admissionv1.Rule{APIGroups: []string{"apis.kcp.io"}, APIVersions: []string{"*"}, Resources: []string{"apibindings"}}}},
		}}
		return nil
	})
	return err
}

func (r *workspaceRuntime) disconnectInspector() disconnectguard.Inspector {
	services := make([]schema.GroupVersionKind, 0, len(r.providers))
	for _, provider := range r.providers {
		services = append(services, schema.GroupVersionKind(provider.Resource))
	}
	return disconnectguard.Inspector{
		Resolve:  r.resolveDisconnectWorkspace,
		Services: services,
	}
}

func (r *workspaceRuntime) resolveDisconnectWorkspace(ctx context.Context, clusterName, bindingName string, uid types.UID) (client.Client, bool, error) {
	r.mu.Lock()
	workspace, ok := r.workspaces[multicluster.ClusterName(clusterName)]
	r.mu.Unlock()
	if !ok {
		return nil, false, fmt.Errorf("workspace is not engaged")
	}
	c := workspace
	binding := &kcpapisv1alpha1.APIBinding{}
	if err := c.Get(ctx, client.ObjectKey{Name: bindingName}, binding); err != nil {
		return nil, false, err
	}
	if binding.UID != uid || !r.matchesWorkspaceExport(binding) {
		return nil, false, fmt.Errorf("binding identity does not match")
	}
	logical := &kcpcorev1alpha1.LogicalCluster{}
	if err := c.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical); err != nil {
		return nil, false, err
	}
	return c, !logical.DeletionTimestamp.IsZero(), nil
}
