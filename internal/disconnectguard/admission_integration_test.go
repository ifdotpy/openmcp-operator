package disconnectguard

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	admission "k8s.io/api/admissionregistration/v1"
	core "k8s.io/api/core/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// This checks normal Kubernetes admission locally, not KCP support.
func TestLocalAPIServerAdmission(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("local API server assets not supplied")
	}
	crd := func(group, kind, plural string, scope apiextensions.ResourceScope) *apiextensions.CustomResourceDefinition {
		preserve := true
		return &apiextensions.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: plural + "." + group}, Spec: apiextensions.CustomResourceDefinitionSpec{Group: group, Scope: scope, Names: apiextensions.CustomResourceDefinitionNames{Kind: kind, Plural: plural}, Versions: []apiextensions.CustomResourceDefinitionVersion{{Name: "v1alpha1", Served: true, Storage: true, Schema: &apiextensions.CustomResourceValidation{OpenAPIV3Schema: &apiextensions.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserve}}}}}}
	}
	environment := &envtest.Environment{CRDs: []*apiextensions.CustomResourceDefinition{crd("apis.kcp.io", "APIBinding", "apibindings", apiextensions.ClusterScoped), crd("services.test.io", "Example", "examples", apiextensions.NamespaceScoped)}}
	cfg, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	binding := &unstructured.Unstructured{}
	binding.SetGroupVersionKind(schema.GroupVersionKind{Group: "apis.kcp.io", Version: "v1alpha1", Kind: "APIBinding"})
	binding.SetName("services")
	binding.SetAnnotations(map[string]string{"kcp.io/cluster": "workspace"})
	if err := c.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, &core.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "custom"}}); err != nil {
		t.Fatal(err)
	}
	serviceGVK := schema.GroupVersionKind{Group: "services.test.io", Version: "v1alpha1", Kind: "Example"}
	order := &unstructured.Unstructured{}
	order.SetGroupVersionKind(serviceGVK)
	order.SetName("custom")
	order.SetNamespace("custom")
	if err := c.Create(ctx, order); err != nil {
		t.Fatal(err)
	}
	inspector := Inspector{Services: []schema.GroupVersionKind{serviceGVK}, Resolve: func(ctx context.Context, cluster, name string, uid types.UID) (client.Client, bool, error) {
		live := binding.DeepCopy()
		if err := c.Get(ctx, client.ObjectKey{Name: name}, live); err != nil {
			return nil, false, err
		}
		if cluster != "workspace" || live.GetUID() != uid {
			return nil, false, fmt.Errorf("wrong identity")
		}
		return c, false, nil
	}}
	server := httptest.NewTLSServer(Handler{Inspect: inspector.Check})
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	failure := admission.Fail
	effects := admission.SideEffectClassNone
	timeout := int32(10)
	webhook := &admission.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "services-test"}, Webhooks: []admission.ValidatingWebhook{{Name: "disconnect.openmcp.test", ClientConfig: admission.WebhookClientConfig{URL: &server.URL, CABundle: ca}, FailurePolicy: &failure, SideEffects: &effects, TimeoutSeconds: &timeout, AdmissionReviewVersions: []string{"v1"}, MatchConditions: []admission.MatchCondition{{Name: "protected-binding", Expression: `request.name == "services"`}}, Rules: []admission.RuleWithOperations{{Operations: []admission.OperationType{admission.Delete}, Rule: admission.Rule{APIGroups: []string{"apis.kcp.io"}, APIVersions: []string{"*"}, Resources: []string{"apibindings"}}}}}}}
	if err := c.Create(ctx, webhook); err != nil {
		t.Fatal(err)
	}
	// Wait with dry-run so admission propagation cannot delete the fixture.
	for {
		err = c.Delete(ctx, binding.DeepCopy(), client.DryRunAll)
		if apierrors.IsForbidden(err) {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("admission did not become active: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := c.Delete(ctx, binding.DeepCopy()); !apierrors.IsForbidden(err) {
		t.Fatalf("order did not prevent deletion: %v", err)
	}
	other := binding.DeepCopy()
	other.SetName("unrelated")
	other.SetResourceVersion("")
	other.SetUID("")
	if err := c.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, other); err != nil {
		t.Fatalf("unrelated binding affected: %v", err)
	}
	if err := c.Delete(ctx, order); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, binding); err != nil {
		t.Fatalf("empty workspace could not disconnect: %v", err)
	}
}
