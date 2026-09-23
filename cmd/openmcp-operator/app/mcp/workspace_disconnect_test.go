package mcp

import (
	"context"
	"errors"
	"testing"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

func TestDisconnectAfterLastProviderRetires(t *testing.T) {
	for _, tc := range []struct {
		name                                    string
		service, terminating, unreadable, allow bool
	}{
		{name: "active", service: true},
		{name: "terminating", service: true, terminating: true},
		{name: "retired", allow: true},
		{name: "unreadable", unreadable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			r, _, workspace := testRuntime(t)
			g.Expect(kcpcorev1alpha1.AddToScheme(workspace.Scheme())).To(Succeed())
			gvk := schema.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Example"}
			workspace.Scheme().AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			workspace.Scheme().AddKnownTypeWithName(gvk.GroupVersion().WithKind("ExampleList"), &unstructured.UnstructuredList{})
			r.providers = []workspaceProvider{{ProviderName: "example", Resource: metav1.GroupVersionKind(gvk)}}
			g.Expect(r.reconcileServiceProviders(ctx)).To(Succeed())
			r.providers = nil
			g.Expect(r.reconcileServiceProviders(ctx)).To(Succeed())
			binding := &kcpapisv1alpha1.APIBinding{ObjectMeta: metav1.ObjectMeta{Name: "services", UID: "binding-1"}, Spec: kcpapisv1alpha1.APIBindingSpec{Reference: kcpapisv1alpha1.BindingReference{Export: &r.bindingExport}}}
			g.Expect(workspace.Create(ctx, binding)).To(Succeed())
			g.Expect(workspace.Create(ctx, &kcpcorev1alpha1.LogicalCluster{ObjectMeta: metav1.ObjectMeta{Name: kcpcorev1alpha1.LogicalClusterName}})).To(Succeed())
			if tc.service {
				service := &unstructured.Unstructured{}
				service.SetGroupVersionKind(gvk)
				service.SetName("service")
				service.SetNamespace("custom")
				service.SetFinalizers([]string{"example.io/cleanup"})
				g.Expect(workspace.Create(ctx, service)).To(Succeed())
				if tc.terminating {
					g.Expect(workspace.Delete(ctx, service)).To(Succeed())
				}
			}
			if tc.unreadable {
				workspace = interceptor.NewClient(workspace.(client.WithWatch), interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
					return errors.New("list denied")
				}})
			}
			r.workspaces = map[multicluster.ClusterName]client.Client{"tenant": workspace}
			// This is the same inspector captured by the HTTP handler after restart.
			inspector := r.disconnectInspector()
			g.Expect(inspector.Services).To(BeEmpty())
			err := inspector.Check(ctx, "tenant", binding.Name, binding.UID)
			if tc.allow {
				g.Expect(err).NotTo(HaveOccurred())
			} else {
				g.Expect(err).To(HaveOccurred())
			}
			g.Expect(inspector.Check(ctx, "tenant", binding.Name, "wrong-uid")).To(HaveOccurred())
		})
	}
}
