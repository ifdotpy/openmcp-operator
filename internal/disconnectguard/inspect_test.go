package disconnectguard

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestInspectAllNamespacesAndWorkspaceDeletion(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "flux.services.open-control-plane.io", Version: "v1alpha1", Kind: "Flux"}
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind("FluxList"), &unstructured.UnstructuredList{})
	for _, tc := range []struct {
		name                                 string
		order, deleting, resolveError, allow bool
	}{
		{name: "empty", allow: true},
		{name: "custom namespace", order: true},
		{name: "PM account deletion", order: true, deleting: true, allow: true},
		{name: "unknown binding", resolveError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder().WithScheme(scheme)
			if tc.order {
				o := &unstructured.Unstructured{}
				o.SetGroupVersionKind(gvk)
				o.SetName("custom")
				o.SetNamespace("other")
				b.WithObjects(o)
			}
			c := b.Build()
			i := Inspector{Services: []schema.GroupVersionKind{gvk}, Resolve: func(context.Context, string, string, types.UID) (client.Client, bool, error) {
				if tc.resolveError {
					return nil, false, errors.New("unknown binding")
				}
				return c, tc.deleting, nil
			}}
			if err := i.Check(context.Background(), "account", "ocp", "uid"); (err == nil) != tc.allow {
				t.Fatalf("unexpected result %v", err)
			}
		})
	}
}
