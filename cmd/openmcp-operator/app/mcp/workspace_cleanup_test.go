package mcp

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

func TestWorkspaceCleanupPreservesRuntimeUntilServicesAreGone(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	name := multicluster.ClusterName("tenant")
	namespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, workspaceControlPlaneNamespace(name))
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if err := platform.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	gvk := schema.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Example"}
	service := &unstructured.Unstructured{}
	service.SetGroupVersionKind(gvk)
	service.SetName("default")
	service.SetNamespace("other-namespace")
	service.SetFinalizers([]string{"services.example.io/cleanup"})
	workspace := clientfake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(service).Build()
	if err := workspace.Delete(ctx, service); err != nil {
		t.Fatal(err)
	}
	r.providers = []workspaceProvider{{Name: "example", Resource: metav1.GroupVersionKind(gvk)}}
	if err := r.cleanupWorkspace(ctx, name, workspace); err == nil {
		t.Fatal("cleanup ignored terminating service")
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(ns), ns); err != nil {
		t.Fatal("runtime removed before service cleanup", err)
	}
	if !ns.DeletionTimestamp.IsZero() {
		t.Fatal("runtime deletion started before service cleanup")
	}
	if err := workspace.Get(ctx, client.ObjectKeyFromObject(service), service); err != nil {
		t.Fatal(err)
	}
	service.SetFinalizers(nil)
	if err := workspace.Update(ctx, service); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanupWorkspace(ctx, name, workspace); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(ns), ns); !apierrors.IsNotFound(err) {
		t.Fatalf("runtime retained after services were removed: %v", err)
	}

}
