package mcp

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWorkspaceProviderDeploymentLifecycle(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	p := workspaceProvider{Name: "example", ProviderName: "example", Image: "registry.example/provider:v1", Resource: metav1.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Example"}, Args: []string{"--service-controller-cluster=platform"}}
	r.providers = []workspaceProvider{p}
	if err := r.ensureWorkspaceProviders(ctx, "tenant"); err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: "tenant", Name: p.Name}, deployment); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(deployment.Spec.Template.Spec.Containers[0].Args, p.Args[0]) {
		t.Fatal("provider argument missing")
	}
	if deployment.Spec.Template.Spec.ServiceAccountName == "" {
		t.Fatal("missing dedicated service account")
	}
	r.providers[0].Image = "registry.example/provider:v2"
	if err := r.ensureWorkspaceProviders(ctx, "tenant"); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(deployment), deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Template.Spec.Containers[0].Image != r.providers[0].Image {
		t.Fatal("deployment image not reconciled")
	}
	unrelated := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "tenant"}}
	if err := platform.Create(ctx, unrelated); err != nil {
		t.Fatal(err)
	}
	r.providers = nil
	if err := r.pruneConfiguredProviderRuntime(ctx, "tenant"); err != nil {
		t.Fatal(err)
	}
	for _, list := range []client.ObjectList{&appsv1.DeploymentList{}, &rbacv1.ClusterRoleList{}, &rbacv1.ClusterRoleBindingList{}} {
		if err := platform.List(ctx, list, client.MatchingLabels{workspaceRuntimeLabel: "tenant"}); err != nil {
			t.Fatal(err)
		}
		switch items := list.(type) {
		case *appsv1.DeploymentList:
			if len(items.Items) > 0 {
				t.Fatal("deployment leaked")
			}
		case *rbacv1.ClusterRoleList:
			if len(items.Items) > 0 {
				t.Fatal("cluster role leaked")
			}
		case *rbacv1.ClusterRoleBindingList:
			if len(items.Items) > 0 {
				t.Fatal("cluster role binding leaked")
			}
		}
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(unrelated), unrelated); err != nil {
		t.Fatal("unrelated service account removed", err)
	}
}

func TestWorkspaceProviderRejectsForeignRBAC(t *testing.T) {
	for _, object := range []client.Object{
		&corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{},
		&rbacv1.ClusterRole{}, &rbacv1.ClusterRoleBinding{},
	} {
		t.Run(fmt.Sprintf("%T", object), func(t *testing.T) {
			ctx := context.Background()
			r, platform, _ := testRuntime(t)
			object.SetName("provider")
			switch object.(type) {
			case *corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding:
				object.SetNamespace("tenant")
			}
			object.SetLabels(map[string]string{"owner": "someone-else"})
			if err := platform.Create(ctx, object); err != nil {
				t.Fatal(err)
			}
			before := object.DeepCopyObject()
			if err := r.ensureProviderRBAC(ctx, "tenant", "provider", workspaceProvider{Name: "example"}); err == nil {
				t.Fatal("accepted foreign RBAC object")
			}
			if err := platform.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, object) {
				t.Fatal("modified foreign RBAC object")
			}
		})
	}
}
