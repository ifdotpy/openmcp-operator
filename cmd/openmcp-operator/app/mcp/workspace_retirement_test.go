package mcp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
)

func TestProviderRetirementWaitsForServices(t *testing.T) {
	for _, placement := range []string{"shared", "workspace"} {
		t.Run(placement, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			r, platform, workspace := testRuntime(t)
			name := multicluster.ClusterName("tenant")
			ns, err := runtimeNamespaceForWorkspace(name)
			g.Expect(err).NotTo(HaveOccurred())
			cfg := &rest.Config{Host: "https://kcp.example/clusters/tenant"}
			gvk := schema.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Example"}
			workspace.Scheme().AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			workspace.Scheme().AddKnownTypeWithName(gvk.GroupVersion().WithKind("ExampleList"), &unstructured.UnstructuredList{})
			provider := workspaceProvider{Name: "example", ProviderName: "example", Image: "registry.example/provider:v1", Resource: metav1.GroupVersionKind(gvk)}
			if placement == "shared" {
				provider.RegistrationNamespace = "shared"
			}
			r.providers = []workspaceProvider{provider}
			g.Expect(r.reconcileServiceProviders(ctx)).To(Succeed())
			binding := &kcpapisv1alpha1.APIBinding{ObjectMeta: metav1.ObjectMeta{Name: "services", UID: "binding-1"}, Spec: kcpapisv1alpha1.APIBindingSpec{Reference: kcpapisv1alpha1.BindingReference{Export: &r.bindingExport}}}
			g.Expect(workspace.Create(ctx, binding)).To(Succeed())
			g.Expect(r.ensureWorkspaceRuntime(ctx, name, ns, cfg.Host)).To(Succeed())
			permissions := []clustersv1alpha1.PermissionsRequest{{Rules: []rbacv1.PolicyRule{{APIGroups: []string{gvk.Group}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch", "update", "patch", "delete"}}}}}
			ar := &clustersv1alpha1.AccessRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "example-onboarding", Namespace: ns, UID: "access-uid", Labels: map[string]string{apiconst.ManagedByLabel: gvk.Kind, workspaceProviderLabel: provider.Name}},
				Spec: clustersv1alpha1.AccessRequestSpec{
					ClusterRef: &commonapi.ObjectReference{Name: workspaceClusterName, Namespace: ns},
					Token:      &clustersv1alpha1.TokenConfig{Permissions: permissions},
				},
			}
			g.Expect(platform.Create(ctx, ar)).To(Succeed())
			cr := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "example-mcp", Namespace: ns, UID: "cluster-request", Labels: map[string]string{apiconst.ManagedByLabel: gvk.Kind}}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP}}
			g.Expect(platform.Create(ctx, cr)).To(Succeed())
			credential := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ar.Name + "-kubeconfig", Namespace: ns, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ar, clustersv1alpha1.GroupVersion.WithKind("AccessRequest"))}}, Data: map[string][]byte{clustersv1alpha1.SecretKeyKubeconfig: []byte("first"), clustersv1alpha1.SecretKeyExpirationTimestamp: []byte(time.Now().Add(time.Hour).UTC().Format(time.RFC3339))}}
			g.Expect(platform.Create(ctx, credential)).To(Succeed())
			service := &unstructured.Unstructured{}
			service.SetGroupVersionKind(gvk)
			service.SetName("default")
			service.SetNamespace("another-namespace")
			service.SetFinalizers([]string{"example.io/provider"})
			g.Expect(workspace.Create(ctx, service)).To(Succeed())
			g.Expect(r.reconcile(ctx, name, workspace, cfg)).To(Succeed())
			registrationKey := client.ObjectKey{Namespace: "shared", Name: workspaceControlPlaneNamespace(name)}
			if placement == "shared" {
				// A registration created before the ownership annotation was introduced.
				registration := &corev1.Secret{}
				g.Expect(platform.Get(ctx, registrationKey, registration)).To(Succeed())
				registration.Annotations = nil
				g.Expect(platform.Update(ctx, registration)).To(Succeed())
			}
			r.providers = nil
			g.Expect(r.reconcileServiceProviders(ctx)).To(Succeed())
			persisted := &providerv1alpha1.ServiceProvider{}
			if err := platform.Get(ctx, client.ObjectKey{Name: provider.ProviderName}, persisted); err != nil {
				t.Fatal("retirement descriptor lost", err)
			}
			if len(persisted.Status.Resources) != 1 {
				t.Fatal("retirement descriptor is empty")
			}
			// A fresh runtime must recover all retirement state from persisted objects.
			restarted, _, _ := testRuntime(t)
			restarted.platform = r.platform
			r = restarted
			foreign := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: ns, Labels: map[string]string{apiconst.ManagedByLabel: gvk.Kind}}, Spec: ar.Spec}
			g.Expect(platform.Create(ctx, foreign)).To(Succeed())
			for _, state := range []string{"active", "terminating"} {
				if state == "terminating" {
					g.Expect(workspace.Delete(ctx, service)).To(Succeed())
				}
				credential.Data[clustersv1alpha1.SecretKeyKubeconfig] = []byte(state)
				g.Expect(platform.Update(ctx, credential)).To(Succeed())
				if err := r.reconcile(ctx, name, workspace, cfg); err != nil {
					t.Fatal(state, err)
				}
				g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(ar), ar)).To(Succeed())
				g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(cr), cr)).To(Succeed())
				if !cr.DeletionTimestamp.IsZero() || !cr.Status.IsGranted() {
					t.Fatal("service lost its cluster request", state)
				}
				if !ar.DeletionTimestamp.IsZero() || !ar.Status.IsGranted() {
					t.Fatal("service lost access", state)
				}
				g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(foreign), foreign)).To(Succeed())
				if len(foreign.Finalizers) != 0 || foreign.Status.SecretRef != nil {
					t.Fatal("foreign request adopted")
				}
				grants := &rbacv1.ClusterRoleBindingList{}
				g.Expect(workspace.List(ctx, grants, client.MatchingLabels{workspaceAccessOwnerLabel: string(ar.UID)})).To(Succeed())
				if len(grants.Items) == 0 {
					t.Fatal("active service lost tenant grants")
				}
				if placement == "shared" {
					registration := &corev1.Secret{}
					g.Expect(platform.Get(ctx, registrationKey, registration)).To(Succeed())
					if string(registration.Data["kubeconfig"]) != state || registration.Annotations[registrationAccessRequestAnnotation] != ar.Name {
						t.Fatal("retired registration did not rotate")
					}
				} else {
					if err := platform.Get(ctx, client.ObjectKey{Namespace: ns, Name: provider.Name}, &appsv1.Deployment{}); err != nil {
						t.Fatal("active service lost deployment", err)
					}
				}
				if err := r.cleanupWorkspace(ctx, name, workspace); err == nil {
					t.Fatal("workspace cleanup ignored retired service")
				}
			}
			g.Expect(workspace.Get(ctx, client.ObjectKeyFromObject(service), service)).To(Succeed())
			service.SetFinalizers(nil)
			g.Expect(workspace.Update(ctx, service)).To(Succeed())
			ar.Finalizers = append(ar.Finalizers, "other.example/cleanup")
			g.Expect(platform.Update(ctx, ar)).To(Succeed())
			g.Expect(r.reconcile(ctx, name, workspace, cfg)).To(Succeed())
			g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(ar), ar)).To(Succeed())
			if ar.DeletionTimestamp.IsZero() || !slices.Equal(ar.Finalizers, []string{"other.example/cleanup"}) {
				t.Fatal("foreign finalizer changed", ar.Finalizers)
			}
			roleKey := client.ObjectKey{Name: providerRuntimeRBACName(ns, provider.Name), Namespace: ns}
			if err := platform.Get(ctx, roleKey, &rbacv1.RoleBinding{}); err != nil {
				t.Fatal("runtime pruned before request finalizers completed", err)
			}
			ar.Finalizers = nil
			g.Expect(platform.Update(ctx, ar)).To(Succeed())
			g.Expect(r.reconcile(ctx, name, workspace, cfg)).To(Succeed())
			for _, object := range []client.Object{ar, cr, credential, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: roleKey.Name, Namespace: ns}}, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: provider.Name, Namespace: ns}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: registrationKey.Name, Namespace: registrationKey.Namespace}}} {
				if err := platform.Get(ctx, client.ObjectKeyFromObject(object), object); !apierrors.IsNotFound(err) {
					t.Fatalf("retired artifact %T remains: %v", object, err)
				}
			}
			if err := r.reconcile(ctx, name, workspace, cfg); err != nil {
				t.Fatal("cleanup retry", err)
			}
		})
	}
}

func TestProviderRetirementRequiresReadableDescriptors(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	provider := &providerv1alpha1.ServiceProvider{ObjectMeta: metav1.ObjectMeta{Name: "old", Labels: map[string]string{workspaceProviderLabel: labelValueTrue}}}
	g.Expect(platform.Create(ctx, provider)).To(Succeed())
	if _, err := r.retainedProviderManagers(ctx, workspace); err == nil {
		t.Fatal("empty descriptor allowed cleanup")
	}
	provider.Status.Resources = []metav1.GroupVersionKind{{Group: "services.example.io", Version: "v1", Kind: "Example"}}
	g.Expect(platform.Status().Update(ctx, provider)).To(Succeed())
	denied := errors.New("list denied")
	failing := interceptor.NewClient(workspace.(client.WithWatch), interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return denied }})
	if _, err := r.retainedProviderManagers(ctx, failing); !errors.Is(err, denied) {
		t.Fatalf("lost service lookup error: %v", err)
	}
	r.providers = []workspaceProvider{{ProviderName: provider.Name}}
	if _, err := r.retainedProviderManagers(ctx, failing); err != nil {
		t.Fatal("restored provider still treated as retired", err)
	}
}

func TestSharedAccessCreationClaimsRequestAtomically(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	r.providers = []workspaceProvider{{Name: "example", RegistrationNamespace: "shared"}}
	g.Expect(r.ensureSharedProviderAccess(ctx, "tenant")).To(Succeed())
	ar := &clustersv1alpha1.AccessRequest{}
	g.Expect(platform.Get(ctx, client.ObjectKey{Namespace: "tenant", Name: "example-onboarding"}, ar)).To(Succeed())
	g.Expect(ar.Finalizers).To(ContainElement(workspaceAccessFinalizer))
}

func TestRecoverUnclaimedSharedAccessAfterRestart(t *testing.T) {
	for _, retain := range []bool{false, true} {
		t.Run(fmt.Sprint(retain), func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			r, platform, workspace := testRuntime(t)
			r.providers = []workspaceProvider{{Name: "example", RegistrationNamespace: "shared", Resource: metav1.GroupVersionKind{Group: "example.io", Kind: "Example"}}}
			g.Expect(r.ensureSharedProviderAccess(ctx, "tenant")).To(Succeed())
			ar := &clustersv1alpha1.AccessRequest{}
			g.Expect(platform.Get(ctx, client.ObjectKey{Namespace: "tenant", Name: "example-onboarding"}, ar)).To(Succeed())
			// Simulate the creation window in an older operator version.
			ar.UID = "access-uid"
			ar.Finalizers = []string{"other.example/cleanup"}
			g.Expect(platform.Update(ctx, ar)).To(Succeed())
			credential := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ar.Name + "-kubeconfig", Namespace: ar.Namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ar, clustersv1alpha1.GroupVersion.WithKind("AccessRequest"))}}, Data: map[string][]byte{clustersv1alpha1.SecretKeyKubeconfig: []byte("credential"), clustersv1alpha1.SecretKeyExpirationTimestamp: []byte(time.Now().Add(time.Hour).UTC().Format(time.RFC3339))}}
			g.Expect(platform.Create(ctx, credential)).To(Succeed())
			foreign := ar.DeepCopy()
			foreign.Name, foreign.UID, foreign.ResourceVersion = "foreign", "foreign-uid", ""
			foreign.Finalizers = nil
			g.Expect(platform.Create(ctx, foreign)).To(Succeed())
			r.providers = nil
			retained := map[string]bool{"Example": retain}
			g.Expect(r.reconcileAccessRequests(ctx, "tenant", workspace, &rest.Config{Host: "https://kcp.example/clusters/tenant"}, testBindingOwner(), retained)).To(Succeed())
			g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(ar), ar)).To(Succeed())
			if retain {
				g.Expect(ar.DeletionTimestamp.IsZero()).To(BeTrue())
				g.Expect(ar.Status.IsGranted()).To(BeTrue())
				g.Expect(ar.Finalizers).To(ContainElement(workspaceAccessFinalizer))
				g.Expect(r.reconcileAccessRequests(ctx, "tenant", workspace, &rest.Config{}, testBindingOwner(), nil)).To(Succeed())
				g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(ar), ar)).To(Succeed())
			}
			g.Expect(ar.DeletionTimestamp.IsZero()).To(BeFalse())
			g.Expect(ar.Finalizers).To(Equal([]string{"other.example/cleanup"}))
			pending, err := r.hasRetiredRequests(ctx, "tenant")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pending).To(BeTrue())
			g.Expect(apierrors.IsNotFound(platform.Get(ctx, client.ObjectKeyFromObject(credential), credential))).To(BeTrue())
			g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(foreign), foreign)).To(Succeed())
			g.Expect(foreign.Finalizers).To(BeEmpty())
			g.Expect(foreign.DeletionTimestamp.IsZero()).To(BeTrue())
			ar.Finalizers = nil
			g.Expect(platform.Update(ctx, ar)).To(Succeed())
			pending, err = r.hasRetiredRequests(ctx, "tenant")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pending).To(BeFalse())
		})
	}
}

func TestRetiredServiceCanFinishRequestDeletion(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	provider := workspaceProvider{ProviderName: "example", Resource: metav1.GroupVersionKind{Group: "example.io", Version: "v1", Kind: "Example"}}
	r.providers = []workspaceProvider{provider}
	g.Expect(r.reconcileServiceProviders(ctx)).To(Succeed())
	r.providers = nil
	name := multicluster.ClusterName("tenant")
	namespace, err := runtimeNamespaceForWorkspace(name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(r.ensureWorkspaceRuntime(ctx, name, namespace, "https://kcp.example/clusters/tenant")).To(Succeed())
	service := &unstructured.Unstructured{}
	service.SetGroupVersionKind(schema.GroupVersionKind(provider.Resource))
	service.SetName("default")
	service.SetNamespace("custom")
	service.SetFinalizers([]string{"example.io/cleanup"})
	g.Expect(workspace.Create(ctx, service)).To(Succeed())
	g.Expect(workspace.Delete(ctx, service)).To(Succeed())
	retained, err := r.retainedProviderManagers(ctx, workspace)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(retained["Example"]).To(BeTrue())
	ar := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "service-access", Namespace: namespace, UID: "access-uid", Labels: map[string]string{apiconst.ManagedByLabel: "Example"}, Finalizers: []string{workspaceAccessFinalizer}}}
	cr := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "service-cluster", Namespace: namespace, UID: "cluster-uid", Labels: ar.Labels, Finalizers: []string{workspaceRequestFinalizer}}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP}}
	g.Expect(platform.Create(ctx, ar)).To(Succeed())
	g.Expect(platform.Create(ctx, cr)).To(Succeed())
	g.Expect(r.reconcileClusterRequests(ctx, namespace, retained)).To(Succeed())
	// The provider requests access cleanup before it removes the service finalizer.
	g.Expect(platform.Delete(ctx, ar)).To(Succeed())
	g.Expect(platform.Delete(ctx, cr)).To(Succeed())
	activeAccess := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "active-access", Namespace: namespace, Labels: ar.Labels, Finalizers: []string{workspaceAccessFinalizer}}}
	activeCluster := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "active-cluster", Namespace: namespace, Labels: ar.Labels, Finalizers: []string{workspaceRequestFinalizer}}}
	g.Expect(platform.Create(ctx, activeAccess)).To(Succeed())
	g.Expect(platform.Create(ctx, activeCluster)).To(Succeed())
	g.Expect(r.cleanupWorkspace(ctx, name, workspace)).NotTo(Succeed())
	g.Expect(apierrors.IsNotFound(platform.Get(ctx, client.ObjectKeyFromObject(ar), ar))).To(BeTrue())
	g.Expect(apierrors.IsNotFound(platform.Get(ctx, client.ObjectKeyFromObject(cr), cr))).To(BeTrue())
	g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(activeAccess), activeAccess)).To(Succeed())
	g.Expect(platform.Get(ctx, client.ObjectKeyFromObject(activeCluster), activeCluster)).To(Succeed())
	g.Expect(activeAccess.DeletionTimestamp.IsZero()).To(BeTrue())
	g.Expect(activeCluster.DeletionTimestamp.IsZero()).To(BeTrue())
	g.Expect(workspace.Get(ctx, client.ObjectKeyFromObject(service), service)).To(Succeed())
	g.Expect(service.GetDeletionTimestamp().IsZero()).To(BeFalse())
	g.Expect(service.GetFinalizers()).To(Equal([]string{"example.io/cleanup"}))
	cluster := &clustersv1alpha1.Cluster{}
	g.Expect(platform.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspaceClusterName}, cluster)).To(Succeed())
	g.Expect(cluster.Finalizers).NotTo(ContainElement(cr.FinalizerForCluster()))
}
