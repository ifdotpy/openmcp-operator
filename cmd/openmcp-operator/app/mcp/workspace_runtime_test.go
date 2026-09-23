package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientgotesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"

	controllerclusters "github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, providerv1alpha1.AddToScheme, rbacv1.AddToScheme, kcpapisv1alpha1.AddToScheme,
		clustersv1alpha1.AddToScheme, corev2alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

func testRuntime(t *testing.T) (*workspaceRuntime, client.Client, client.Client) {
	t.Helper()
	scheme := testScheme(t)
	platform := clientfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&providerv1alpha1.ServiceProvider{}, &clustersv1alpha1.Cluster{}, &clustersv1alpha1.ClusterRequest{}, &clustersv1alpha1.AccessRequest{}).
		Build()
	workspace := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev2alpha1.ControlPlane{}).Build()
	log, err := logging.New(&logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &workspaceRuntime{
		log:           log,
		platform:      controllerclusters.NewTestClusterFromClient("platform", platform),
		bindingName:   "services",
		bindingExport: kcpapisv1alpha1.ExportBindingReference{Path: "root:providers", Name: "services.example.io"},
		tokenLifetime: time.Hour,
	}, platform, workspace
}

func testBindingOwner() metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: kcpapisv1alpha1.SchemeGroupVersion.String(), Kind: "APIBinding", Name: "services", UID: types.UID("binding-1")}
}

func managedLabels() map[string]string {
	return map[string]string{apiconst.ManagedByLabel: controlplane.ControllerName}
}

func TestWorkspaceRuntimeDiscoversBindingByExport(t *testing.T) {
	ctx := context.Background()
	r, _, workspace := testRuntime(t)
	binding := &kcpapisv1alpha1.APIBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "generated", UID: types.UID("binding-1")},
		Spec: kcpapisv1alpha1.APIBindingSpec{Reference: kcpapisv1alpha1.BindingReference{Export: &kcpapisv1alpha1.ExportBindingReference{
			Path: r.bindingExport.Path, Name: r.bindingExport.Name,
		}}},
	}
	if err := workspace.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	r.bindingName = ""
	got, err := r.workspaceBinding(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != binding.Name || got.UID != binding.UID {
		t.Fatalf("discovered wrong APIBinding: %#v", got)
	}
}

func TestWorkspaceNamespaceIsStableAndDistinct(t *testing.T) {
	a := workspaceControlPlaneNamespace("root:tenants:a")
	b := workspaceControlPlaneNamespace("root:tenants:b")
	if a == b || workspaceControlPlaneNamespace("root:tenants:a") != a {
		t.Fatalf("workspace namespace is not stable and distinct: %q %q", a, b)
	}
}

func TestDirectWorkspaceConfigUsesLogicalClusterEndpoint(t *testing.T) {
	base := &rest.Config{Host: "https://kcp.example/prefix/clusters/root:providers?old=true#fragment", BearerToken: "operator-token", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("ca")}}
	got, err := directWorkspaceConfig(base, multicluster.ClusterName("logical-cluster"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Host != "https://kcp.example/prefix/clusters/logical-cluster" || got.BearerToken != base.BearerToken || string(got.CAData) != "ca" {
		t.Fatalf("wrong workspace configuration: %#v", got)
	}
	if base.Host == got.Host {
		t.Fatal("base configuration was modified")
	}
	if _, err := directWorkspaceConfig(&rest.Config{Host: "https://kubernetes.example"}, "workspace"); err == nil {
		t.Fatal("non-kcp host was accepted")
	}
}

func TestWorkspaceRuntimeRegistersAPICluster(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	name := multicluster.ClusterName("root:tenants:demo")
	namespace, err := runtimeNamespaceForWorkspace(name)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "https://kcp.example/clusters/logical-cluster"
	if err := r.ensureWorkspaceRuntime(ctx, name, namespace, endpoint); err != nil {
		t.Fatal(err)
	}
	cluster := &clustersv1alpha1.Cluster{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspaceClusterName}, cluster); err != nil {
		t.Fatal(err)
	}
	if len(cluster.Spec.Purposes) != 2 || cluster.Spec.Purposes[0] != clustersv1alpha1.PURPOSE_ONBOARDING || cluster.Spec.Purposes[1] != clustersv1alpha1.PURPOSE_MCP {
		t.Fatalf("workspace cluster has unexpected purposes: %#v", cluster.Spec.Purposes)
	}
	gotEndpoint, found := cluster.Status.Endpoints.Get(clustersv1alpha1.APISERVER_ENDPOINT_EXTERNAL)
	if cluster.Status.Phase != commonapi.StatusPhaseReady || !found || gotEndpoint != endpoint {
		t.Fatalf("workspace cluster is not ready at its KCP endpoint: %#v", cluster.Status)
	}
}

func TestWorkspaceRuntimeSchedulesOnlyControlPlaneRequest(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	name := multicluster.ClusterName("root:tenants:demo")
	namespace, _ := runtimeNamespaceForWorkspace(name)
	if err := r.ensureWorkspaceRuntime(ctx, name, namespace, "https://kcp.example/clusters/demo"); err != nil {
		t.Fatal(err)
	}
	managed := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace, UID: types.UID("managed"), Labels: managedLabels()}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP}}
	foreign := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: namespace, UID: types.UID("foreign")}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP}}
	for _, request := range []*clustersv1alpha1.ClusterRequest{managed, foreign} {
		if err := platform.Create(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.reconcileClusterRequests(ctx, namespace, nil); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(managed), managed); err != nil {
		t.Fatal(err)
	}
	if !managed.Status.IsGranted() || managed.Status.Cluster == nil || managed.Status.Cluster.Name != workspaceClusterName {
		t.Fatalf("managed request was not granted to the workspace: %#v", managed.Status)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
		t.Fatal(err)
	}
	if foreign.Status.IsGranted() || len(foreign.Finalizers) != 0 {
		t.Fatalf("foreign request was changed: %#v", foreign)
	}
}

func TestWorkspaceRuntimeIssuesOnlyControlPlaneCredential(t *testing.T) {
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	name := multicluster.ClusterName("root:tenants:demo")
	namespace, _ := runtimeNamespaceForWorkspace(name)
	if err := r.ensureWorkspaceRuntime(ctx, name, namespace, "https://kcp.example/clusters/demo"); err != nil {
		t.Fatal(err)
	}
	request := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace, UID: types.UID("request"), Labels: managedLabels()}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP}}
	if err := platform.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileClusterRequests(ctx, namespace, nil); err != nil {
		t.Fatal(err)
	}
	managed := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "admin", Namespace: namespace, UID: types.UID("access"), Labels: managedLabels()}, Spec: clustersv1alpha1.AccessRequestSpec{RequestRef: &commonapi.ObjectReference{Name: request.Name, Namespace: namespace}, Token: &clustersv1alpha1.TokenConfig{RoleRefs: []commonapi.RoleRef{{Kind: clusterRoleKind, Name: "cluster-admin"}}}}}
	foreign := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: namespace, UID: types.UID("foreign-access")}, Spec: managed.Spec}
	for _, access := range []*clustersv1alpha1.AccessRequest{managed, foreign} {
		if err := platform.Create(ctx, access); err != nil {
			t.Fatal(err)
		}
	}
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("create", "serviceaccounts", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authv1.TokenRequest{Status: authv1.TokenRequestStatus{Token: "workspace-token", ExpirationTimestamp: metav1.NewTime(time.Now().Add(time.Hour))}}, nil
	})
	configureTestWorkspaceIssuer(t, r, workspace, managed, clientset)
	cfg := &rest.Config{Host: "https://kcp.example/clusters/demo", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("ca")}}
	if err := r.reconcileAccessRequests(ctx, namespace, workspace, cfg, testBindingOwner(), nil); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(managed), managed); err != nil {
		t.Fatal(err)
	}
	if !managed.Status.IsGranted() || managed.Status.SecretRef == nil {
		t.Fatalf("managed access was not granted: %#v", managed.Status)
	}
	secret := &corev1.Secret{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: namespace, Name: managed.Status.SecretRef.Name}, secret); err != nil {
		t.Fatal(err)
	}
	if value := string(secret.Data[clustersv1alpha1.SecretKeyKubeconfig]); !strings.Contains(value, cfg.Host) || !strings.Contains(value, "workspace-token") {
		t.Fatalf("credential is not scoped to the workspace: %s", value)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(foreign), foreign); err != nil {
		t.Fatal(err)
	}
	if foreign.Status.IsGranted() || len(foreign.Finalizers) != 0 {
		t.Fatalf("foreign access request was changed: %#v", foreign)
	}
	assertWorkspaceIssuer(t, workspace, managed)
}

func assertWorkspaceIssuer(t *testing.T, workspace client.Client, access *clustersv1alpha1.AccessRequest) {
	t.Helper()
	roles := &rbacv1.RoleList{}
	if err := workspace.List(context.Background(), roles, client.InNamespace(workspaceAccessNamespace(access)), client.MatchingLabels{workspaceAccessOwnerLabel: string(access.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(roles.Items) != 1 || len(roles.Items[0].Rules) != 1 {
		t.Fatalf("wrong issuer roles: %#v", roles.Items)
	}
	rule := roles.Items[0].Rules[0]
	if len(rule.ResourceNames) != 0 || !contains(rule.Resources, "serviceaccounts/token") || !contains(rule.Verbs, "create") {
		t.Fatalf("issuer token rule cannot authorize TokenRequest create: %#v", rule)
	}
}

func configureTestWorkspaceIssuer(t *testing.T, r *workspaceRuntime, workspace client.Client, access *clustersv1alpha1.AccessRequest, clientset kubernetes.Interface) {
	t.Helper()
	r.clientsetForConfig = func(config *rest.Config) (kubernetes.Interface, error) {
		if config.BearerToken != "issuer-token" {
			t.Fatalf("issuer token not used: %#v", config)
		}
		return clientset, nil
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: workspaceIssuerSecretName(access), Namespace: workspaceAccessNamespace(access)}, Data: map[string][]byte{corev1.ServiceAccountTokenKey: []byte("issuer-token")}}
	if err := workspace.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceRuntimeRefusesForeignRBACAdoption(t *testing.T) {
	ctx := context.Background()
	_, _, workspace := testRuntime(t)
	if err := workspace.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "target"}}); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Create(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "target", UID: types.UID("foreign")}}); err != nil {
		t.Fatal(err)
	}
	access := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "access", UID: types.UID("access")}, Spec: clustersv1alpha1.AccessRequestSpec{Token: &clustersv1alpha1.TokenConfig{Permissions: []clustersv1alpha1.PermissionsRequest{{Name: "foreign", Namespace: "target"}}}}}
	if err := ensureWorkspaceAccess(ctx, workspace, access, testBindingOwner()); err == nil || !strings.Contains(err.Error(), "refusing to adopt foreign") {
		t.Fatalf("foreign RBAC object was adopted: %v", err)
	}
}

func TestDefaultControlPlaneIsBootstrappedOnce(t *testing.T) {
	ctx := context.Background()
	r, _, workspace := testRuntime(t)
	namespace := workspaceControlPlaneNamespace("root:tenants:demo")
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, workspace, namespace, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	cp := &corev2alpha1.ControlPlane{}
	key := client.ObjectKey{Name: defaultControlPlaneName, Namespace: namespace}
	if err := workspace.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Delete(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.ensureDefault(ctx, workspace, namespace, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Get(ctx, key, &corev2alpha1.ControlPlane{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted default ControlPlane was recreated: %v", err)
	}
}

func runtimeNamespaceForWorkspace(name multicluster.ClusterName) (string, error) {
	return libutils.StableMCPNamespace(defaultControlPlaneName, workspaceControlPlaneNamespace(name))
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted || value == "*" {
			return true
		}
	}
	return false
}

func TestWorkspaceRuntimeLeavesWorkloadRequestsToScheduler(t *testing.T) {
	r, platform, _ := testRuntime(t)
	r.providers = []workspaceProvider{{Name: "provider", Resource: metav1.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Service"}}}
	ctx := context.Background()
	request := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "tenant", Labels: map[string]string{apiconst.ManagedByLabel: "Service"}}, Spec: clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_WORKLOAD}}
	if err := platform.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileClusterRequests(ctx, "tenant", nil); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(request), request); err != nil {
		t.Fatal(err)
	}
	if request.Status.Cluster != nil || len(request.Finalizers) != 0 {
		t.Fatalf("workspace runtime claimed workload request: %#v", request)
	}
}

func TestWorkspaceProviderRequestOwnership(t *testing.T) {
	r, _, _ := testRuntime(t)
	r.providers = []workspaceProvider{{Resource: metav1.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Service"}}}
	for manager, expected := range map[string]bool{controlplane.ControllerName: true, "Service": true, "service.services.example.io": true, "Unknown": false, "": false} {
		if got := r.ownsWorkspaceRequest(manager); got != expected {
			t.Errorf("manager %q: got %v, want %v", manager, got, expected)
		}
	}
}

func TestConsumerCredentialsUseNativeWorkspace(t *testing.T) {
	r := &workspaceRuntime{consumerBaseConfig: &rest.Config{Host: "https://kcp.example/clusters/root:providers", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("native-ca")}}}
	virtual := &rest.Config{Host: "https://virtual.example/services/clusters/tenant", BearerToken: "provider-token"}
	cfg, err := r.consumerWorkspaceConfig(multicluster.ClusterName("tenant"), virtual)
	if err != nil {
		t.Fatal(err)
	}
	issuer := workspaceIssuerConfig(cfg, "consumer-token")
	if issuer.Host != "https://kcp.example/clusters/tenant" || issuer.BearerToken != "consumer-token" || string(issuer.CAData) != "native-ca" {
		t.Fatal("consumer credential did not use native workspace configuration")
	}
	if virtual.Host != "https://virtual.example/services/clusters/tenant" || virtual.BearerToken != "provider-token" {
		t.Fatal("provider configuration was changed")
	}
}
