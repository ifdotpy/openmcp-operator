package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	admissionv1 "k8s.io/api/admissionregistration/v1"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	controllerclusters "github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, rbacv1.AddToScheme, admissionv1.AddToScheme,
		kcpapisv1alpha1.AddToScheme, kcpcorev1alpha1.AddToScheme,
		clustersv1alpha1.AddToScheme, corev2alpha1.AddToScheme, providerv1alpha1.AddToScheme,
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
		WithStatusSubresource(&clustersv1alpha1.Cluster{}, &clustersv1alpha1.ClusterRequest{}, &clustersv1alpha1.AccessRequest{}, &providerv1alpha1.ServiceProvider{}).
		Build()
	workspace := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev2alpha1.ControlPlane{}).Build()
	log, err := logging.New(&logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	r := &workspaceRuntime{
		log:           log,
		platform:      controllerclusters.NewTestClusterFromClient("platform", platform),
		environment:   "test",
		bindingName:   "services",
		bindingExport: kcpapisv1alpha1.ExportBindingReference{Path: "root:providers", Name: "services.example.io"},
		tokenLifetime: time.Hour,
		providers: []workspaceProvider{
			{Name: "example-a", Image: "example.test/a:v1", ProviderName: "example-a-config", Resource: metav1.GroupVersionKind{Group: "a.services.example.io", Version: "v1alpha1", Kind: "ServiceA"}, ClusterRoleRules: []rbacv1.PolicyRule{{APIGroups: []string{"a.services.example.io"}, Resources: []string{"providerconfigs"}, Verbs: []string{"get", "list", "watch"}}}},
			{Name: "example-b", Image: "example.test/b:v1", ProviderName: "example-b-config", Resource: metav1.GroupVersionKind{Group: "b.services.example.io", Version: "v1alpha1", Kind: "ServiceB"}, ClusterRoleRules: []rbacv1.PolicyRule{{APIGroups: []string{"b.services.example.io"}, Resources: []string{"providerconfigs"}, Verbs: []string{"get", "list", "watch"}}}},
		},
	}
	return r, platform, workspace
}

func TestWorkspaceRuntimeDiscoversBindingByExport(t *testing.T) {
	ctx := context.Background()
	r, _, workspace := testRuntime(t)
	binding := &kcpapisv1alpha1.APIBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "generated-binding-name", UID: types.UID("binding-1")},
		Spec: kcpapisv1alpha1.APIBindingSpec{Reference: kcpapisv1alpha1.BindingReference{Export: &kcpapisv1alpha1.ExportBindingReference{
			Path: r.bindingExport.Path,
			Name: r.bindingExport.Name,
		}}},
	}
	if err := workspace.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	got, err := r.workspaceBinding(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != binding.Name || got.UID != binding.UID {
		t.Fatalf("discovered wrong APIBinding: %#v", got)
	}
}

func testBindingOwner() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: kcpapisv1alpha1.SchemeGroupVersion.String(),
		Kind:       "APIBinding",
		Name:       "services",
		UID:        types.UID("binding-1"),
	}
}

func TestWorkspaceNamespaceIsStableAndDistinct(t *testing.T) {
	a := workspaceControlPlaneNamespace(multicluster.ClusterName("root:tenants:a"))
	b := workspaceControlPlaneNamespace(multicluster.ClusterName("root:tenants:b"))
	if a == b {
		t.Fatalf("different workspaces got the same namespace %q", a)
	}
	if got := workspaceControlPlaneNamespace(multicluster.ClusterName("root:tenants:a")); got != a {
		t.Fatalf("namespace changed: %q != %q", got, a)
	}
}

func TestDirectWorkspaceConfigUsesLogicalClusterEndpoint(t *testing.T) {
	base := &rest.Config{
		Host:        "https://kcp.example/prefix/clusters/root:providers:opencontrolplane?old=true#fragment",
		BearerToken: "operator-token",
		TLSClientConfig: rest.TLSClientConfig{
			CAData: []byte("workspace-ca"),
		},
	}
	got, err := directWorkspaceConfig(base, multicluster.ClusterName("92xa9couy27y62m5"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Host != "https://kcp.example/prefix/clusters/92xa9couy27y62m5" {
		t.Fatalf("wrong workspace endpoint: %q", got.Host)
	}
	if got.BearerToken != base.BearerToken || string(got.CAData) != string(base.CAData) {
		t.Fatal("workspace configuration did not preserve authentication")
	}
	if base.Host == got.Host {
		t.Fatal("base configuration was modified")
	}
}

func TestDirectWorkspaceConfigRejectsNonKCPHost(t *testing.T) {
	if _, err := directWorkspaceConfig(&rest.Config{Host: "https://kubernetes.example"}, "workspace"); err == nil {
		t.Fatal("non-kcp host was accepted")
	}
}

func TestWorkspaceRuntimeUsesWorkspaceAsOnlyCluster(t *testing.T) {
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	workspaceName := multicluster.ClusterName("root:tenants:demo")
	workspaceNamespace := workspaceControlPlaneNamespace(workspaceName)
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, workspace, workspaceNamespace, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{}
	if err := workspace.Get(ctx, client.ObjectKey{Name: workspaceNamespace}, ns); err != nil {
		t.Fatal(err)
	}
	if len(ns.OwnerReferences) != 1 || ns.OwnerReferences[0].UID != testBindingOwner().UID {
		t.Fatalf("control-plane namespace is not owned by the APIBinding: %#v", ns.OwnerReferences)
	}
	cp := &corev2alpha1.ControlPlane{}
	if err := workspace.Get(ctx, client.ObjectKey{Name: defaultControlPlaneName, Namespace: workspaceNamespace}, cp); err != nil {
		t.Fatal(err)
	}

	platformNamespace, err := platformNamespaceForWorkspace(workspaceName)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePlatformRuntime(ctx, workspaceName, platformNamespace, "https://kcp.example/clusters/root:tenants:demo"); err != nil {
		t.Fatal(err)
	}
	request := &clustersv1alpha1.ClusterRequest{
		ObjectMeta: metav1.ObjectMeta{Name: defaultControlPlaneName, Namespace: platformNamespace, UID: types.UID("request-1")},
		Spec:       clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_MCP},
	}
	if err := platform.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileClusterRequests(ctx, platformNamespace); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(request), request); err != nil {
		t.Fatal(err)
	}
	if !request.Status.IsGranted() || request.Status.Cluster == nil || request.Status.Cluster.Name != workspaceClusterName {
		t.Fatalf("request was not granted to the workspace Cluster: %#v", request.Status)
	}
	clusters := &clustersv1alpha1.ClusterList{}
	if err := platform.List(ctx, clusters, client.InNamespace(platformNamespace)); err != nil {
		t.Fatal(err)
	}
	if len(clusters.Items) != 1 || clusters.Items[0].Name != workspaceClusterName {
		t.Fatalf("runtime created nested clusters: %#v", clusters.Items)
	}
	deployments := &appsv1.DeploymentList{}
	if err := platform.List(ctx, deployments, client.InNamespace(platformNamespace)); err != nil {
		t.Fatal(err)
	}
	if len(deployments.Items) != 2 {
		t.Fatalf("got %d provider deployments, want 2", len(deployments.Items))
	}
	clusterRoles := &rbacv1.ClusterRoleList{}
	if err := platform.List(ctx, clusterRoles, client.MatchingLabels{workspaceRuntimeLabel: platformNamespace}); err != nil {
		t.Fatal(err)
	}
	if len(clusterRoles.Items) != 1 || !allows(clusterRoles.Items[0].Rules, "", "namespaces", "get") || !allows(clusterRoles.Items[0].Rules, "a.services.example.io", "providerconfigs", "list") {
		t.Fatalf("provider cannot inspect its platform namespace: %#v", clusterRoles.Items)
	}
	for name, expected := range map[string]struct{ kind, image string }{
		"example-a-config": {kind: "ServiceA", image: "example.test/a:v1"},
		"example-b-config": {kind: "ServiceB", image: "example.test/b:v1"},
	} {
		sp := &providerv1alpha1.ServiceProvider{}
		if err := platform.Get(ctx, client.ObjectKey{Name: name}, sp); err != nil {
			t.Fatal(err)
		}
		if sp.Spec.Image != expected.image {
			t.Fatalf("service provider %s has image %q, want %q", name, sp.Spec.Image, expected.image)
		}
		if sp.Annotations[apiconst.OperationAnnotation] != apiconst.OperationAnnotationValueIgnore {
			t.Fatalf("service provider %s is not excluded from platform-wide installation", name)
		}
		if sp.Status.ObservedGeneration != sp.Generation {
			t.Fatalf("service provider %s observed generation %d, want %d", name, sp.Status.ObservedGeneration, sp.Generation)
		}
		if len(sp.Status.Resources) != 1 || sp.Status.Resources[0].Kind != expected.kind {
			t.Fatalf("service provider %s has wrong resources: %#v", name, sp.Status.Resources)
		}
	}
}

func TestWorkspaceRuntimeWithoutProvidersCreatesNoProviderResources(t *testing.T) {
	ctx := context.Background()
	r, platform, _ := testRuntime(t)
	r.providers = nil
	if err := r.ensurePlatformRuntime(ctx, "root:tenants:plain", "openmcp-plain", "https://kcp.example/clusters/plain"); err != nil {
		t.Fatal(err)
	}
	deployments := &appsv1.DeploymentList{}
	if err := platform.List(ctx, deployments, client.InNamespace("openmcp-plain")); err != nil {
		t.Fatal(err)
	}
	if len(deployments.Items) != 0 {
		t.Fatalf("got %d provider deployments, want 0", len(deployments.Items))
	}
	serviceAccount := &corev1.ServiceAccount{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: "openmcp-plain", Name: providerServiceAccount}, serviceAccount); !apierrors.IsNotFound(err) {
		t.Fatalf("provider ServiceAccount exists without providers: %v", err)
	}
}

func allows(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	for _, rule := range rules {
		if contains(rule.APIGroups, group) && contains(rule.Resources, resource) && contains(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted || value == "*" {
			return true
		}
	}
	return false
}

func TestWorkspaceRuntimeIssuesScopedCredential(t *testing.T) {
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	workspaceName := multicluster.ClusterName("root:tenants:demo")
	platformNamespace, err := platformNamespaceForWorkspace(workspaceName)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePlatformRuntime(ctx, workspaceName, platformNamespace, "https://kcp.example/clusters/root:tenants:demo"); err != nil {
		t.Fatal(err)
	}
	request := &clustersv1alpha1.ClusterRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "onboarding-run", Namespace: platformNamespace, UID: types.UID("request-2")},
		Spec:       clustersv1alpha1.ClusterRequestSpec{Purpose: clustersv1alpha1.PURPOSE_ONBOARDING},
	}
	if err := platform.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileClusterRequests(ctx, platformNamespace); err != nil {
		t.Fatal(err)
	}
	access := &clustersv1alpha1.AccessRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "onboarding-run", Namespace: platformNamespace, UID: types.UID("access-1")},
		Spec: clustersv1alpha1.AccessRequestSpec{
			RequestRef: &commonapi.ObjectReference{Name: request.Name, Namespace: request.Namespace},
			Token:      &clustersv1alpha1.TokenConfig{RoleRefs: []commonapi.RoleRef{{Kind: "ClusterRole", Name: "cluster-admin"}}},
		},
	}
	if err := platform.Create(ctx, access); err != nil {
		t.Fatal(err)
	}
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("create", "serviceaccounts", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		return true, &authv1.TokenRequest{Status: authv1.TokenRequestStatus{Token: "workspace-token", ExpirationTimestamp: metav1.NewTime(time.Now().Add(time.Hour))}}, nil
	})
	configureTestWorkspaceIssuer(t, r, workspace, access, clientset)
	config := &rest.Config{Host: "https://kcp.example/clusters/root:tenants:demo", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("workspace-ca")}}
	if err := r.reconcileAccessRequests(ctx, platformNamespace, workspace, config, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	if err := platform.Get(ctx, client.ObjectKeyFromObject(access), access); err != nil {
		t.Fatal(err)
	}
	if !access.Status.IsGranted() || access.Status.SecretRef == nil {
		t.Fatalf("access not granted: %#v", access.Status)
	}
	secret := &corev1.Secret{}
	if err := platform.Get(ctx, client.ObjectKey{Name: access.Status.SecretRef.Name, Namespace: platformNamespace}, secret); err != nil {
		t.Fatal(err)
	}
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].UID != access.UID {
		t.Fatalf("credential Secret is not owned by its AccessRequest: %#v", secret.OwnerReferences)
	}
	kubeconfig := string(secret.Data[clustersv1alpha1.SecretKeyKubeconfig])
	if !containsAll(kubeconfig, "https://kcp.example/clusters/root:tenants:demo", "workspace-token") {
		t.Fatalf("credential is not scoped to the workspace endpoint: %s", kubeconfig)
	}
	grants := &rbacv1.ClusterRoleBindingList{}
	if err := workspace.List(ctx, grants, client.MatchingLabels{workspaceAccessOwnerLabel: string(access.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(grants.Items) != 1 || len(grants.Items[0].OwnerReferences) != 1 || grants.Items[0].OwnerReferences[0].UID != testBindingOwner().UID {
		t.Fatalf("workspace access is not owned by the APIBinding: %#v", grants.Items)
	}
	assertWorkspaceIssuer(t, workspace, access)
}

func assertWorkspaceIssuer(t *testing.T, workspace client.Client, access *clustersv1alpha1.AccessRequest) {
	t.Helper()
	ctx := context.Background()
	issuerRoles := &rbacv1.RoleList{}
	if err := workspace.List(ctx, issuerRoles, client.InNamespace(workspaceAccessNamespace(access)), client.MatchingLabels{workspaceAccessOwnerLabel: string(access.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(issuerRoles.Items) != 1 || !allows(issuerRoles.Items[0].Rules, "", "serviceaccounts/token", verbCreate) {
		t.Fatalf("workspace credential issuer has wrong access: %#v", issuerRoles.Items)
	}
	issuerBindings := &rbacv1.RoleBindingList{}
	if err := workspace.List(ctx, issuerBindings, client.InNamespace(workspaceAccessNamespace(access)), client.MatchingLabels{workspaceAccessOwnerLabel: string(access.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(issuerBindings.Items) != 1 || len(issuerBindings.Items[0].Subjects) != 1 || issuerBindings.Items[0].Subjects[0].Kind != serviceAccountKind || issuerBindings.Items[0].Subjects[0].Name != credentialIssuerName || issuerBindings.Items[0].Subjects[0].Namespace != workspaceAccessNamespace(access) {
		t.Fatalf("workspace credential issuer binding is wrong: %#v", issuerBindings.Items)
	}
	issuerSecret := &corev1.Secret{}
	if err := workspace.Get(ctx, client.ObjectKey{Name: workspaceIssuerSecretName(access), Namespace: workspaceAccessNamespace(access)}, issuerSecret); err != nil {
		t.Fatal(err)
	}
	if issuerSecret.Type != corev1.SecretTypeServiceAccountToken || issuerSecret.Annotations[corev1.ServiceAccountNameKey] != credentialIssuerName {
		t.Fatalf("workspace credential issuer Secret is wrong: %#v", issuerSecret)
	}
}

func configureTestWorkspaceIssuer(t *testing.T, r *workspaceRuntime, workspace client.Client, access *clustersv1alpha1.AccessRequest, clientset kubernetes.Interface) {
	t.Helper()
	r.clientsetForConfig = func(config *rest.Config) (kubernetes.Interface, error) {
		if config.BearerToken != "issuer-token" {
			t.Fatalf("workspace issuer token not used: %#v", config)
		}
		return clientset, nil
	}
	issuerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: workspaceIssuerSecretName(access), Namespace: workspaceAccessNamespace(access)},
		Data:       map[string][]byte{corev1.ServiceAccountTokenKey: []byte("issuer-token")},
	}
	if err := workspace.Create(context.Background(), issuerSecret); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceRuntimeCancelsStaleCleanupAndRemovesDisengagedRuntime(t *testing.T) {
	ctx := context.Background()
	r, platform, workspace := testRuntime(t)
	r.cleanupDelay = 20 * time.Millisecond
	workspaceName := multicluster.ClusterName("root:tenants:demo")
	workspaceNamespace := workspaceControlPlaneNamespace(workspaceName)
	platformNamespace, err := platformNamespaceForWorkspace(workspaceName)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, workspace, workspaceNamespace, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePlatformRuntime(ctx, workspaceName, platformNamespace, "https://kcp.example/clusters/root:tenants:demo"); err != nil {
		t.Fatal(err)
	}
	request := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "request", Namespace: platformNamespace, UID: types.UID("request-cleanup"), Finalizers: []string{workspaceRequestFinalizer}}}
	if err := platform.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	cluster := &clustersv1alpha1.Cluster{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: platformNamespace, Name: workspaceClusterName}, cluster); err != nil {
		t.Fatal(err)
	}
	controllerutil.AddFinalizer(cluster, request.FinalizerForCluster())
	if err := platform.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	access := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "access", Namespace: platformNamespace, UID: types.UID("access-cleanup"), Finalizers: []string{workspaceAccessFinalizer}}}
	if err := platform.Create(ctx, access); err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	r.generations = map[multicluster.ClusterName]uint64{workspaceName: 1}
	r.mu.Unlock()
	r.scheduleCleanup(workspaceName, 1, workspace, r.log)
	r.mu.Lock()
	r.generations[workspaceName] = 2
	r.mu.Unlock()
	time.Sleep(4 * r.cleanupDelay)
	if err := workspace.Get(ctx, client.ObjectKey{Name: workspaceNamespace}, &corev1.Namespace{}); err != nil {
		t.Fatalf("re-engagement did not cancel cleanup: %v", err)
	}
	if err := platform.Get(ctx, client.ObjectKey{Name: platformNamespace}, &corev1.Namespace{}); err != nil {
		t.Fatalf("re-engagement removed the platform runtime: %v", err)
	}

	r.scheduleCleanup(workspaceName, 2, workspace, r.log)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		workspaceErr := workspace.Get(ctx, client.ObjectKey{Name: workspaceNamespace}, &corev1.Namespace{})
		platformErr := platform.Get(ctx, client.ObjectKey{Name: platformNamespace}, &corev1.Namespace{})
		if apierrors.IsNotFound(workspaceErr) && apierrors.IsNotFound(platformErr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("disengaged workspace runtime was not removed")
}

func TestWorkspaceRuntimeRefusesForeignRBACAdoption(t *testing.T) {
	ctx := context.Background()
	_, _, workspace := testRuntime(t)
	if err := workspace.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "target"}}); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "target", UID: types.UID("foreign-uid")},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get"}}},
	}); err != nil {
		t.Fatal(err)
	}
	access := &clustersv1alpha1.AccessRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "access", UID: types.UID("access-2")},
		Spec: clustersv1alpha1.AccessRequestSpec{Token: &clustersv1alpha1.TokenConfig{Permissions: []clustersv1alpha1.PermissionsRequest{{
			Name: "foreign", Namespace: "target", Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}}},
		}}}},
	}
	err := ensureWorkspaceAccess(ctx, workspace, access, testBindingOwner())
	if err == nil || !strings.Contains(err.Error(), "refusing to adopt foreign") {
		t.Fatalf("foreign RBAC object was adopted: %v", err)
	}
}

func TestWorkspaceRuntimeInstallsAndEnforcesDisconnectGuard(t *testing.T) {
	ctx := context.Background()
	r, _, workspace := testRuntime(t)
	workspaceName := multicluster.ClusterName("workspace")
	binding := &kcpapisv1alpha1.APIBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "generated-binding-name", UID: types.UID("binding-1")},
		Spec: kcpapisv1alpha1.APIBindingSpec{Reference: kcpapisv1alpha1.BindingReference{Export: &kcpapisv1alpha1.ExportBindingReference{
			Path: r.bindingExport.Path,
			Name: r.bindingExport.Name,
		}}},
	}
	logical := &kcpcorev1alpha1.LogicalCluster{ObjectMeta: metav1.ObjectMeta{Name: kcpcorev1alpha1.LogicalClusterName}}
	if err := workspace.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Create(ctx, logical); err != nil {
		t.Fatal(err)
	}
	r.disconnectGuard = &workspaceDisconnectGuard{url: "https://guard.example.test/disconnect", caBundle: []byte("ca")}
	r.workspaces = map[multicluster.ClusterName]client.Client{workspaceName: workspace}
	if err := r.ensureDisconnectWebhook(ctx, workspaceName, workspace, binding); err != nil {
		t.Fatal(err)
	}
	webhook := &admissionv1.ValidatingWebhookConfiguration{}
	if err := workspace.Get(ctx, client.ObjectKey{Name: "openmcp-disconnect-binding-1"}, webhook); err != nil {
		t.Fatal(err)
	}
	if len(webhook.Webhooks) != 1 || webhook.Webhooks[0].ClientConfig.URL == nil || *webhook.Webhooks[0].ClientConfig.URL != r.disconnectGuard.url {
		t.Fatalf("wrong disconnect webhook: %#v", webhook.Webhooks)
	}
	resolved, deleting, err := r.resolveDisconnectWorkspace(ctx, string(workspaceName), binding.Name, binding.UID)
	if err != nil || deleting || resolved == nil {
		t.Fatalf("workspace resolution failed: client=%v deleting=%v err=%v", resolved, deleting, err)
	}
	if _, _, err := r.resolveDisconnectWorkspace(ctx, string(workspaceName), binding.Name, types.UID("other")); err == nil {
		t.Fatal("wrong binding UID was accepted")
	}
}

func platformNamespaceForWorkspace(name multicluster.ClusterName) (string, error) {
	return libutils.StableMCPNamespace(defaultControlPlaneName, workspaceControlPlaneNamespace(name))
}

func containsAll(value string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(value, needle) {
			return false
		}
	}
	return true
}
