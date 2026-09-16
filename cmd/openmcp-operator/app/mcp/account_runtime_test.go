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

func testRuntime(t *testing.T) (*accountRuntime, client.Client, client.Client) {
	t.Helper()
	scheme := testScheme(t)
	platform := clientfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&clustersv1alpha1.Cluster{}, &clustersv1alpha1.ClusterRequest{}, &clustersv1alpha1.AccessRequest{}, &providerv1alpha1.ServiceProvider{}).
		Build()
	account := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev2alpha1.ControlPlane{}).Build()
	log, err := logging.New(&logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	r := &accountRuntime{
		log:           log,
		platform:      controllerclusters.NewTestClusterFromClient("platform", platform),
		environment:   "test",
		bindingName:   "ocp",
		tokenLifetime: time.Hour,
		providers: []accountProvider{
			{name: "flux", image: "example.test/flux:v1", providerName: "flux-config", resource: metav1.GroupVersionKind{Group: "flux.services.open-control-plane.io", Version: "v1alpha1", Kind: "Flux"}},
			{name: "external-secrets", image: "example.test/eso:v1", providerName: "eso-config", resource: metav1.GroupVersionKind{Group: "external-secrets.services.open-control-plane.io", Version: "v1alpha1", Kind: "ExternalSecretsOperator"}},
		},
	}
	return r, platform, account
}

func testBindingOwner() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: kcpapisv1alpha1.SchemeGroupVersion.String(),
		Kind:       "APIBinding",
		Name:       "ocp",
		UID:        types.UID("binding-1"),
	}
}

func TestAccountNamespaceIsStableAndDistinct(t *testing.T) {
	a := accountControlPlaneNamespace(multicluster.ClusterName("root:orgs:a"))
	b := accountControlPlaneNamespace(multicluster.ClusterName("root:orgs:b"))
	if a == b {
		t.Fatalf("different accounts got the same namespace %q", a)
	}
	if got := accountControlPlaneNamespace(multicluster.ClusterName("root:orgs:a")); got != a {
		t.Fatalf("namespace changed: %q != %q", got, a)
	}
}

func TestAccountRuntimeUsesAccountAsOnlyCluster(t *testing.T) {
	ctx := context.Background()
	r, platform, account := testRuntime(t)
	accountName := multicluster.ClusterName("root:orgs:demo")
	accountNamespace := accountControlPlaneNamespace(accountName)
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, account, accountNamespace, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{}
	if err := account.Get(ctx, client.ObjectKey{Name: accountNamespace}, ns); err != nil {
		t.Fatal(err)
	}
	if len(ns.OwnerReferences) != 1 || ns.OwnerReferences[0].UID != testBindingOwner().UID {
		t.Fatalf("control-plane namespace is not owned by the APIBinding: %#v", ns.OwnerReferences)
	}
	cp := &corev2alpha1.ControlPlane{}
	if err := account.Get(ctx, client.ObjectKey{Name: defaultControlPlaneName, Namespace: accountNamespace}, cp); err != nil {
		t.Fatal(err)
	}

	platformNamespace, err := platformNamespaceForAccount(accountName)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePlatformRuntime(ctx, accountName, platformNamespace, "https://kcp.example/clusters/root:orgs:demo"); err != nil {
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
	if !request.Status.IsGranted() || request.Status.Cluster == nil || request.Status.Cluster.Name != accountClusterName {
		t.Fatalf("request was not granted to the account Cluster: %#v", request.Status)
	}
	clusters := &clustersv1alpha1.ClusterList{}
	if err := platform.List(ctx, clusters, client.InNamespace(platformNamespace)); err != nil {
		t.Fatal(err)
	}
	if len(clusters.Items) != 1 || clusters.Items[0].Name != accountClusterName {
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
	if err := platform.List(ctx, clusterRoles, client.MatchingLabels{accountRuntimeLabel: platformNamespace}); err != nil {
		t.Fatal(err)
	}
	if len(clusterRoles.Items) != 1 || !allows(clusterRoles.Items[0].Rules, "", "namespaces", "get") {
		t.Fatalf("provider cannot inspect its platform namespace: %#v", clusterRoles.Items)
	}
	for name, kind := range map[string]string{"flux-config": "Flux", "eso-config": "ExternalSecretsOperator"} {
		sp := &providerv1alpha1.ServiceProvider{}
		if err := platform.Get(ctx, client.ObjectKey{Name: name}, sp); err != nil {
			t.Fatal(err)
		}
		if len(sp.Status.Resources) != 1 || sp.Status.Resources[0].Kind != kind {
			t.Fatalf("service provider %s has wrong resources: %#v", name, sp.Status.Resources)
		}
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

func TestAccountRuntimeIssuesScopedCredential(t *testing.T) {
	ctx := context.Background()
	r, platform, account := testRuntime(t)
	accountName := multicluster.ClusterName("root:orgs:demo")
	platformNamespace, err := platformNamespaceForAccount(accountName)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePlatformRuntime(ctx, accountName, platformNamespace, "https://kcp.example/clusters/root:orgs:demo"); err != nil {
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
		return true, &authv1.TokenRequest{Status: authv1.TokenRequestStatus{Token: "account-token", ExpirationTimestamp: metav1.NewTime(time.Now().Add(time.Hour))}}, nil
	})
	config := &rest.Config{Host: "https://kcp.example/clusters/root:orgs:demo", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("account-ca")}}
	if err := r.reconcileAccessRequests(ctx, platformNamespace, account, clientset, config, testBindingOwner()); err != nil {
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
	if !containsAll(kubeconfig, "https://kcp.example/clusters/root:orgs:demo", "account-token") {
		t.Fatalf("credential is not scoped to the account endpoint: %s", kubeconfig)
	}
	grants := &rbacv1.ClusterRoleBindingList{}
	if err := account.List(ctx, grants, client.MatchingLabels{accountAccessOwnerLabel: string(access.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(grants.Items) != 1 || len(grants.Items[0].OwnerReferences) != 1 || grants.Items[0].OwnerReferences[0].UID != testBindingOwner().UID {
		t.Fatalf("account access is not owned by the APIBinding: %#v", grants.Items)
	}
}

func TestAccountRuntimeCancelsStaleCleanupAndRemovesDisengagedRuntime(t *testing.T) {
	ctx := context.Background()
	r, platform, account := testRuntime(t)
	r.cleanupDelay = 20 * time.Millisecond
	accountName := multicluster.ClusterName("root:orgs:demo")
	accountNamespace := accountControlPlaneNamespace(accountName)
	platformNamespace, err := platformNamespaceForAccount(accountName)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, account, accountNamespace, testBindingOwner()); err != nil {
		t.Fatal(err)
	}
	if err := r.ensurePlatformRuntime(ctx, accountName, platformNamespace, "https://kcp.example/clusters/root:orgs:demo"); err != nil {
		t.Fatal(err)
	}
	request := &clustersv1alpha1.ClusterRequest{ObjectMeta: metav1.ObjectMeta{Name: "request", Namespace: platformNamespace, UID: types.UID("request-cleanup"), Finalizers: []string{accountRequestFinalizer}}}
	if err := platform.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	cluster := &clustersv1alpha1.Cluster{}
	if err := platform.Get(ctx, client.ObjectKey{Namespace: platformNamespace, Name: accountClusterName}, cluster); err != nil {
		t.Fatal(err)
	}
	controllerutil.AddFinalizer(cluster, request.FinalizerForCluster())
	if err := platform.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	access := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "access", Namespace: platformNamespace, UID: types.UID("access-cleanup"), Finalizers: []string{accountAccessFinalizer}}}
	if err := platform.Create(ctx, access); err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	r.generations = map[multicluster.ClusterName]uint64{accountName: 1}
	r.mu.Unlock()
	r.scheduleCleanup(accountName, 1, account, r.log)
	r.mu.Lock()
	r.generations[accountName] = 2
	r.mu.Unlock()
	time.Sleep(4 * r.cleanupDelay)
	if err := account.Get(ctx, client.ObjectKey{Name: accountNamespace}, &corev1.Namespace{}); err != nil {
		t.Fatalf("re-engagement did not cancel cleanup: %v", err)
	}
	if err := platform.Get(ctx, client.ObjectKey{Name: platformNamespace}, &corev1.Namespace{}); err != nil {
		t.Fatalf("re-engagement removed the platform runtime: %v", err)
	}

	r.scheduleCleanup(accountName, 2, account, r.log)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		accountErr := account.Get(ctx, client.ObjectKey{Name: accountNamespace}, &corev1.Namespace{})
		platformErr := platform.Get(ctx, client.ObjectKey{Name: platformNamespace}, &corev1.Namespace{})
		if apierrors.IsNotFound(accountErr) && apierrors.IsNotFound(platformErr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("disengaged account runtime was not removed")
}

func TestAccountRuntimeRefusesForeignRBACAdoption(t *testing.T) {
	ctx := context.Background()
	_, _, account := testRuntime(t)
	if err := account.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "target"}}); err != nil {
		t.Fatal(err)
	}
	if err := account.Create(ctx, &rbacv1.Role{
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
	err := ensureAccountAccess(ctx, account, access, testBindingOwner())
	if err == nil || !strings.Contains(err.Error(), "refusing to adopt foreign") {
		t.Fatalf("foreign RBAC object was adopted: %v", err)
	}
}

func TestAccountRuntimeInstallsAndEnforcesDisconnectGuard(t *testing.T) {
	ctx := context.Background()
	r, _, account := testRuntime(t)
	accountName := multicluster.ClusterName("account")
	binding := &kcpapisv1alpha1.APIBinding{ObjectMeta: metav1.ObjectMeta{Name: "ocp", UID: types.UID("binding-1")}}
	logical := &kcpcorev1alpha1.LogicalCluster{ObjectMeta: metav1.ObjectMeta{Name: kcpcorev1alpha1.LogicalClusterName}}
	if err := account.Create(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := account.Create(ctx, logical); err != nil {
		t.Fatal(err)
	}
	r.disconnectGuard = &accountDisconnectGuard{bindingName: "ocp", url: "https://guard.example.test/disconnect", caBundle: []byte("ca")}
	r.accounts = map[multicluster.ClusterName]client.Client{accountName: account}
	if err := r.ensureDisconnectWebhook(ctx, accountName, account); err != nil {
		t.Fatal(err)
	}
	webhook := &admissionv1.ValidatingWebhookConfiguration{}
	if err := account.Get(ctx, client.ObjectKey{Name: "ocp-disconnect-binding-1"}, webhook); err != nil {
		t.Fatal(err)
	}
	if len(webhook.Webhooks) != 1 || webhook.Webhooks[0].ClientConfig.URL == nil || *webhook.Webhooks[0].ClientConfig.URL != r.disconnectGuard.url {
		t.Fatalf("wrong disconnect webhook: %#v", webhook.Webhooks)
	}
	resolved, deleting, err := r.resolveDisconnectAccount(ctx, string(accountName), binding.Name, binding.UID)
	if err != nil || deleting || resolved == nil {
		t.Fatalf("account resolution failed: client=%v deleting=%v err=%v", resolved, deleting, err)
	}
	if _, _, err := r.resolveDisconnectAccount(ctx, string(accountName), binding.Name, types.UID("other")); err == nil {
		t.Fatal("wrong binding UID was accepted")
	}
}

func platformNamespaceForAccount(name multicluster.ClusterName) (string, error) {
	return libutils.StableMCPNamespace(defaultControlPlaneName, accountControlPlaneNamespace(name))
}

func containsAll(value string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(value, needle) {
			return false
		}
	}
	return true
}
