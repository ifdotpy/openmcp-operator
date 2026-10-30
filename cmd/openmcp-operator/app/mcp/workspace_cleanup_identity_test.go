package mcp

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	controllerclusters "github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	authv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	"github.com/openmcp-project/openmcp-operator/internal/config"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
)

// This HTTP fixture tests the broker and provider REST callers. It models KCP's
// foreign identity gate and namespace sweep; it does not replace a live KCP test.
type cleanupFixture struct {
	t                         *testing.T
	r                         *workspaceRuntime
	platform, account, issuer client.Client
	ar                        *clustersv1alpha1.AccessRequest
	service                   *unstructured.Unstructured
	record                    *corev1.Secret
	server                    *httptest.Server
	mu                        sync.Mutex
	events                    []string
	tokens                    map[string]string
	deny, failStatus          bool
}

func newCleanupFixture(t *testing.T) *cleanupFixture {
	t.Helper()
	r, platform, _ := testRuntime(t)
	scheme := testScheme(t)
	if err := kcpcorev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	gvk := schema.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Example"}
	provider := workspaceProvider{Name: "example", ProviderName: "example", RegistrationNamespace: "shared", Resource: metav1.GroupVersionKind(gvk)}
	r.providers = []workspaceProvider{provider}
	r.tokenLifetime = time.Minute
	name := multicluster.ClusterName("tenant")
	ns, err := runtimeNamespaceForWorkspace(name)
	if err != nil {
		t.Fatal(err)
	}
	ar := &clustersv1alpha1.AccessRequest{ObjectMeta: metav1.ObjectMeta{Name: "example-onboarding", Namespace: ns, UID: "request-uid", Labels: map[string]string{workspaceProviderLabel: "example", apiconst.ManagedByLabel: "Example"}}, Spec: clustersv1alpha1.AccessRequestSpec{ClusterRef: &commonapi.ObjectReference{Name: workspaceClusterName, Namespace: ns}, Token: &clustersv1alpha1.TokenConfig{Permissions: []clustersv1alpha1.PermissionsRequest{{Rules: []rbacv1.PolicyRule{{APIGroups: []string{gvk.Group}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch", "create", "delete", "update", "patch"}}}}}}}}
	logical := &kcpcorev1alpha1.LogicalCluster{ObjectMeta: metav1.ObjectMeta{Name: kcpcorev1alpha1.LogicalClusterName, UID: "account-uid", Annotations: map[string]string{"kcp.io/cluster": "tenant"}}}
	binding := &kcpapisv1alpha1.APIBinding{ObjectMeta: metav1.ObjectMeta{Name: "services", UID: "binding-1", Finalizers: []string{"kcp.io/test"}}}
	service := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": gvk.GroupVersion().String(), "kind": gvk.Kind, "metadata": map[string]interface{}{"name": defaultControlPlaneName, "namespace": workspaceControlPlaneNamespace(name), "finalizers": []interface{}{"services.example.io/cleanup"}}}}
	record := &cleanupRecord{Provider: provider, Resource: schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: "examples"}, Namespaced: true}
	account := clientfake.NewClientBuilder().WithScheme(scheme).WithRESTMapper(cleanupRESTMapper(record)).WithStatusSubresource(service, &corev2alpha1.ControlPlane{}).WithObjects(logical, binding, service).Build()
	issuer := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cleanupNamespace}}).Build()
	for _, obj := range []client.Object{&providerv1alpha1.ServiceProvider{ObjectMeta: metav1.ObjectMeta{Name: "example", UID: "provider-uid"}, Status: providerv1alpha1.ServiceProviderStatus{Resources: []metav1.GroupVersionKind{provider.Resource}}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, UID: "runtime-uid"}}, ar} {
		if err := platform.Create(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
	}
	f := &cleanupFixture{t: t, r: r, platform: platform, account: account, issuer: issuer, ar: ar, service: service, tokens: map[string]string{}}
	f.server = httptest.NewTLSServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.server.Close)
	r.cleanupClient, r.cleanupCluster = issuer, "provider"
	r.cleanupBrokerUser = "operator-broker"
	r.cleanupConfig = &rest.Config{Host: f.server.URL + "/clusters/provider", BearerToken: "broker-token", TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw})}}
	r.cleanupNewClient = func(cfg *rest.Config, opts client.Options) (client.Client, error) {
		if cfg.BearerToken == "broker-token" {
			return f.brokerNativeClient(), nil
		}
		return client.New(cfg, opts)
	}
	cfg := &rest.Config{Host: f.server.URL + "/clusters/tenant", BearerToken: "broker-token", TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw})}}
	if err := r.reconcileAccessRequests(context.Background(), ns, account, cfg, testBindingOwner(), nil); err != nil {
		t.Fatal(err)
	}
	f.record = &corev1.Secret{}
	if err := issuer.Get(context.Background(), client.ObjectKey{Namespace: cleanupNamespace, Name: cleanupIdentityName(ar)}, f.record); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *cleanupFixture) brokerNativeClient() client.Client {
	return interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := f.checkBrokerPermission(ctx, c, obj, key.Name, "get"); err != nil {
				return err
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := f.checkBrokerPermission(ctx, c, obj, obj.GetName(), "delete"); err != nil {
				return err
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
}

func (f *cleanupFixture) checkBrokerPermission(ctx context.Context, c client.Client, obj client.Object, name, verb string) error {
	resource := ""
	switch obj.(type) {
	case *kcpcorev1alpha1.LogicalCluster:
		resource = "logicalclusters"
	case *kcpapisv1alpha1.APIBinding:
		resource = "apibindings"
	case *rbacv1.ClusterRole:
		resource = "clusterroles"
	case *rbacv1.ClusterRoleBinding:
		resource = "clusterrolebindings"
	}
	bindings := &rbacv1.ClusterRoleBindingList{}
	if err := c.List(ctx, bindings); err != nil {
		return err
	}
	for _, binding := range bindings.Items {
		if !slices.ContainsFunc(binding.Subjects, func(subject rbacv1.Subject) bool { return subject.Kind == "User" && subject.Name == "operator-broker" }) {
			continue
		}
		role := &rbacv1.ClusterRole{}
		if err := c.Get(ctx, client.ObjectKey{Name: binding.RoleRef.Name}, role); err != nil {
			return err
		}
		access := slices.ContainsFunc(role.Rules, func(rule rbacv1.PolicyRule) bool {
			return slices.Contains(rule.NonResourceURLs, "/") && slices.Contains(rule.Verbs, "access")
		})
		if !access {
			continue
		}
		for _, rule := range role.Rules {
			if slices.Contains(rule.Resources, resource) && slices.Contains(rule.ResourceNames, name) && slices.Contains(rule.Verbs, verb) {
				return nil
			}
		}
	}
	return apierrors.NewForbidden(schema.GroupResource{Resource: resource}, name, fmt.Errorf("broker has no explicit native grant"))
}

func (f *cleanupFixture) event(value string) { f.events = append(f.events, value) }

func (f *cleanupFixture) serveHTTP(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	ctx := req.Context()
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "clusters" {
		f.t.Errorf("unexpected discovery request: %s", req.URL.Path)
		http.Error(w, "discovery forbidden", http.StatusForbidden)
		return
	}
	account := parts[1]
	token := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
	write := func(obj interface{}) {
		if err := json.NewEncoder(w).Encode(obj); err != nil {
			f.t.Error(err)
		}
	}
	reject := func(code int, reason metav1.StatusReason) {
		w.WriteHeader(code)
		write(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: reason, Message: req.Method + " " + req.URL.Path, Code: int32(code)})
	}
	if strings.HasSuffix(req.URL.Path, "/token") {
		if token != "broker-token" {
			reject(401, metav1.StatusReasonUnauthorized)
			return
		}
		var issuer client.Client
		switch account {
		case "provider":
			issuer = f.issuer
		case "tenant":
			issuer = f.account
		default:
			reject(403, metav1.StatusReasonForbidden)
			return
		}
		sa := &corev1.ServiceAccount{}
		if err := issuer.Get(ctx, client.ObjectKey{Namespace: parts[5], Name: parts[7]}, sa); err != nil {
			reject(404, metav1.StatusReasonNotFound)
			return
		}
		credential := fmt.Sprintf("token-%d", len(f.tokens)+1)
		f.tokens[credential] = sa.Name
		f.event("renew:" + account)
		write(&authv1.TokenRequest{TypeMeta: metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenRequest"}, Status: authv1.TokenRequestStatus{Token: credential, ExpirationTimestamp: metav1.NewTime(time.Now().Add(time.Minute))}})
		return
	}
	identity := f.tokens[token]
	sa := &corev1.ServiceAccount{}
	binding := &rbacv1.ClusterRoleBinding{}
	authorized := identity != "" && account == "tenant" && !f.deny && f.issuer.Get(ctx, client.ObjectKey{Namespace: cleanupNamespace, Name: identity}, sa) == nil && f.account.Get(ctx, client.ObjectKey{Name: identity}, binding) == nil
	if !authorized {
		reject(403, metav1.StatusReasonForbidden)
		return
	}
	if strings.HasSuffix(req.URL.Path, "/selfsubjectaccessreviews") {
		write(&authorizationv1.SelfSubjectAccessReview{TypeMeta: metav1.TypeMeta{APIVersion: "authorization.k8s.io/v1", Kind: "SelfSubjectAccessReview"}, Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}})
		return
	}
	if !strings.Contains(req.URL.Path, "/apis/services.example.io/v1/") {
		reject(403, metav1.StatusReasonForbidden)
		return
	}
	if strings.HasSuffix(req.URL.Path, "/examples") && req.Method == http.MethodGet {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "ExampleList"})
		if err := f.account.List(ctx, list); err != nil {
			f.t.Error(err)
			reject(500, metav1.StatusReasonInternalError)
			return
		}
		f.event("list:tenant")
		write(list)
		return
	}
	if req.Method != http.MethodPatch {
		reject(403, metav1.StatusReasonForbidden)
		return
	}
	service := f.service.DeepCopy()
	if err := f.account.Get(ctx, client.ObjectKeyFromObject(service), service); err != nil {
		reject(404, metav1.StatusReasonNotFound)
		return
	}
	var patch map[string]interface{}
	if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
		f.t.Error(err)
		reject(400, metav1.StatusReasonBadRequest)
		return
	}
	if strings.HasSuffix(req.URL.Path, "/status") {
		if f.failStatus {
			reject(403, metav1.StatusReasonForbidden)
			return
		}
		service.Object["status"] = patch["status"]
		if err := f.account.Status().Update(ctx, service); err != nil {
			f.t.Error(err)
			reject(500, metav1.StatusReasonInternalError)
			return
		}
		f.event("status")
	} else {
		if !slices.Contains(f.events, "status") {
			f.t.Error("finalizer patch preceded final status")
		}
		service.SetFinalizers(nil)
		if err := f.account.Update(ctx, service); err != nil {
			f.t.Error(err)
			reject(500, metav1.StatusReasonInternalError)
			return
		}
		f.event("finalizer")
	}
	write(service)
}

func (f *cleanupFixture) expire() {
	f.t.Helper()
	if err := f.issuer.Get(context.Background(), client.ObjectKeyFromObject(f.record), f.record); err != nil {
		f.t.Fatal(err)
	}
	f.record.Data[clustersv1alpha1.SecretKeyExpirationTimestamp] = []byte(time.Now().Add(-time.Hour).Format(time.RFC3339))
	if err := f.issuer.Update(context.Background(), f.record); err != nil {
		f.t.Fatal(err)
	}
}

func (f *cleanupFixture) scoped() client.Client {
	f.t.Helper()
	if err := f.issuer.Get(context.Background(), client.ObjectKeyFromObject(f.record), f.record); err != nil {
		f.t.Fatal(err)
	}
	record, err := readCleanupRecord(f.record)
	if err != nil {
		f.t.Fatal(err)
	}
	c, err := f.r.cleanupAccountClient(record, f.record.Data[clustersv1alpha1.SecretKeyKubeconfig])
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

func TestCleanupIdentitySurvivesDeletionAndRestart(t *testing.T) {
	for _, trigger := range []string{"account", "APIBinding", "ControlPlane"} {
		t.Run(trigger, func(t *testing.T) {
			f := newCleanupFixture(t)
			ctx := context.Background()
			if err := f.account.Delete(ctx, f.service); err != nil {
				t.Fatal(err)
			}
			switch trigger {
			case "account":
				logical := &kcpcorev1alpha1.LogicalCluster{}
				if err := f.account.Get(ctx, client.ObjectKey{Name: "cluster"}, logical); err != nil {
					t.Fatal(err)
				}
				logical.Finalizers = []string{"kcp.io/test"}
				if err := f.account.Update(ctx, logical); err != nil {
					t.Fatal(err)
				}
				if err := f.account.Delete(ctx, logical); err != nil {
					t.Fatal(err)
				}
				// The namespace sweep removes the account-local identity and issuer material.
				for _, obj := range []client.Object{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: controllerName, Namespace: workspaceAccessNamespace(f.ar)}}, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: credentialIssuerName, Namespace: workspaceAccessNamespace(f.ar)}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: workspaceIssuerSecretName(f.ar), Namespace: workspaceAccessNamespace(f.ar)}}} {
					if err := f.account.Create(ctx, obj); err != nil {
						t.Fatal(err)
					}
					if err := f.account.Delete(ctx, obj); err != nil {
						t.Fatal(err)
					}
				}
			case "APIBinding":
				if err := f.account.Delete(ctx, &kcpapisv1alpha1.APIBinding{ObjectMeta: metav1.ObjectMeta{Name: "services"}}); err != nil {
					t.Fatal(err)
				}
			case "ControlPlane":
				// The ControlPlane caller deletes its access only after service cleanup.
				if err := f.platform.Delete(ctx, f.ar); err != nil {
					t.Fatal(err)
				}
			}
			f.expire()
			// Reconstruct the runtime with no engagement or remembered account clients.
			fresh, _, _ := testRuntime(t)
			fresh.platform = f.r.platform
			fresh.cleanupClient, fresh.cleanupConfig, fresh.cleanupCluster = f.issuer, f.r.cleanupConfig, "provider"
			fresh.cleanupNewClient = f.r.cleanupNewClient
			fresh.tokenLifetime = time.Minute
			f.r = fresh
			if err := fresh.reconcileCleanupRecords(ctx); err == nil || !strings.Contains(err.Error(), "waiting for") {
				t.Fatalf("cleanup did not wait for final writes: %v", err)
			}
			if f.events[len(f.events)-2] != "renew:provider" {
				t.Fatalf("restart did not renew in provider workspace: %v", f.events)
			}
			scoped := f.scoped()
			old := f.service.DeepCopy()
			f.service.Object["status"] = map[string]interface{}{"phase": "Deleted"}
			f.failStatus = true
			if err := scoped.Status().Patch(ctx, f.service, client.MergeFrom(old)); !apierrors.IsForbidden(err) {
				t.Fatalf("status denial missing: %v", err)
			}
			if err := fresh.reconcileCleanupRecords(ctx); err == nil {
				t.Fatal("revoked after failed status write")
			}
			f.failStatus = false
			if err := scoped.Status().Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
				t.Fatal(err)
			}
			if err := fresh.reconcileCleanupRecords(ctx); err == nil {
				t.Fatal("revoked before finalizer patch")
			}
			old = f.service.DeepCopy()
			f.service.SetFinalizers(nil)
			if err := scoped.Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
				t.Fatal(err)
			}
			if err := fresh.reconcileCleanupRecords(ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("cleanup record remains: %v", err)
			}
			if err := f.issuer.Get(ctx, client.ObjectKey{Namespace: cleanupNamespace, Name: f.record.Name}, &corev1.ServiceAccount{}); !apierrors.IsNotFound(err) {
				t.Fatalf("cleanup identity remains: %v", err)
			}
			record := &cleanupRecord{Provider: workspaceProvider{Resource: metav1.GroupVersionKind{Group: "services.example.io", Version: "v1", Kind: "Example"}}, Resource: schema.GroupVersionResource{Group: "services.example.io", Version: "v1", Resource: "examples"}, Namespaced: true}
			if err := cleanupServicesGone(ctx, scoped, record); !apierrors.IsForbidden(err) {
				t.Fatalf("old token still authorized: %v", err)
			}
		})
	}
}

func TestCleanupAccountLocalIssuerNegativeControl(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	cfg := &rest.Config{Host: f.server.URL + "/clusters/tenant", BearerToken: "broker-token", TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw})}}
	issuer, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: controllerName, Namespace: workspaceAccessNamespace(f.ar)}}
	if err := f.account.Create(ctx, sa); err != nil {
		t.Fatal(err)
	}
	if _, _, err := mintWorkspaceCredential(ctx, issuer, cfg, f.ar, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := f.account.Delete(ctx, sa); err != nil {
		t.Fatal(err)
	}
	if _, _, err := mintWorkspaceCredential(ctx, issuer, cfg, f.ar, time.Minute); !apierrors.IsNotFound(err) {
		t.Fatalf("negative control did not expose deleted account-local issuer: %v", err)
	}
	f.expire()
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatalf("external issuer did not survive: %v", err)
	}
}

func TestCleanupDenialHasNoDestructiveEffect(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	deletes := 0
	f.account = interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		deletes++
		return c.Delete(ctx, obj, opts...)
	}})
	f.deny = true
	if err := f.r.reconcileCleanupRecords(ctx); !apierrors.IsForbidden(err) {
		t.Fatalf("expected denial, got %v", err)
	}
	if deletes != 0 {
		t.Fatalf("denied cleanup performed %d deletes", deletes)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.Secret{}); err != nil {
		t.Fatal("denial discarded durable record", err)
	}
	ns := &corev1.Namespace{}
	if err := f.platform.Get(ctx, client.ObjectKey{Name: f.ar.Namespace}, ns); err != nil || !ns.DeletionTimestamp.IsZero() {
		t.Fatalf("denial destroyed runtime: %v", err)
	}
}

func TestCleanupCredentialCannotAccessAnotherAccount(t *testing.T) {
	f := newCleanupFixture(t)
	cfg, err := clientcmd.RESTConfigFromKubeConfig(f.record.Data[clustersv1alpha1.SecretKeyKubeconfig])
	if err != nil {
		t.Fatal(err)
	}
	cfg.Host = f.server.URL + "/clusters/other"
	record, err := readCleanupRecord(f.record)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: f.issuer.Scheme(), Mapper: cleanupRESTMapper(record)})
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanupServicesGone(context.Background(), c, record); !apierrors.IsForbidden(err) {
		t.Fatalf("foreign account accepted cleanup token: %v", err)
	}
	grants := cleanupGrants(record, "provider")
	role := grants[0].(*rbacv1.ClusterRole)
	if !slices.Equal(role.Rules[0].NonResourceURLs, []string{"/"}) || !slices.Equal(role.Rules[0].Verbs, []string{"access"}) {
		t.Fatal("missing foreign workspace access gate")
	}
	for _, rule := range role.Rules[1:] {
		if slices.Contains(rule.Resources, "*") || slices.Contains(rule.Verbs, "*") || slices.Contains(rule.Verbs, "create") || slices.Contains(rule.Verbs, "delete") {
			t.Fatalf("excess cleanup permission: %#v", rule)
		}
	}
	binding := grants[1].(*rbacv1.ClusterRoleBinding)
	if binding.Subjects[0].Kind != "User" || binding.Subjects[0].Name != "system:kcp:serviceaccount:provider:openmcp-cleanup:"+f.record.Name || len(binding.OwnerReferences) != 0 {
		t.Fatalf("wrong cross-workspace identity: %#v", binding)
	}
}

func TestCleanupRevocationResumesWithoutRenewal(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.account.Delete(ctx, f.service); err != nil {
		t.Fatal(err)
	}
	scoped := f.scoped()
	old := f.service.DeepCopy()
	f.service.Object["status"] = map[string]interface{}{"phase": "Deleted"}
	if err := scoped.Status().Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
		t.Fatal(err)
	}
	old = f.service.DeepCopy()
	f.service.SetFinalizers(nil)
	if err := scoped.Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	original := f.account
	fail := true
	f.account = interceptor.NewClient(original.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if fail {
			return apierrors.NewForbidden(schema.GroupResource{Group: rbacv1.GroupName, Resource: "clusterrolebindings"}, obj.GetName(), fmt.Errorf("injected revocation denial"))
		}
		return c.Delete(ctx, obj, opts...)
	}})
	if err := f.r.reconcileCleanupRecords(ctx); !apierrors.IsForbidden(err) {
		t.Fatalf("lost failed revocation: %v", err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), f.record); err != nil {
		t.Fatal(err)
	}
	record, err := readCleanupRecord(f.record)
	if err != nil {
		t.Fatal(err)
	}
	if record.Phase != cleanupRevoking {
		t.Fatalf("failed revocation lost durable phase: %s", record.Phase)
	}
	before := len(f.tokens)
	f.expire()
	fail = false
	fresh, _, _ := testRuntime(t)
	fresh.platform = f.r.platform
	fresh.cleanupClient, fresh.cleanupConfig, fresh.cleanupCluster = f.issuer, f.r.cleanupConfig, "provider"
	fresh.cleanupNewClient = f.r.cleanupNewClient
	if err := fresh.reconcileCleanupRecords(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.tokens) != before {
		t.Fatal("restart renewed a credential during revocation")
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("retry left cleanup record: %v", err)
	}
}

func TestCleanupDoesNotWidenAccessRequests(t *testing.T) {
	f := newCleanupFixture(t)
	for _, change := range []func(*clustersv1alpha1.AccessRequest){
		func(ar *clustersv1alpha1.AccessRequest) { ar.Spec.Token.Permissions[0].Namespace = "one-namespace" },
		func(ar *clustersv1alpha1.AccessRequest) {
			ar.Spec.Token.Permissions[0].Rules[0].ResourceNames = []string{"one-service"}
		},
		func(ar *clustersv1alpha1.AccessRequest) {
			ar.Spec.Token.Permissions[0].Rules[0].Resources = []string{"unrelated"}
		},
		func(ar *clustersv1alpha1.AccessRequest) {
			ar.Spec.Token.Permissions[0].Rules[0].Verbs = []string{"get"}
		},
		func(ar *clustersv1alpha1.AccessRequest) {
			ar.Spec.Token.RoleRefs = []commonapi.RoleRef{{Kind: clusterRoleKind, Name: "read-only"}}
		},
	} {
		ar := f.ar.DeepCopy()
		change(ar)
		if _, ok := f.r.cleanupProvider(ar); ok {
			t.Fatal("converted restricted request into account-wide cleanup grant")
		}
	}
}

func TestCleanupRejectsReplacementRequestBeforePublication(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil {
		t.Fatal(err)
	}
	f.ar.UID = "replacement"
	if err := f.platform.Update(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	target := &corev1.Secret{}
	key := client.ObjectKey{Namespace: f.ar.Namespace, Name: f.ar.Name + "-kubeconfig"}
	if err := f.platform.Get(ctx, key, target); err != nil {
		t.Fatal(err)
	}
	original := string(target.Data[clustersv1alpha1.SecretKeyKubeconfig])
	f.expire()
	if err := f.r.reconcileCleanupRecords(ctx); err == nil || !strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("replacement request accepted: %v", err)
	}
	if err := f.platform.Get(ctx, key, target); err != nil {
		t.Fatal(err)
	}
	if string(target.Data[clustersv1alpha1.SecretKeyKubeconfig]) != original {
		t.Fatal("published old identity to replacement request")
	}
}

func TestCleanupThroughControlPlaneReconciler(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := logging.NewContext(context.Background(), f.r.log)
	cp := &corev2alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: defaultControlPlaneName, Namespace: workspaceControlPlaneNamespace("tenant"), UID: "cp-uid", Finalizers: []string{corev2alpha1.MCPFinalizer}}}
	// Use the real ControlPlane caller to request service and runtime deletion.
	if err := f.account.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{}
	if err := f.platform.Get(ctx, client.ObjectKey{Name: f.ar.Namespace}, ns); err != nil {
		t.Fatal(err)
	}
	ns.Labels = map[string]string{corev2alpha1.MCPNameLabel: cp.Name, corev2alpha1.MCPNamespaceLabel: cp.Namespace, apiconst.ManagedByLabel: controlplane.ControllerName}
	if err := f.platform.Update(ctx, ns); err != nil {
		t.Fatal(err)
	}
	rec, err := controlplane.NewManagedControlPlaneReconciler(f.r.platform, controllerclusters.NewTestClusterFromClient("workspace", f.account), events.NewFakeRecorder(100), func(context.Context) (*config.ManagedControlPlaneConfig, error) {
		return &config.ManagedControlPlaneConfig{}, nil
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.account.Delete(ctx, cp); err != nil {
		t.Fatal(err)
	}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := f.account.Get(ctx, client.ObjectKeyFromObject(f.service), f.service); err != nil || f.service.GetDeletionTimestamp().IsZero() {
		t.Fatalf("ControlPlane did not request service deletion: %v", err)
	}
	f.expire()
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.Secret{}); err != nil {
		t.Fatal("ControlPlane revoked access before service writes", err)
	}
	scoped := f.scoped()
	old := f.service.DeepCopy()
	f.service.Object["status"] = map[string]interface{}{"phase": "Deleted"}
	if err := scoped.Status().Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
		t.Fatal(err)
	}
	old = f.service.DeepCopy()
	f.service.SetFinalizers(nil)
	if err := scoped.Patch(ctx, f.service, client.MergeFrom(old)); err != nil {
		t.Fatal(err)
	}
	if _, err := rec.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := f.platform.Get(ctx, client.ObjectKey{Name: f.ar.Namespace}, ns); !apierrors.IsNotFound(err) {
		t.Fatalf("ControlPlane did not remove completed runtime: %v", err)
	}
	if err := f.r.reconcileCleanupRecords(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("ControlPlane cleanup left external identity: %v", err)
	}
}

func TestCleanupWorkspaceDenialPreservesPendingRequests(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.platform.Delete(ctx, f.ar); err != nil {
		t.Fatal(err)
	}
	denied := interceptor.NewClient(f.account.(client.WithWatch), interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "services.example.io", Resource: "examples"}, "", fmt.Errorf("injected access denial"))
	}})
	if err := f.r.cleanupWorkspace(ctx, "tenant", denied); !apierrors.IsForbidden(err) {
		t.Fatalf("expected access denial: %v", err)
	}
	if err := f.platform.Get(ctx, client.ObjectKeyFromObject(f.ar), f.ar); err != nil || !slices.Contains(f.ar.Finalizers, workspaceAccessFinalizer) {
		t.Fatalf("denial released pending access: %v", err)
	}
	if err := f.issuer.Get(ctx, client.ObjectKeyFromObject(f.record), f.record); err != nil {
		t.Fatal("denial removed cleanup record", err)
	}
	ns := &corev1.Namespace{}
	if err := f.platform.Get(ctx, client.ObjectKey{Name: f.ar.Namespace}, ns); err != nil || !ns.DeletionTimestamp.IsZero() {
		t.Fatalf("denial deleted runtime: %v", err)
	}
}

func TestCleanupBrokerCannotDeleteUnlistedGrant(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	unrelated := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "another-account-role"}}
	if err := f.account.Create(ctx, unrelated); err != nil {
		t.Fatal(err)
	}
	if err := f.brokerNativeClient().Delete(ctx, unrelated); !apierrors.IsForbidden(err) {
		t.Fatalf("broker deleted unlisted grant: %v", err)
	}
	if err := f.account.Get(ctx, client.ObjectKeyFromObject(unrelated), unrelated); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupReadinessRetainsLegacyServices(t *testing.T) {
	f := newCleanupFixture(t)
	ctx := context.Background()
	if err := f.r.cleanupProvidersReady(ctx, f.ar.Namespace, false); err != nil {
		t.Fatal(err)
	}
	if err := f.issuer.Delete(ctx, f.record); err != nil {
		t.Fatal(err)
	}
	if err := f.r.cleanupProvidersReady(ctx, f.ar.Namespace, true); err == nil || !strings.Contains(err.Error(), "retain services") {
		t.Fatalf("legacy service had no retained-state error: %v", err)
	}
}
