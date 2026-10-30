package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kcp-dev/logicalcluster/v3"
	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	authv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/openmcp-project/controller-utils/pkg/clusteraccess"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
)

var errCleanupServicesRemain = errors.New("cleanup services remain")

const (
	cleanupNamespace          = "openmcp-cleanup"
	cleanupRecordLabel        = "workspace.openmcp.cloud/cleanup-record"
	cleanupRecordKey          = "record"
	cleanupActive             = "active"
	cleanupDraining           = "draining"
	cleanupRevoking           = "revoking"
	cleanupServicesFinal      = "services-final"
	cleanupGrantRetry         = "grant-retry"
	cleanupCompleteAnnotation = "workspace.openmcp.cloud/cleanup-complete"
)

// cleanupRecord has no owner in the account or runtime namespace. The immutable
// account and request UIDs prevent a restart from adopting a replacement account.
type cleanupRecord struct {
	Account     string                         `json:"account"`
	AccountUID  types.UID                      `json:"accountUID"`
	RuntimeUID  types.UID                      `json:"runtimeUID"`
	ProviderUID types.UID                      `json:"providerUID"`
	Request     clustersv1alpha1.AccessRequest `json:"request"`
	Provider    workspaceProvider              `json:"provider"`
	Resource    schema.GroupVersionResource    `json:"resource"`
	Namespaced  bool                           `json:"namespaced"`
	Binding     metav1.OwnerReference          `json:"binding"`
	Phase       string                         `json:"phase"`
	Ready       bool                           `json:"ready"`
}

func cleanupIdentityName(ar *clustersv1alpha1.AccessRequest) string {
	return "cleanup-" + string(ar.UID)
}

func cleanupFinalPhase(phase string) bool {
	switch phase {
	case cleanupServicesFinal, cleanupRevoking, cleanupGrantRetry:
		return true
	default:
		return false
	}
}

func (r *workspaceRuntime) configureCleanupBroker(ctx context.Context, c client.Client, cfg *rest.Config) error {
	logical := &kcpcorev1alpha1.LogicalCluster{}
	if err := c.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical); err != nil {
		return err
	}
	name := logical.Annotations["kcp.io/cluster"]
	if name == "" {
		return fmt.Errorf("provider workspace has no immutable logical cluster name")
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cleanupNamespace}}
	if err := c.Get(ctx, client.ObjectKeyFromObject(ns), ns); err != nil {
		return fmt.Errorf("permanent cleanup namespace must exist before startup: %w", err)
	}
	if !ns.DeletionTimestamp.IsZero() {
		return fmt.Errorf("permanent cleanup namespace is terminating")
	}
	issuer, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	identity, err := issuer.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("read broker identity: %w", err)
	}
	brokerUser, err := cleanupBrokerUsername(identity.Status.UserInfo)
	if err != nil {
		return err
	}
	r.cleanupBrokerUser = brokerUser
	r.cleanupClient, r.cleanupConfig, r.cleanupCluster = c, rest.CopyConfig(cfg), name
	return nil
}

// KCP authenticates a ServiceAccount with its local username. Foreign workspace
// RBAC matches the global username from EffectiveUsers instead.
func cleanupBrokerUsername(identity authv1.UserInfo) (string, error) {
	if identity.Username == "" {
		return "", fmt.Errorf("broker identity is empty")
	}
	const localPrefix = "system:serviceaccount:"
	if !strings.HasPrefix(identity.Username, localPrefix) {
		return identity.Username, nil
	}
	parts := strings.Split(strings.TrimPrefix(identity.Username, localPrefix), ":")
	origins := identity.Extra["authentication.kcp.io/cluster-name"]
	if len(parts) != 2 || len(validation.IsDNS1123Label(parts[0])) != 0 || len(validation.IsDNS1123Subdomain(parts[1])) != 0 || len(origins) != 1 || !logicalcluster.Name(origins[0]).IsValid() {
		return "", fmt.Errorf("ServiceAccount broker requires one authenticated KCP origin and a valid local username")
	}
	return fmt.Sprintf("system:kcp:serviceaccount:%s:%s:%s", origins[0], parts[0], parts[1]), nil
}

// Only account-wide service permissions qualify. Namespace permissions and role
// references retain their original AccessRequest semantics.
func (r *workspaceRuntime) cleanupProvider(ar *clustersv1alpha1.AccessRequest) (workspaceProvider, bool) {
	for _, provider := range r.providers {
		if cleanupProviderMatches(ar, provider) {
			return provider, true
		}
	}
	return workspaceProvider{}, false
}

func cleanupProviderMatches(ar *clustersv1alpha1.AccessRequest, provider workspaceProvider) bool {
	if ar.Spec.Token == nil || len(ar.Spec.Token.RoleRefs) != 0 || len(ar.Spec.Token.Permissions) == 0 {
		return false
	}
	manager := ar.Labels[apiconst.ManagedByLabel]
	if manager != provider.Resource.Kind && manager != strings.ToLower(provider.Resource.Kind)+"."+provider.Resource.Group {
		return false
	}
	for _, permission := range ar.Spec.Token.Permissions {
		if permission.Namespace != "" || len(permission.Rules) == 0 {
			return false
		}
		for _, rule := range permission.Rules {
			if len(rule.APIGroups) != 1 || rule.APIGroups[0] != provider.Resource.Group || len(rule.NonResourceURLs) != 0 || len(rule.ResourceNames) != 0 || len(rule.Resources) != 1 || rule.Resources[0] != "*" || !containsCleanupVerbs(rule.Verbs) {
				return false
			}
		}
	}
	return true
}

func (r *workspaceRuntime) cleanupProviderForRequest(ctx context.Context, ar *clustersv1alpha1.AccessRequest) (workspaceProvider, bool, error) {
	if r.cleanupClient == nil {
		return workspaceProvider{}, false, nil
	}
	secret := &corev1.Secret{}
	err := r.cleanupClient.Get(ctx, client.ObjectKey{Namespace: cleanupNamespace, Name: cleanupIdentityName(ar)}, secret)
	if apierrors.IsNotFound(err) {
		provider, ok := r.cleanupProvider(ar)
		return provider, ok, nil
	}
	if err != nil {
		return workspaceProvider{}, false, err
	}
	record, err := readCleanupRecord(secret)
	if err != nil {
		return workspaceProvider{}, false, err
	}
	if !cleanupProviderMatches(ar, record.Provider) {
		return workspaceProvider{}, false, fmt.Errorf("cleanup request scope changed; finish existing service cleanup before replacing the request")
	}
	return record.Provider, true, nil
}

func containsCleanupVerbs(verbs []string) bool {
	for _, required := range []string{verbGet, verbList, verbWatch, verbUpdate, verbPatch} {
		found := false
		for _, verb := range verbs {
			if verb == required || verb == "*" {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func cleanupGrants(record *cleanupRecord, issuerCluster string) []client.Object {
	name := cleanupIdentityName(&record.Request)
	metadata := metav1.ObjectMeta{Name: name, Labels: map[string]string{cleanupRecordLabel: name}}
	subject := rbacv1.Subject{APIGroup: rbacv1.GroupName, Kind: "User", Name: fmt.Sprintf("system:kcp:serviceaccount:%s:%s:%s", issuerCluster, cleanupNamespace, name)}
	return []client.Object{
		&rbacv1.ClusterRole{ObjectMeta: metadata, Rules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/"}, Verbs: []string{"access"}},
			{APIGroups: []string{record.Resource.Group}, Resources: []string{record.Resource.Resource}, Verbs: []string{verbGet, verbList, verbWatch, verbUpdate, verbPatch}},
			{APIGroups: []string{record.Resource.Group}, Resources: []string{record.Resource.Resource + "/status"}, Verbs: []string{verbUpdate, verbPatch}},
		}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metadata, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: name}, Subjects: []rbacv1.Subject{subject}},
	}
}

// This account-level broker grant outlives individual cleanup identities. It has
// no service permissions and permits recovery after their credentials are revoked.
func (r *workspaceRuntime) ensureCleanupBrokerGrant(ctx context.Context, workspace client.Client, ar *clustersv1alpha1.AccessRequest, binding metav1.OwnerReference) error {
	if r.cleanupBrokerUser == "" {
		return fmt.Errorf("cleanup broker user is required")
	}
	digest := sha256.Sum256([]byte(r.cleanupBrokerUser))
	name := fmt.Sprintf("openmcp-cleanup-broker-%x", digest[:8])
	owner := fmt.Sprintf("broker-%x", digest[:8])
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := controllerutil.CreateOrUpdate(ctx, workspace, role, func() error {
		if role.ResourceVersion != "" && role.Labels[workspaceAccessOwnerLabel] != owner {
			return fmt.Errorf("refusing to adopt account broker grant")
		}
		names := []string{cleanupIdentityName(ar)}
		if len(role.Rules) > 3 {
			names = append(names, role.Rules[3].ResourceNames...)
		}
		names = slices.Compact(slices.Sorted(slices.Values(names)))
		role.Labels = map[string]string{workspaceAccessOwnerLabel: owner}
		role.OwnerReferences = nil
		role.Rules = []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/"}, Verbs: []string{"access"}},
			{APIGroups: []string{kcpcorev1alpha1.SchemeGroupVersion.Group}, Resources: []string{"logicalclusters"}, ResourceNames: []string{kcpcorev1alpha1.LogicalClusterName}, Verbs: []string{verbGet}},
			{APIGroups: []string{kcpapisv1alpha1.SchemeGroupVersion.Group}, Resources: []string{"apibindings"}, ResourceNames: []string{binding.Name}, Verbs: []string{verbGet}},
			{APIGroups: []string{rbacv1.GroupName}, Resources: []string{"clusterroles", "clusterrolebindings"}, ResourceNames: names, Verbs: []string{verbGet, verbDelete}},
		}
		return nil
	}); err != nil {
		return err
	}
	grant := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{workspaceAccessOwnerLabel: owner}}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: name}, Subjects: []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: "User", Name: r.cleanupBrokerUser}}}
	return workspaceOwnedUpsert(ctx, workspace, grant, owner)
}

func (r *workspaceRuntime) saveCleanupRecord(ctx context.Context, secret *corev1.Secret, record *cleanupRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	secret.Data[cleanupRecordKey] = data
	return r.cleanupClient.Update(ctx, secret)
}

func readCleanupRecord(secret *corev1.Secret) (*cleanupRecord, error) {
	record := &cleanupRecord{}
	if err := json.Unmarshal(secret.Data[cleanupRecordKey], record); err != nil {
		return nil, err
	}
	if record.Account == "" || record.AccountUID == "" || record.ProviderUID == "" || record.RuntimeUID == "" || record.Request.UID == "" || cleanupIdentityName(&record.Request) != secret.Name || record.Resource.Resource == "" {
		return nil, fmt.Errorf("invalid cleanup record %s", secret.Name)
	}
	switch record.Phase {
	case cleanupActive, cleanupDraining, cleanupServicesFinal, cleanupRevoking, cleanupGrantRetry:
	default:
		return nil, fmt.Errorf("unknown cleanup phase %q", record.Phase)
	}
	return record, nil
}

func (r *workspaceRuntime) reconcileCleanupAccess(ctx context.Context, workspace client.Client, cfg *rest.Config, ar *clustersv1alpha1.AccessRequest, provider workspaceProvider, binding metav1.OwnerReference) error {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	if ar.UID == "" {
		return fmt.Errorf("cleanup AccessRequest UID is required")
	}
	if err := r.ensureCleanupBrokerGrant(ctx, workspace, ar, binding); err != nil {
		return err
	}
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: cleanupNamespace, Name: cleanupIdentityName(ar)}
	err := r.cleanupClient.Get(ctx, key, secret)
	if apierrors.IsNotFound(err) {
		secret, err = r.createCleanupRecord(ctx, workspace, cfg, ar, provider, binding)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	record, err := readCleanupRecord(secret)
	if err != nil {
		return err
	}
	if record.Request.UID != ar.UID || record.Request.Namespace != ar.Namespace || record.Phase != cleanupActive {
		return fmt.Errorf("cleanup record is not active for request %s", ar.Name)
	}
	providerObject := &providerv1alpha1.ServiceProvider{}
	if err := r.platform.Client().Get(ctx, client.ObjectKey{Name: record.Provider.ProviderName}, providerObject); err != nil {
		return err
	}
	if providerObject.UID != record.ProviderUID {
		return fmt.Errorf("cleanup ServiceProvider UID changed")
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: secret.Name, Namespace: cleanupNamespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.cleanupClient, sa, func() error {
		if sa.ResourceVersion != "" && sa.Labels[cleanupRecordLabel] != secret.Name {
			return fmt.Errorf("refusing to adopt cleanup ServiceAccount")
		}
		sa.Labels = map[string]string{cleanupRecordLabel: secret.Name}
		return nil
	}); err != nil {
		return err
	}
	for _, object := range cleanupGrants(record, r.cleanupCluster) {
		object.SetLabels(map[string]string{cleanupRecordLabel: secret.Name, workspaceAccessOwnerLabel: string(ar.UID)})
		if err := workspaceOwnedUpsert(ctx, workspace, object, string(ar.UID)); err != nil {
			return err
		}
	}
	if err := r.refreshCleanupCredential(ctx, secret, record); err != nil {
		return err
	}
	if err := r.validateCleanupCredential(ctx, secret, record); err != nil {
		return err
	}
	record.Ready = true
	if err := r.saveCleanupRecord(ctx, secret, record); err != nil {
		return err
	}
	if err := r.publishCleanupCredential(ctx, secret, record); err != nil {
		return err
	}
	old := ar.DeepCopy()
	ar.Status.Phase = clustersv1alpha1.REQUEST_GRANTED
	ar.Status.ObservedGeneration = ar.Generation
	ar.Status.SecretRef = &commonapi.LocalObjectReference{Name: ar.Name + "-kubeconfig"}
	return r.platform.Client().Status().Patch(ctx, ar, client.MergeFrom(old))
}

func (r *workspaceRuntime) createCleanupRecord(ctx context.Context, workspace client.Client, cfg *rest.Config, ar *clustersv1alpha1.AccessRequest, provider workspaceProvider, binding metav1.OwnerReference) (*corev1.Secret, error) {
	key := client.ObjectKey{Namespace: cleanupNamespace, Name: cleanupIdentityName(ar)}
	logical := &kcpcorev1alpha1.LogicalCluster{}
	accountClient, err := r.newCleanupNativeClient(cfg, nil)
	if err != nil {
		return nil, err
	}
	if err := accountClient.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical); err != nil {
		return nil, err
	}
	account := logical.Annotations["kcp.io/cluster"]
	if account == "" || logical.UID == "" || account == r.cleanupCluster {
		return nil, fmt.Errorf("cleanup requires a distinct immutable account workspace")
	}
	expected, err := directWorkspaceConfig(r.cleanupConfig, multicluster.ClusterName(account))
	if err != nil {
		return nil, err
	}
	if expected.Host != cfg.Host {
		return nil, fmt.Errorf("cleanup account endpoint does not match logical cluster")
	}
	gvk := schema.GroupVersionKind(provider.Resource)
	mapping, err := workspace.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}
	runtimeNamespace := &corev1.Namespace{}
	if err := r.platform.Client().Get(ctx, client.ObjectKey{Name: ar.Namespace}, runtimeNamespace); err != nil {
		return nil, err
	}
	providerObject := &providerv1alpha1.ServiceProvider{}
	if err := r.platform.Client().Get(ctx, client.ObjectKey{Name: provider.ProviderName}, providerObject); err != nil {
		return nil, err
	}
	record := &cleanupRecord{Account: account, AccountUID: logical.UID, RuntimeUID: runtimeNamespace.UID, ProviderUID: providerObject.UID, Request: *ar.DeepCopy(), Provider: provider, Resource: mapping.Resource, Namespaced: mapping.Scope.Name() == meta.RESTScopeNameNamespace, Binding: binding, Phase: cleanupActive}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: map[string]string{cleanupRecordLabel: labelValueTrue}}, Data: map[string][]byte{cleanupRecordKey: data}}
	if err := r.cleanupClient.Create(ctx, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// The broker authenticates token requests with its own provider-workspace config.
// It never uses the expired runtime token or the consumer APIExport endpoint.
func (r *workspaceRuntime) refreshCleanupCredential(ctx context.Context, secret *corev1.Secret, record *cleanupRecord) error {
	expires, err := time.Parse(time.RFC3339, string(secret.Data[clustersv1alpha1.SecretKeyExpirationTimestamp]))
	if err == nil && time.Until(expires) > r.tokenLifetime/3 && len(secret.Data[clustersv1alpha1.SecretKeyKubeconfig]) != 0 {
		return nil
	}
	factory := r.clientsetForConfig
	if factory == nil {
		factory = func(cfg *rest.Config) (kubernetes.Interface, error) { return kubernetes.NewForConfig(cfg) }
	}
	issuer, err := factory(rest.CopyConfig(r.cleanupConfig))
	if err != nil {
		return err
	}
	seconds := int64(r.tokenLifetime.Seconds())
	token, err := issuer.CoreV1().ServiceAccounts(cleanupNamespace).CreateToken(ctx, secret.Name, &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &seconds}}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("renew external cleanup credential: %w", err)
	}
	if token.Status.Token == "" || !token.Status.ExpirationTimestamp.After(time.Now()) {
		return fmt.Errorf("cleanup token request returned no usable credential")
	}
	cfg, err := directWorkspaceConfig(r.cleanupConfig, multicluster.ClusterName(record.Account))
	if err != nil {
		return err
	}
	data, err := clusteraccess.CreateTokenKubeconfig("cleanup", cfg.Host, cfg.CAData, token.Status.Token)
	if err != nil {
		return err
	}
	secret.Data[clustersv1alpha1.SecretKeyKubeconfig] = data
	secret.Data[clustersv1alpha1.SecretKeyExpirationTimestamp] = []byte(token.Status.ExpirationTimestamp.UTC().Format(time.RFC3339))
	secret.Data[clustersv1alpha1.SecretKeyCreationTimestamp] = []byte(time.Now().UTC().Format(time.RFC3339))
	return r.cleanupClient.Update(ctx, secret)
}

func (r *workspaceRuntime) validateCleanupCredential(ctx context.Context, secret *corev1.Secret, record *cleanupRecord) error {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(secret.Data[clustersv1alpha1.SecretKeyKubeconfig])
	if err != nil {
		return err
	}
	factory := r.clientsetForConfig
	if factory == nil {
		factory = func(cfg *rest.Config) (kubernetes.Interface, error) { return kubernetes.NewForConfig(cfg) }
	}
	scoped, err := factory(cfg)
	if err != nil {
		return err
	}
	for _, resource := range []string{record.Resource.Resource, record.Resource.Resource + "/status"} {
		for _, verb := range []string{verbUpdate, verbPatch} {
			parts := strings.SplitN(resource, "/", 2)
			attrs := &authorizationv1.ResourceAttributes{Group: record.Resource.Group, Version: record.Resource.Version, Resource: parts[0], Verb: verb}
			if len(parts) == 2 {
				attrs.Subresource = parts[1]
			}
			review, err := scoped.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: attrs}}, metav1.CreateOptions{})
			if err != nil {
				return err
			}
			if !review.Status.Allowed {
				return fmt.Errorf("cleanup identity cannot %s %s", verb, resource)
			}
		}
	}
	scopedClient, err := r.cleanupAccountClient(record, secret.Data[clustersv1alpha1.SecretKeyKubeconfig])
	if err != nil {
		return err
	}
	list := &unstructured.UnstructuredList{}
	gvk := schema.GroupVersionKind(record.Provider.Resource)
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	return scopedClient.List(ctx, list, client.Limit(1))
}

func (r *workspaceRuntime) publishCleanupCredential(ctx context.Context, source *corev1.Secret, record *cleanupRecord) error {
	ar := &record.Request
	c := r.platform.Client()
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: ar.Namespace}, ns); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}
	if ns.UID != record.RuntimeUID {
		return fmt.Errorf("cleanup runtime namespace UID changed")
	}
	if !ns.DeletionTimestamp.IsZero() {
		return nil
	}
	liveRequest := &clustersv1alpha1.AccessRequest{}
	requestErr := c.Get(ctx, client.ObjectKeyFromObject(ar), liveRequest)
	if requestErr != nil && !apierrors.IsNotFound(requestErr) {
		return requestErr
	}
	if requestErr == nil && liveRequest.UID != ar.UID {
		return fmt.Errorf("cleanup AccessRequest UID changed")
	}
	target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ar.Name + "-kubeconfig", Namespace: ar.Namespace}}
	if requestErr == nil {
		if _, err := controllerutil.CreateOrUpdate(ctx, c, target, func() error {
			if target.ResourceVersion != "" && !metav1.IsControlledBy(target, ar) {
				return fmt.Errorf("refusing to adopt cleanup credential")
			}
			target.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ar, clustersv1alpha1.GroupVersion.WithKind("AccessRequest"))}
			target.Data = map[string][]byte{}
			for _, key := range []string{clustersv1alpha1.SecretKeyKubeconfig, clustersv1alpha1.SecretKeyExpirationTimestamp, clustersv1alpha1.SecretKeyCreationTimestamp} {
				target.Data[key] = source.Data[key]
			}
			return nil
		}); err != nil {
			return err
		}
	}
	// Refresh the existing shared registration even after engagement stops.
	if record.Provider.RegistrationNamespace == "" {
		return nil
	}
	registration := &corev1.Secret{}
	key := client.ObjectKey{Namespace: record.Provider.RegistrationNamespace, Name: workspaceControlPlaneNamespace(multicluster.ClusterName(record.Account))}
	if err := c.Get(ctx, key, registration); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}
	if !metav1.IsControlledBy(registration, ns) || registration.Annotations[registrationAccessRequestAnnotation] != ar.Name {
		return fmt.Errorf("cleanup registration ownership does not match")
	}
	registration.Data = map[string][]byte{clustersv1alpha1.SecretKeyKubeconfig: source.Data[clustersv1alpha1.SecretKeyKubeconfig]}
	return c.Update(ctx, registration)
}

func cleanupRESTMapper(record *cleanupRecord) meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{record.Resource.GroupVersion(), corev1.SchemeGroupVersion, kcpapisv1alpha1.SchemeGroupVersion, kcpcorev1alpha1.SchemeGroupVersion})
	scope := meta.RESTScopeRoot
	if record.Namespaced {
		scope = meta.RESTScopeNamespace
	}
	if record.Resource.Resource != "" {
		mapper.AddSpecific(schema.GroupVersionKind(record.Provider.Resource), record.Resource, record.Resource, scope)
	}
	mapper.Add(corev1.SchemeGroupVersion.WithKind("Namespace"), meta.RESTScopeRoot)
	mapper.Add(corev2alpha1.GroupVersion.WithKind("ControlPlane"), meta.RESTScopeNamespace)
	mapper.Add(kcpapisv1alpha1.SchemeGroupVersion.WithKind("APIBinding"), meta.RESTScopeRoot)
	mapper.Add(kcpcorev1alpha1.SchemeGroupVersion.WithKind("LogicalCluster"), meta.RESTScopeRoot)
	mapper.Add(rbacv1.SchemeGroupVersion.WithKind("ClusterRole"), meta.RESTScopeRoot)
	mapper.Add(rbacv1.SchemeGroupVersion.WithKind("ClusterRoleBinding"), meta.RESTScopeRoot)
	return mapper
}

func (r *workspaceRuntime) cleanupAccountClient(record *cleanupRecord, credential []byte) (client.Client, error) {
	var cfg *rest.Config
	var err error
	if credential == nil {
		cfg, err = directWorkspaceConfig(r.cleanupConfig, multicluster.ClusterName(record.Account))
	} else {
		cfg, err = clientcmd.RESTConfigFromKubeConfig(credential)
	}
	if err != nil {
		return nil, err
	}
	// Reject a malformed persisted credential before it can address another account.
	endpoint, err := directWorkspaceConfig(r.cleanupConfig, multicluster.ClusterName(record.Account))
	if err != nil || cfg.Host != endpoint.Host {
		return nil, fmt.Errorf("cleanup credential addresses another account")
	}
	return r.newCleanupNativeClient(cfg, record)
}

func (r *workspaceRuntime) newCleanupNativeClient(cfg *rest.Config, record *cleanupRecord) (client.Client, error) {
	factory := r.cleanupNewClient
	if factory == nil {
		factory = client.New
	}
	if record == nil {
		record = &cleanupRecord{}
	}
	return factory(cfg, client.Options{Scheme: r.cleanupClient.Scheme(), Mapper: cleanupRESTMapper(record)})
}

func cleanupServicesGone(ctx context.Context, c client.Client, record *cleanupRecord) error {
	list := &unstructured.UnstructuredList{}
	gvk := schema.GroupVersionKind(record.Provider.Resource)
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err := c.List(ctx, list, client.Limit(1)); err != nil {
		return fmt.Errorf("verify cleanup service access: %w", err)
	}
	if len(list.Items) != 0 {
		return fmt.Errorf("waiting for %s status and finalizer updates: %w", gvk.Kind, errCleanupServicesRemain)
	}
	return nil
}

func (r *workspaceRuntime) reconcileCleanupRecords(ctx context.Context) error {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	list := &corev1.SecretList{}
	if err := r.cleanupClient.List(ctx, list, client.InNamespace(cleanupNamespace), client.MatchingLabels{cleanupRecordLabel: labelValueTrue}); err != nil {
		return err
	}
	var errs []error
	for i := range list.Items {
		secret := &list.Items[i]
		record, err := readCleanupRecord(secret)
		if err == nil {
			err = r.reconcileCleanupRecord(ctx, secret, record)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("cleanup record %s: %w", secret.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (r *workspaceRuntime) reconcileCleanupRecord(ctx context.Context, secret *corev1.Secret, record *cleanupRecord) error {
	account, err := r.cleanupAccountClient(record, nil)
	if err != nil {
		return err
	}
	if cleanupFinalPhase(record.Phase) {
		return r.revokeFinalCleanupRecord(ctx, account, secret, record)
	}
	logical := &kcpcorev1alpha1.LogicalCluster{}
	err = account.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical)
	if apierrors.IsNotFound(err) || (err == nil && logical.UID != record.AccountUID) {
		// The original account no longer exists. Never touch its replacement.
		return r.deleteExternalCleanupIdentity(ctx, secret)
	}
	if err != nil {
		return err
	}
	draining, err := r.cleanupRequested(ctx, account, record, logical)
	if err != nil {
		return err
	}
	if draining && record.Phase == cleanupActive {
		record.Phase = cleanupDraining
		if err := r.saveCleanupRecord(ctx, secret, record); err != nil {
			return err
		}
	}
	if err := r.refreshCleanupCredential(ctx, secret, record); err != nil {
		return err
	}
	if !record.Ready {
		if err := r.validateCleanupCredential(ctx, secret, record); err != nil {
			return err
		}
		record.Ready = true
		if err := r.saveCleanupRecord(ctx, secret, record); err != nil {
			return err
		}
	}
	if record.Phase == cleanupActive {
		return r.publishCleanupCredential(ctx, secret, record)
	}

	scoped, err := r.cleanupAccountClient(record, secret.Data[clustersv1alpha1.SecretKeyKubeconfig])
	if err != nil {
		return err
	}
	if err := cleanupServicesGone(ctx, scoped, record); err != nil {
		if errors.Is(err, errCleanupServicesRemain) {
			return errors.Join(err, r.publishCleanupCredential(ctx, secret, record))
		}
		return err
	}
	// Persist the irrevocable phase first. Restart must not renew or regrant here.
	record.Phase = cleanupRevoking
	if err := r.saveCleanupRecord(ctx, secret, record); err != nil {
		return err
	}
	return r.revokeFinalCleanupRecord(ctx, account, secret, record)
}

func (r *workspaceRuntime) cleanupRequested(ctx context.Context, account client.Client, record *cleanupRecord, logical *kcpcorev1alpha1.LogicalCluster) (bool, error) {
	ar := &clustersv1alpha1.AccessRequest{}
	requestErr := r.platform.Client().Get(ctx, client.ObjectKeyFromObject(&record.Request), ar)
	if requestErr != nil && !apierrors.IsNotFound(requestErr) {
		return false, requestErr
	}
	binding := &kcpapisv1alpha1.APIBinding{}
	bindingErr := account.Get(ctx, client.ObjectKey{Name: record.Binding.Name}, binding)
	if bindingErr != nil && !apierrors.IsNotFound(bindingErr) {
		return false, bindingErr
	}
	runtimeNamespace := &corev1.Namespace{}
	namespaceErr := r.platform.Client().Get(ctx, client.ObjectKey{Name: record.Request.Namespace}, runtimeNamespace)
	if namespaceErr != nil && !apierrors.IsNotFound(namespaceErr) {
		return false, namespaceErr
	}
	return apierrors.IsNotFound(namespaceErr) || runtimeNamespace.UID != record.RuntimeUID || !runtimeNamespace.DeletionTimestamp.IsZero() || !logical.DeletionTimestamp.IsZero() || apierrors.IsNotFound(requestErr) || !ar.DeletionTimestamp.IsZero() || ar.UID != record.Request.UID || apierrors.IsNotFound(bindingErr) || !binding.DeletionTimestamp.IsZero() || binding.UID != record.Binding.UID, nil
}

// The runtime persists service completion before it tears down the runtime.
// This checkpoint remains readable after KCP removes the account's RBAC.
func (r *workspaceRuntime) markCleanupServicesFinal(ctx context.Context, namespace string, workspace client.Client) error {
	if r.cleanupClient == nil {
		return nil
	}
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	list := &corev1.SecretList{}
	if err := r.cleanupClient.List(ctx, list, client.InNamespace(cleanupNamespace), client.MatchingLabels{cleanupRecordLabel: labelValueTrue}); err != nil {
		return err
	}
	records := map[int]*cleanupRecord{}
	for i := range list.Items {
		record, err := readCleanupRecord(&list.Items[i])
		if err != nil {
			return err
		}
		if record.Request.Namespace != namespace || cleanupFinalPhase(record.Phase) {
			continue
		}
		if err := cleanupServicesGone(ctx, workspace, record); err != nil {
			return err
		}
		records[i] = record
	}
	for i, record := range records {
		record.Phase = cleanupServicesFinal
		if err := r.saveCleanupRecord(ctx, &list.Items[i], record); err != nil {
			return err
		}
	}
	return nil
}

func (r *workspaceRuntime) revokeFinalCleanupRecord(ctx context.Context, account client.Client, secret *corev1.Secret, record *cleanupRecord) error {
	// Invalidate the identity before grant deletion. Failed account writes must
	// retain only retry metadata, never a renewable credential.
	if err := r.prepareCleanupGrantRetry(ctx, secret, record); err != nil {
		return err
	}
	logical := &kcpcorev1alpha1.LogicalCluster{}
	err := account.Get(ctx, client.ObjectKey{Name: kcpcorev1alpha1.LogicalClusterName}, logical)
	if apierrors.IsForbidden(err) || apierrors.IsNotFound(err) || err == nil && logical.UID != record.AccountUID {
		return r.completeCleanupRecord(ctx, secret, record)
	}
	if err != nil {
		return err
	}
	return r.revokeCleanupRecord(ctx, account, secret, record)
}

func (r *workspaceRuntime) revokeCleanupRecord(ctx context.Context, account client.Client, secret *corev1.Secret, record *cleanupRecord) error {
	objects := cleanupGrants(record, r.cleanupCluster)
	// Remove dependents before their roles. Keep the record on every failed write.
	for i := len(objects) - 1; i >= 0; i-- {
		object := objects[i]
		if err := account.Get(ctx, client.ObjectKeyFromObject(object), object); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
		if object.GetLabels()[cleanupRecordLabel] != secret.Name {
			return fmt.Errorf("refusing to revoke foreign cleanup grant")
		}
		if err := account.Delete(ctx, object); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return r.completeCleanupRecord(ctx, secret, record)
}

func (r *workspaceRuntime) deleteExternalCleanupIdentity(ctx context.Context, secret *corev1.Secret) error {
	record, err := readCleanupRecord(secret)
	if err != nil {
		return err
	}
	if err := r.prepareCleanupGrantRetry(ctx, secret, record); err != nil {
		return err
	}
	return r.completeCleanupRecord(ctx, secret, record)
}

func (r *workspaceRuntime) prepareCleanupGrantRetry(ctx context.Context, secret *corev1.Secret, record *cleanupRecord) error {
	if err := r.deleteExternalCleanupServiceAccount(ctx, secret); err != nil {
		return err
	}
	if record.Phase == cleanupGrantRetry && len(secret.Data) == 1 {
		return nil
	}
	record.Phase = cleanupGrantRetry
	secret.Data = map[string][]byte{}
	return r.saveCleanupRecord(ctx, secret, record)
}

func (r *workspaceRuntime) deleteExternalCleanupServiceAccount(ctx context.Context, secret *corev1.Secret) error {
	sa := &corev1.ServiceAccount{}
	key := client.ObjectKey{Namespace: cleanupNamespace, Name: secret.Name}
	if err := r.cleanupClient.Get(ctx, key, sa); err == nil {
		if sa.Labels[cleanupRecordLabel] != secret.Name {
			return fmt.Errorf("refusing to revoke foreign cleanup identity")
		}
		if err := r.cleanupClient.Delete(ctx, sa); client.IgnoreNotFound(err) != nil {
			return err
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// Retain the outcome on the platform request before deleting its broker record.
// The request finalizer can then resume after an independent broker restart.
func (r *workspaceRuntime) completeCleanupRecord(ctx context.Context, secret *corev1.Secret, record *cleanupRecord) error {
	ar := &clustersv1alpha1.AccessRequest{}
	err := r.platform.Client().Get(ctx, client.ObjectKeyFromObject(&record.Request), ar)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil && ar.UID == record.Request.UID && ar.Annotations[cleanupCompleteAnnotation] != string(ar.UID) {
		if ar.Annotations == nil {
			ar.Annotations = map[string]string{}
		}
		ar.Annotations[cleanupCompleteAnnotation] = string(ar.UID)
		if err := r.platform.Client().Update(ctx, ar); err != nil {
			return err
		}
	}
	return client.IgnoreNotFound(r.cleanupClient.Delete(ctx, secret))
}

func (r *workspaceRuntime) finalizeCleanupAccess(ctx context.Context, ar *clustersv1alpha1.AccessRequest) (bool, error) {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	secret := &corev1.Secret{}
	if err := r.cleanupClient.Get(ctx, client.ObjectKey{Namespace: cleanupNamespace, Name: cleanupIdentityName(ar)}, secret); apierrors.IsNotFound(err) {
		return ar.UID != "" && ar.Annotations[cleanupCompleteAnnotation] == string(ar.UID), nil
	} else if err != nil {
		return true, err
	}
	record, err := readCleanupRecord(secret)
	if err != nil {
		return true, err
	}
	if record.Phase == cleanupActive {
		record.Phase = cleanupDraining
		if err := r.saveCleanupRecord(ctx, secret, record); err != nil {
			return true, err
		}
	}
	return true, r.reconcileCleanupRecord(ctx, secret, record)
}

// Check before finalization removes the requests and their broker records.
// Mixed or legacy requests still need the generic account access sweep.
func (r *workspaceRuntime) cleanupRequestsUseBroker(ctx context.Context, namespace string) (bool, error) {
	if r.cleanupClient == nil {
		return false, nil
	}
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	requests := &clustersv1alpha1.AccessRequestList{}
	if err := r.platform.Client().List(ctx, requests, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	if len(requests.Items) == 0 {
		return false, nil
	}
	for i := range requests.Items {
		ar := &requests.Items[i]
		if ar.DeletionTimestamp.IsZero() || ar.UID == "" {
			return false, nil
		}
		if ar.Annotations[cleanupCompleteAnnotation] == string(ar.UID) {
			continue
		}
		secret := &corev1.Secret{}
		if err := r.cleanupClient.Get(ctx, client.ObjectKey{Namespace: cleanupNamespace, Name: cleanupIdentityName(ar)}, secret); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		record, err := readCleanupRecord(secret)
		if err != nil {
			return false, err
		}
		if record.Request.UID != ar.UID || record.Request.Namespace != ar.Namespace || record.Request.Name != ar.Name {
			return false, nil
		}
	}
	return true, nil
}

func (r *workspaceRuntime) cleanupProvidersReady(ctx context.Context, namespace string, deleting bool) error {
	if r.cleanupClient == nil {
		return nil
	}
	list := &corev1.SecretList{}
	if err := r.cleanupClient.List(ctx, list, client.InNamespace(cleanupNamespace), client.MatchingLabels{cleanupRecordLabel: labelValueTrue}); err != nil {
		return err
	}
	ready := map[string]bool{}
	for i := range list.Items {
		record, err := readCleanupRecord(&list.Items[i])
		if err != nil {
			return err
		}
		if record.Request.Namespace == namespace && record.Ready && (record.Phase == cleanupActive || deleting && record.Phase == cleanupDraining) {
			ready[record.Provider.Name] = true
		}
	}
	for _, provider := range r.providers {
		if !ready[provider.Name] {
			return fmt.Errorf("provider %s has no durable cleanup access; retain services and restore access preparation before teardown", provider.Name)
		}
	}
	return nil
}

func (r *workspaceRuntime) releaseCleanupAccessRequest(ctx context.Context, ar *clustersv1alpha1.AccessRequest) error {
	// Broker completion can update this request independently of the caller.
	current := &clustersv1alpha1.AccessRequest{}
	if err := r.platform.Client().Get(ctx, client.ObjectKeyFromObject(ar), current); err != nil {
		return client.IgnoreNotFound(err)
	}
	if current.UID != ar.UID {
		return fmt.Errorf("cleanup AccessRequest UID changed before finalizer release")
	}
	ar = current
	if err := r.deleteWorkspaceCredential(ctx, ar); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(ar, workspaceAccessFinalizer)
	return client.IgnoreNotFound(r.platform.Client().Update(ctx, ar))
}
