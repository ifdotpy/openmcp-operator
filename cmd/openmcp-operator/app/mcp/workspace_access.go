package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/openmcp-project/controller-utils/pkg/clusteraccess"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
)

const workspaceAccessOwnerLabel = "workspace.openmcp.cloud/access-uid"

func workspaceAccessNamespace(ar *clustersv1alpha1.AccessRequest) string {
	return "openmcp-access-" + string(ar.UID)
}

func (r *workspaceRuntime) reconcileAccessRequests(ctx context.Context, namespace string, workspaceClient client.Client, workspaceConfig *rest.Config, bindingOwner metav1.OwnerReference) error {
	platform := r.platform.Client()
	list := &clustersv1alpha1.AccessRequestList{}
	if err := platform.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list AccessRequests: %w", err)
	}
	for i := range list.Items {
		ar := &list.Items[i]
		if !r.ownsWorkspaceRequest(ar.Labels[apiconst.ManagedByLabel]) {
			continue
		}
		if !ar.DeletionTimestamp.IsZero() {
			if !controllerutil.ContainsFinalizer(ar, workspaceAccessFinalizer) {
				continue
			}
			done, err := revokeWorkspaceAccess(ctx, workspaceClient, ar)
			if err != nil {
				return err
			}
			if done && controllerutil.RemoveFinalizer(ar, workspaceAccessFinalizer) {
				if err := platform.Update(ctx, ar); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
			continue
		}
		if ar.Spec.OIDC != nil {
			continue
		}
		if ar.Spec.Token == nil {
			continue
		}
		if controllerutil.AddFinalizer(ar, workspaceAccessFinalizer) {
			if err := platform.Update(ctx, ar); err != nil {
				return err
			}
		}
		resolved, err := r.resolveAccessRequest(ctx, ar, namespace)
		if err != nil {
			return err
		}
		if !resolved {
			continue
		}
		if err := ensureWorkspaceAccess(ctx, workspaceClient, ar, bindingOwner); err != nil {
			return err
		}
		secretName := ar.Name + "-kubeconfig"
		secret := &corev1.Secret{}
		err = platform.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, secret)
		rotate := apierrors.IsNotFound(err)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil {
			expires, parseErr := time.Parse(time.RFC3339, string(secret.Data[clustersv1alpha1.SecretKeyExpirationTimestamp]))
			rotate = parseErr != nil || time.Until(expires) < r.tokenLifetime/3
		}
		if rotate {
			issuerToken, err := workspaceIssuerToken(ctx, workspaceClient, ar)
			if err != nil {
				return err
			}
			clientsetForConfig := r.clientsetForConfig
			if clientsetForConfig == nil {
				clientsetForConfig = func(config *rest.Config) (kubernetes.Interface, error) {
					return kubernetes.NewForConfig(config)
				}
			}
			workspaceClientset, err := clientsetForConfig(workspaceIssuerConfig(workspaceConfig, issuerToken))
			if err != nil {
				return fmt.Errorf("create workspace credential issuer client: %w", err)
			}
			kubeconfig, expires, err := mintWorkspaceCredential(ctx, workspaceClientset, workspaceConfig, ar, r.tokenLifetime)
			if err != nil {
				return fmt.Errorf("mint workspace credential for %s: %w", ar.Name, err)
			}
			secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace}}
			if _, err := controllerutil.CreateOrUpdate(ctx, platform, secret, func() error {
				secret.Type = corev1.SecretTypeOpaque
				secret.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ar, clustersv1alpha1.GroupVersion.WithKind("AccessRequest"))}
				secret.Data = map[string][]byte{
					clustersv1alpha1.SecretKeyKubeconfig:          kubeconfig,
					clustersv1alpha1.SecretKeyExpirationTimestamp: []byte(expires.UTC().Format(time.RFC3339)),
					clustersv1alpha1.SecretKeyCreationTimestamp:   []byte(time.Now().UTC().Format(time.RFC3339)),
				}
				return nil
			}); err != nil {
				return err
			}
		}
		old := ar.DeepCopy()
		ar.Status.Phase = clustersv1alpha1.REQUEST_GRANTED
		ar.Status.ObservedGeneration = ar.Generation
		ar.Status.SecretRef = &commonapi.LocalObjectReference{Name: secretName}
		if err := platform.Status().Patch(ctx, ar, client.MergeFrom(old)); err != nil {
			return err
		}
	}
	return nil
}

func (r *workspaceRuntime) resolveAccessRequest(ctx context.Context, ar *clustersv1alpha1.AccessRequest, namespace string) (bool, error) {
	if ar.Spec.ClusterRef != nil {
		refNamespace := ar.Spec.ClusterRef.Namespace
		if refNamespace == "" {
			refNamespace = namespace
		}
		matchesWorkspace := ar.Spec.ClusterRef.Name == workspaceClusterName && refNamespace == namespace
		if ar.Labels[clustersv1alpha1.ProviderLabel] == workspaceProviderName {
			if !matchesWorkspace {
				return false, fmt.Errorf("access request %s is assigned to the workspace provider but references another cluster", ar.Name)
			}
			return true, nil
		}
		if !matchesWorkspace {
			return false, nil
		}
	}
	old := ar.DeepCopy()
	if ar.Spec.ClusterRef == nil {
		if ar.Spec.RequestRef == nil {
			return false, fmt.Errorf("access request %s has no cluster reference", ar.Name)
		}
		request := &clustersv1alpha1.ClusterRequest{}
		if err := r.platform.Client().Get(ctx, ar.Spec.RequestRef.NamespacedName(), request); err != nil {
			return false, err
		}
		if !request.Status.IsGranted() || request.Status.Cluster == nil {
			return false, nil
		}
		ref := *request.Status.Cluster
		ar.Spec.ClusterRef = &ref
	}
	if ar.Spec.ClusterRef.Name != workspaceClusterName || (ar.Spec.ClusterRef.Namespace != "" && ar.Spec.ClusterRef.Namespace != namespace) {
		return false, nil
	}
	if ar.Labels == nil {
		ar.Labels = map[string]string{}
	}
	ar.Labels[clustersv1alpha1.ProviderLabel] = workspaceProviderName
	ar.Labels[clustersv1alpha1.ProfileLabel] = workspaceClusterProfile
	if ar.Spec.ClusterRef.Namespace == "" {
		ar.Spec.ClusterRef.Namespace = namespace
	}
	return true, r.platform.Client().Patch(ctx, ar, client.MergeFrom(old))
}

func workspaceGrantObjects(ar *clustersv1alpha1.AccessRequest) ([]client.Object, error) {
	if ar.UID == "" || ar.Spec.Token == nil {
		return nil, fmt.Errorf("token AccessRequest UID is required")
	}
	data, err := json.Marshal(ar.Spec.Token)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	prefix := "openmcp-" + string(ar.UID) + "-" + hex.EncodeToString(digest[:4])
	owner := string(ar.UID)
	subjects := []rbacv1.Subject{{Kind: serviceAccountKind, Name: controllerName, Namespace: workspaceAccessNamespace(ar)}}
	var out []client.Object
	bind := func(name, namespace string, ref rbacv1.RoleRef) {
		metadata := metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{workspaceAccessOwnerLabel: owner}}
		if namespace == "" {
			out = append(out, &rbacv1.ClusterRoleBinding{ObjectMeta: metadata, RoleRef: ref, Subjects: subjects})
		} else {
			out = append(out, &rbacv1.RoleBinding{ObjectMeta: metadata, RoleRef: ref, Subjects: subjects})
		}
	}
	for i, permission := range ar.Spec.Token.Permissions {
		name := permission.Name
		if name == "" {
			name = fmt.Sprintf("%s-p%d", prefix, i)
		}
		kind := roleKind
		if permission.Namespace == "" {
			kind = clusterRoleKind
			out = append(out, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{workspaceAccessOwnerLabel: owner}}, Rules: permission.Rules})
		} else {
			out = append(out, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: permission.Namespace, Labels: map[string]string{workspaceAccessOwnerLabel: owner}}, Rules: permission.Rules})
		}
		bind(fmt.Sprintf("%s-p%d", prefix, i), permission.Namespace, rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: kind, Name: name})
	}
	for i, ref := range ar.Spec.Token.RoleRefs {
		if ref.Name == "" || (ref.Kind != roleKind && ref.Kind != clusterRoleKind) {
			return nil, fmt.Errorf("invalid role reference")
		}
		bind(fmt.Sprintf("%s-r%d", prefix, i), ref.Namespace, rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: ref.Kind, Name: ref.Name})
	}
	issuerRole := prefix + "-issuer"
	accessNamespace := workspaceAccessNamespace(ar)
	out = append(out,
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: issuerRole, Namespace: accessNamespace, Labels: map[string]string{workspaceAccessOwnerLabel: owner}},
			// Kubernetes authorization cannot constrain create by resourceNames:
			// the target ServiceAccount name is carried by the token subresource URL.
			// The issuer remains confined to this request's isolated namespace.
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"}, Verbs: []string{verbCreate}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: issuerRole, Namespace: accessNamespace, Labels: map[string]string{workspaceAccessOwnerLabel: owner}},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: roleKind, Name: issuerRole},
			Subjects:   []rbacv1.Subject{{Kind: serviceAccountKind, Name: credentialIssuerName, Namespace: accessNamespace}},
		},
	)
	return out, nil
}

func workspaceObjectID(obj client.Object) string {
	return fmt.Sprintf("%T/%s/%s", obj, obj.GetNamespace(), obj.GetName())
}

func workspaceOwnedUpsert(ctx context.Context, c client.Client, object client.Object, owner string) error {
	desired := object.DeepCopyObject().(client.Object)
	_, err := controllerutil.CreateOrUpdate(ctx, c, object, func() error {
		if object.GetUID() != "" && object.GetLabels()[workspaceAccessOwnerLabel] != owner {
			return fmt.Errorf("refusing to adopt foreign %s", workspaceObjectID(object))
		}
		object.SetLabels(desired.GetLabels())
		object.SetOwnerReferences(desired.GetOwnerReferences())
		switch current := object.(type) {
		case *rbacv1.Role:
			current.Rules = desired.(*rbacv1.Role).Rules
		case *rbacv1.ClusterRole:
			current.Rules = desired.(*rbacv1.ClusterRole).Rules
		case *rbacv1.RoleBinding:
			want := desired.(*rbacv1.RoleBinding)
			current.RoleRef, current.Subjects = want.RoleRef, want.Subjects
		case *rbacv1.ClusterRoleBinding:
			want := desired.(*rbacv1.ClusterRoleBinding)
			current.RoleRef, current.Subjects = want.RoleRef, want.Subjects
		case *corev1.Secret:
			want := desired.(*corev1.Secret)
			current.Type = want.Type
			current.Annotations = want.Annotations
		}
		return nil
	})
	return err
}

func pruneWorkspaceGrants(ctx context.Context, c client.Client, owner string, keep map[string]bool) error {
	for _, list := range []client.ObjectList{&rbacv1.RoleBindingList{}, &rbacv1.ClusterRoleBindingList{}, &rbacv1.RoleList{}, &rbacv1.ClusterRoleList{}} {
		if err := c.List(ctx, list, client.MatchingLabels{workspaceAccessOwnerLabel: owner}); err != nil {
			return err
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			return err
		}
		for _, item := range items {
			object := item.(client.Object)
			if keep[workspaceObjectID(object)] {
				continue
			}
			if err := c.Delete(ctx, object); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
	}
	return nil
}

func ensureWorkspaceAccess(ctx context.Context, c client.Client, ar *clustersv1alpha1.AccessRequest, bindingOwner metav1.OwnerReference) error {
	objects, err := workspaceGrantObjects(ar)
	if err != nil {
		if ar.UID != "" {
			_ = pruneWorkspaceGrants(ctx, c, string(ar.UID), nil)
		}
		return err
	}
	owner := string(ar.UID)
	keep := map[string]bool{}
	for _, object := range objects {
		object.SetOwnerReferences([]metav1.OwnerReference{bindingOwner})
		keep[workspaceObjectID(object)] = true
	}
	if err := pruneWorkspaceGrants(ctx, c, owner, keep); err != nil {
		return err
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: workspaceAccessNamespace(ar), Labels: map[string]string{workspaceAccessOwnerLabel: owner}, OwnerReferences: []metav1.OwnerReference{bindingOwner}}}
	if err := workspaceOwnedUpsert(ctx, c, ns, owner); err != nil {
		return err
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: controllerName, Namespace: ns.Name, Labels: map[string]string{workspaceAccessOwnerLabel: owner}, OwnerReferences: []metav1.OwnerReference{bindingOwner}}}
	if err := workspaceOwnedUpsert(ctx, c, sa, owner); err != nil {
		return err
	}
	issuerServiceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: credentialIssuerName, Namespace: ns.Name, Labels: map[string]string{workspaceAccessOwnerLabel: owner}, OwnerReferences: []metav1.OwnerReference{bindingOwner}}}
	if err := workspaceOwnedUpsert(ctx, c, issuerServiceAccount, owner); err != nil {
		return err
	}
	issuerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            workspaceIssuerSecretName(ar),
			Namespace:       ns.Name,
			Labels:          map[string]string{workspaceAccessOwnerLabel: owner},
			Annotations:     map[string]string{corev1.ServiceAccountNameKey: credentialIssuerName},
			OwnerReferences: []metav1.OwnerReference{bindingOwner},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	}
	if err := workspaceOwnedUpsert(ctx, c, issuerSecret, owner); err != nil {
		return err
	}
	for _, permission := range ar.Spec.Token.Permissions {
		if permission.Namespace == "" {
			continue
		}
		target := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: permission.Namespace}}
		if err := c.Get(ctx, client.ObjectKeyFromObject(target), target); apierrors.IsNotFound(err) && !permission.DisableAutomaticNamespaceCreation {
			if err := c.Create(ctx, target); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		} else if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	for _, object := range objects {
		if err := workspaceOwnedUpsert(ctx, c, object, owner); err != nil {
			return err
		}
	}
	return nil
}

func workspaceIssuerSecretName(ar *clustersv1alpha1.AccessRequest) string {
	return "issuer-token-" + string(ar.UID)
}

func workspaceIssuerToken(ctx context.Context, c client.Client, ar *clustersv1alpha1.AccessRequest) (string, error) {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: workspaceAccessNamespace(ar), Name: workspaceIssuerSecretName(ar)}
	if err := c.Get(ctx, key, secret); err != nil {
		return "", fmt.Errorf("read workspace credential issuer token: %w", err)
	}
	token := string(secret.Data[corev1.ServiceAccountTokenKey])
	if token == "" {
		return "", fmt.Errorf("workspace credential issuer token is not ready")
	}
	return token, nil
}

func workspaceIssuerConfig(base *rest.Config, token string) *rest.Config {
	config := rest.CopyConfig(base)
	config.BearerToken = token
	config.BearerTokenFile = ""
	config.Username = ""
	config.Password = ""
	config.CertData = nil
	config.KeyData = nil
	config.CertFile = ""
	config.KeyFile = ""
	config.ExecProvider = nil
	config.AuthProvider = nil
	return config
}

func revokeWorkspaceAccess(ctx context.Context, c client.Client, ar *clustersv1alpha1.AccessRequest) (bool, error) {
	owner := string(ar.UID)
	if err := pruneWorkspaceGrants(ctx, c, owner, nil); err != nil {
		return false, err
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: workspaceAccessNamespace(ar)}}
	if err := c.Delete(ctx, ns); client.IgnoreNotFound(err) != nil {
		return false, err
	}
	return true, nil
}

func mintWorkspaceCredential(ctx context.Context, c kubernetes.Interface, cfg *rest.Config, ar *clustersv1alpha1.AccessRequest, lifetime time.Duration) ([]byte, time.Time, error) {
	seconds := int64(lifetime.Seconds())
	token, err := c.CoreV1().ServiceAccounts(workspaceAccessNamespace(ar)).CreateToken(ctx, controllerName, &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &seconds}}, metav1.CreateOptions{})
	if err != nil {
		return nil, time.Time{}, err
	}
	if token.Status.Token == "" || !token.Status.ExpirationTimestamp.After(time.Now()) {
		return nil, time.Time{}, fmt.Errorf("token request returned no usable credential")
	}
	config, err := clusteraccess.CreateTokenKubeconfig("provider", cfg.Host, cfg.CAData, token.Status.Token)
	return config, token.Status.ExpirationTimestamp.Time, err
}

func (r *workspaceRuntime) cleanupWorkspaceAccess(name multicluster.ClusterName, c client.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var cleanupErrors []error
	for _, list := range []client.ObjectList{&rbacv1.RoleBindingList{}, &rbacv1.ClusterRoleBindingList{}, &rbacv1.RoleList{}, &rbacv1.ClusterRoleList{}} {
		if err := c.List(ctx, list); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("list workspace access grants: %w", err))
			continue
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("read workspace access grants: %w", err))
			continue
		}
		for _, item := range items {
			obj := item.(client.Object)
			if _, ok := obj.GetLabels()[workspaceAccessOwnerLabel]; !ok {
				continue
			}
			if err := c.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove workspace access grant %s: %w", workspaceObjectID(obj), err))
			}
		}
	}
	namespaces := &corev1.NamespaceList{}
	if err := c.List(ctx, namespaces); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("list workspace access namespaces: %w", err))
	} else {
		for i := range namespaces.Items {
			ns := &namespaces.Items[i]
			if _, ok := ns.Labels[workspaceAccessOwnerLabel]; !ok {
				continue
			}
			if err := c.Delete(ctx, ns); client.IgnoreNotFound(err) != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove workspace access namespace %q: %w", ns.Name, err))
			}
		}
	}
	workspaceNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: workspaceControlPlaneNamespace(name)}}
	if err := c.Delete(ctx, workspaceNamespace); client.IgnoreNotFound(err) != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("remove workspace control-plane namespace %q: %w", workspaceNamespace.Name, err))
	}
	return errors.Join(cleanupErrors...)
}
