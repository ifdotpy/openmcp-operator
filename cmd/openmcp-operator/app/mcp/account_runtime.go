package mcp

import (
	"context"
	"fmt"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"

	controllerclusters "github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

const (
	accountClusterName        = "account"
	accountClusterProfile     = "kcp-account"
	accountProviderName       = "openmcp-account-runtime"
	accountRuntimeLabel       = "openmcp.cloud/account-runtime"
	accountRequestFinalizer   = "account.openmcp.cloud/request"
	accountAccessFinalizer    = "account.openmcp.cloud/access"
	clusterRoleKind           = "ClusterRole"
	controllerName            = "controller"
	externalSecretsAPIGroup   = "external-secrets.services.open-control-plane.io"
	fluxAPIGroup              = "flux.services.open-control-plane.io"
	providerServiceAccount    = "service-provider"
	providerRoleName          = "service-provider"
	providerClusterRolePrefix = "ocp-account-"
	roleKind                  = "Role"
	serviceAccountKind        = "ServiceAccount"
	verbCreate                = "create"
	verbDelete                = "delete"
	verbGet                   = "get"
	verbList                  = "list"
	verbPatch                 = "patch"
	verbUpdate                = "update"
	verbWatch                 = "watch"
	serviceAPIVersion         = "v1alpha1"
)

type accountProvider struct {
	name         string
	image        string
	providerName string
	resource     metav1.GroupVersionKind
}

type accountRuntime struct {
	log               logging.Logger
	platform          *controllerclusters.Cluster
	environment       string
	bindingName       string
	reconcileInterval time.Duration
	cleanupDelay      time.Duration
	tokenLifetime     time.Duration
	providers         []accountProvider
	disconnectGuard   *accountDisconnectGuard

	mu          sync.Mutex
	generations map[multicluster.ClusterName]uint64
	accounts    map[multicluster.ClusterName]client.Client
}

var _ multicluster.Aware = (*accountRuntime)(nil)

func (r *accountRuntime) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (r *accountRuntime) Engage(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	r.mu.Lock()
	if r.generations == nil {
		r.generations = map[multicluster.ClusterName]uint64{}
	}
	if r.accounts == nil {
		r.accounts = map[multicluster.ClusterName]client.Client{}
	}
	r.generations[name]++
	generation := r.generations[name]
	r.accounts[name] = cl.GetClient()
	r.mu.Unlock()

	log := r.log.WithValues("cluster", string(name), "generation", generation)
	go r.run(ctx, name, generation, cl, log)
	return nil
}

func (r *accountRuntime) run(ctx context.Context, name multicluster.ClusterName, generation uint64, cl cluster.Cluster, log logging.Logger) {
	accountClientset, err := kubernetes.NewForConfig(cl.GetConfig())
	if err != nil {
		log.Error(err, "unable to create account clientset")
		return
	}
	reconcile := func() {
		if err := r.reconcile(ctx, name, cl, accountClientset); err != nil && ctx.Err() == nil {
			log.Error(err, "account runtime reconciliation failed")
		}
	}
	reconcile()
	ticker := time.NewTicker(r.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.scheduleCleanup(name, generation, cl.GetClient(), log)
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

func (r *accountRuntime) reconcile(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster, accountClientset kubernetes.Interface) error {
	owner, err := r.accountOwnerReference(ctx, cl.GetClient())
	if err != nil {
		return err
	}
	accountNamespace := accountControlPlaneNamespace(name)
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, cl.GetClient(), accountNamespace, owner); err != nil {
		return err
	}
	if r.disconnectGuard != nil {
		if err := r.ensureDisconnectWebhook(ctx, name, cl.GetClient()); err != nil {
			return err
		}
	}
	platformNamespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, accountNamespace)
	if err != nil {
		return err
	}
	if err := r.ensurePlatformRuntime(ctx, name, platformNamespace, cl.GetConfig().Host); err != nil {
		return err
	}
	if err := r.reconcileClusterRequests(ctx, platformNamespace); err != nil {
		return err
	}
	return r.reconcileAccessRequests(ctx, platformNamespace, cl.GetClient(), accountClientset, cl.GetConfig(), owner)
}

func (r *accountRuntime) accountOwnerReference(ctx context.Context, c client.Client) (metav1.OwnerReference, error) {
	binding := &kcpapisv1alpha1.APIBinding{}
	if err := c.Get(ctx, client.ObjectKey{Name: r.bindingName}, binding); err != nil {
		return metav1.OwnerReference{}, fmt.Errorf("get account APIBinding: %w", err)
	}
	if binding.UID == "" {
		return metav1.OwnerReference{}, fmt.Errorf("account APIBinding has no UID")
	}
	return metav1.OwnerReference{
		APIVersion: kcpapisv1alpha1.SchemeGroupVersion.String(),
		Kind:       "APIBinding",
		Name:       binding.Name,
		UID:        binding.UID,
	}, nil
}

func (r *accountRuntime) ensurePlatformRuntime(ctx context.Context, name multicluster.ClusterName, namespace, endpoint string) error {
	c := r.platform.Client()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, ns, func() error {
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[accountRuntimeLabel] = accountControlPlaneNamespace(name)
		return nil
	}); err != nil {
		return fmt.Errorf("ensure platform namespace: %w", err)
	}

	accountCluster := &clustersv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: accountClusterName, Namespace: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, accountCluster, func() error {
		accountCluster.Labels = map[string]string{
			accountRuntimeLabel:            accountControlPlaneNamespace(name),
			clustersv1alpha1.ProviderLabel: accountProviderName,
			clustersv1alpha1.ProfileLabel:  accountClusterProfile,
		}
		accountCluster.Spec = clustersv1alpha1.ClusterSpec{
			Profile:  accountClusterProfile,
			Purposes: []string{clustersv1alpha1.PURPOSE_ONBOARDING, clustersv1alpha1.PURPOSE_MCP},
			Tenancy:  clustersv1alpha1.TENANCY_SHARED,
		}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure account Cluster: %w", err)
	}
	oldCluster := accountCluster.DeepCopy()
	accountCluster.Status.Phase = commonapi.StatusPhaseReady
	accountCluster.Status.ObservedGeneration = accountCluster.Generation
	accountCluster.Status.Endpoints.Set(clustersv1alpha1.APISERVER_ENDPOINT_EXTERNAL, endpoint)
	if err := c.Status().Patch(ctx, accountCluster, client.MergeFrom(oldCluster)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("mark account Cluster ready: %w", err)
	}

	if err := r.ensureProviderRBAC(ctx, namespace); err != nil {
		return err
	}
	for _, provider := range r.providers {
		if provider.image == "" {
			continue
		}
		if err := r.ensureServiceProvider(ctx, provider); err != nil {
			return err
		}
		if err := r.ensureProviderDeployment(ctx, namespace, provider); err != nil {
			return err
		}
	}
	return nil
}

func (r *accountRuntime) ensureServiceProvider(ctx context.Context, provider accountProvider) error {
	c := r.platform.Client()
	sp := &providerv1alpha1.ServiceProvider{ObjectMeta: metav1.ObjectMeta{Name: provider.providerName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, sp, func() error { return nil }); err != nil {
		return fmt.Errorf("ensure %s ServiceProvider: %w", provider.name, err)
	}
	old := sp.DeepCopy()
	sp.Status.Resources = []metav1.GroupVersionKind{provider.resource}
	if err := c.Status().Patch(ctx, sp, client.MergeFrom(old)); err != nil {
		return fmt.Errorf("register %s service resource: %w", provider.name, err)
	}
	return nil
}

func (r *accountRuntime) ensureProviderRBAC(ctx context.Context, namespace string) error {
	c := r.platform.Client()
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: providerServiceAccount, Namespace: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, sa, func() error { return nil }); err != nil {
		return fmt.Errorf("ensure provider ServiceAccount: %w", err)
	}
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: providerRoleName, Namespace: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{APIGroups: []string{clustersv1alpha1.GroupVersion.Group}, Resources: []string{"clusters", "clusterrequests", "clusterrequests/status", "accessrequests", "accessrequests/status"}, Verbs: []string{verbGet, verbList, verbWatch, verbCreate, verbUpdate, verbPatch, verbDelete}},
			{APIGroups: []string{""}, Resources: []string{"secrets", "configmaps", "events"}, Verbs: []string{verbGet, verbList, verbWatch, verbCreate, verbUpdate, verbPatch, verbDelete}},
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{verbGet, verbList, verbWatch, verbCreate, verbUpdate, verbPatch, verbDelete}},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure provider Role: %w", err)
	}
	rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: providerRoleName, Namespace: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, rb, func() error {
		rb.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: roleKind, Name: providerRoleName}
		rb.Subjects = []rbacv1.Subject{{Kind: serviceAccountKind, Name: providerServiceAccount, Namespace: namespace}}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure provider RoleBinding: %w", err)
	}

	clusterRoleName := providerClusterRolePrefix + namespace[len(namespace)-16:]
	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName, Labels: map[string]string{accountRuntimeLabel: namespace}}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, cr, func() error {
		cr.Rules = []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{verbGet}},
			{APIGroups: []string{fluxAPIGroup}, Resources: []string{"providerconfigs"}, Verbs: []string{verbGet, verbList, verbWatch}},
			{APIGroups: []string{externalSecretsAPIGroup}, Resources: []string{"providerconfigs"}, Verbs: []string{verbGet, verbList, verbWatch}},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure provider ClusterRole: %w", err)
	}
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName, Labels: map[string]string{accountRuntimeLabel: namespace}}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, crb, func() error {
		crb.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: clusterRoleName}
		crb.Subjects = []rbacv1.Subject{{Kind: serviceAccountKind, Name: providerServiceAccount, Namespace: namespace}}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure provider ClusterRoleBinding: %w", err)
	}
	return nil
}

func (r *accountRuntime) ensureProviderDeployment(ctx context.Context, namespace string, provider accountProvider) error {
	c := r.platform.Client()
	labels := map[string]string{"app.kubernetes.io/name": provider.name, accountRuntimeLabel: namespace}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: provider.name, Namespace: namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, dep, func() error {
		one := int32(1)
		dep.Spec.Replicas = &one
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": provider.name}}
		dep.Spec.Template.Labels = labels
		dep.Spec.Template.Spec.ServiceAccountName = providerServiceAccount
		dep.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true), RunAsUser: int64Ptr(65532), RunAsGroup: int64Ptr(65532), FSGroup: int64Ptr(65532),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		}
		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            controllerName,
			Image:           provider.image,
			Args:            []string{"run", "--environment", r.environment, "--provider-name", provider.providerName, "--metrics-bind-address", "0", "--health-probe-bind-address", ":8081"},
			Env:             []corev1.EnvVar{{Name: apiconst.EnvVariablePodNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}},
			Ports:           []corev1.ContainerPort{{Name: "health", ContainerPort: 8081}},
			ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("health")}}},
			LivenessProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("health")}}},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: boolPtr(false), ReadOnlyRootFilesystem: boolPtr(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensure %s Deployment: %w", provider.name, err)
	}
	return nil
}

func (r *accountRuntime) reconcileClusterRequests(ctx context.Context, namespace string) error {
	c := r.platform.Client()
	list := &clustersv1alpha1.ClusterRequestList{}
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list ClusterRequests: %w", err)
	}
	for i := range list.Items {
		cr := &list.Items[i]
		if cr.Spec.Purpose != clustersv1alpha1.PURPOSE_ONBOARDING && cr.Spec.Purpose != clustersv1alpha1.PURPOSE_MCP {
			continue
		}
		accountCluster := &clustersv1alpha1.Cluster{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: accountClusterName}, accountCluster); err != nil {
			return err
		}
		if !cr.DeletionTimestamp.IsZero() {
			if controllerutil.RemoveFinalizer(accountCluster, cr.FinalizerForCluster()) {
				if err := c.Update(ctx, accountCluster); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
			if controllerutil.RemoveFinalizer(cr, accountRequestFinalizer) {
				if err := c.Update(ctx, cr); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
			continue
		}
		if controllerutil.AddFinalizer(cr, accountRequestFinalizer) {
			if err := c.Update(ctx, cr); err != nil {
				return err
			}
		}
		if controllerutil.AddFinalizer(accountCluster, cr.FinalizerForCluster()) {
			if err := c.Update(ctx, accountCluster); err != nil {
				return err
			}
		}
		old := cr.DeepCopy()
		cr.Status.Phase = clustersv1alpha1.REQUEST_GRANTED
		cr.Status.ObservedGeneration = cr.Generation
		cr.Status.Cluster = &commonapi.ObjectReference{Name: accountClusterName, Namespace: namespace}
		if err := c.Status().Patch(ctx, cr, client.MergeFrom(old)); err != nil {
			return err
		}
	}
	return nil
}

func (r *accountRuntime) scheduleCleanup(name multicluster.ClusterName, generation uint64, accountClient client.Client, log logging.Logger) {
	time.AfterFunc(r.cleanupDelay, func() {
		r.mu.Lock()
		current := r.generations[name]
		if current != generation {
			r.mu.Unlock()
			return
		}
		delete(r.generations, name)
		delete(r.accounts, name)
		r.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r.cleanupAccountAccess(name, accountClient, log)
		if err := r.cleanupPlatform(ctx, name); err != nil {
			log.Error(err, "account runtime cleanup failed")
		}
	})
}

func (r *accountRuntime) cleanupPlatform(ctx context.Context, name multicluster.ClusterName) error {
	namespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, accountControlPlaneNamespace(name))
	if err != nil {
		return err
	}
	c := r.platform.Client()
	if err := r.releasePlatformRequests(ctx, c, namespace); err != nil {
		return err
	}
	for _, obj := range []client.Object{
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: providerClusterRolePrefix + namespace[len(namespace)-16:]}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: providerClusterRolePrefix + namespace[len(namespace)-16:]}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}},
	} {
		if err := c.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func (r *accountRuntime) releasePlatformRequests(ctx context.Context, c client.Client, namespace string) error {
	accessRequests := &clustersv1alpha1.AccessRequestList{}
	if err := c.List(ctx, accessRequests, client.InNamespace(namespace)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("list runtime AccessRequests: %w", err)
	}
	for i := range accessRequests.Items {
		request := &accessRequests.Items[i]
		if controllerutil.RemoveFinalizer(request, accountAccessFinalizer) {
			if err := c.Update(ctx, request); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("release AccessRequest %s: %w", request.Name, err)
			}
		}
		if err := c.Delete(ctx, request); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete AccessRequest %s: %w", request.Name, err)
		}
	}

	cluster := &clustersv1alpha1.Cluster{}
	clusterErr := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: accountClusterName}, cluster)
	if clusterErr != nil && !apierrors.IsNotFound(clusterErr) {
		return fmt.Errorf("get runtime Cluster: %w", clusterErr)
	}
	clusterRequests := &clustersv1alpha1.ClusterRequestList{}
	if err := c.List(ctx, clusterRequests, client.InNamespace(namespace)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("list runtime ClusterRequests: %w", err)
	}
	for i := range clusterRequests.Items {
		request := &clusterRequests.Items[i]
		if clusterErr == nil && controllerutil.RemoveFinalizer(cluster, request.FinalizerForCluster()) {
			if err := c.Update(ctx, cluster); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("release runtime Cluster: %w", err)
			}
		}
		if controllerutil.RemoveFinalizer(request, accountRequestFinalizer) {
			if err := c.Update(ctx, request); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("release ClusterRequest %s: %w", request.Name, err)
			}
		}
		if err := c.Delete(ctx, request); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete ClusterRequest %s: %w", request.Name, err)
		}
	}
	if clusterErr == nil {
		if err := c.Delete(ctx, cluster); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete runtime Cluster: %w", err)
		}
	}
	return nil
}

func boolPtr(v bool) *bool    { return &v }
func int64Ptr(v int64) *int64 { return &v }
