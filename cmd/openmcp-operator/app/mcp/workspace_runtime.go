package mcp

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

const (
	workspaceClusterName      = "workspace"
	workspaceClusterProfile   = "kcp-workspace"
	workspaceProviderName     = "openmcp-workspace-runtime"
	workspaceRuntimeLabel     = "openmcp.cloud/workspace-runtime"
	workspaceRequestFinalizer = "workspace.openmcp.cloud/request"
	workspaceAccessFinalizer  = "workspace.openmcp.cloud/access"
	clusterRoleKind           = "ClusterRole"
	controllerName            = "controller"
	credentialIssuerName      = "credential-issuer"
	providerServiceAccount    = "service-provider"
	providerRoleName          = "service-provider"
	providerClusterRolePrefix = "openmcp-workspace-"
	roleKind                  = "Role"
	serviceAccountKind        = "ServiceAccount"
	verbCreate                = "create"
	verbDelete                = "delete"
	verbGet                   = "get"
	verbList                  = "list"
	verbPatch                 = "patch"
	verbUpdate                = "update"
	verbWatch                 = "watch"
)

type workspaceRuntime struct {
	log                logging.Logger
	platform           *controllerclusters.Cluster
	environment        string
	bindingName        string
	bindingExport      kcpapisv1alpha1.ExportBindingReference
	reconcileInterval  time.Duration
	cleanupDelay       time.Duration
	tokenLifetime      time.Duration
	providers          []workspaceProvider
	disconnectGuard    *workspaceDisconnectGuard
	kcpConfig          *rest.Config
	clientsetForConfig func(*rest.Config) (kubernetes.Interface, error)

	mu          sync.Mutex
	generations map[multicluster.ClusterName]uint64
	workspaces  map[multicluster.ClusterName]client.Client
}

var _ multicluster.Aware = (*workspaceRuntime)(nil)

func (r *workspaceRuntime) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (r *workspaceRuntime) Engage(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	r.mu.Lock()
	if r.generations == nil {
		r.generations = map[multicluster.ClusterName]uint64{}
	}
	if r.workspaces == nil {
		r.workspaces = map[multicluster.ClusterName]client.Client{}
	}
	r.generations[name]++
	generation := r.generations[name]
	r.workspaces[name] = cl.GetClient()
	r.mu.Unlock()

	log := r.log.WithValues("cluster", string(name), "generation", generation)
	go r.run(ctx, name, generation, cl, log)
	return nil
}

func (r *workspaceRuntime) run(ctx context.Context, name multicluster.ClusterName, generation uint64, cl cluster.Cluster, log logging.Logger) {
	workspaceConfig, err := directWorkspaceConfig(r.kcpConfig, name)
	if err != nil {
		log.Error(err, "unable to create direct workspace configuration")
		return
	}
	reconcile := func() {
		if err := r.reconcile(ctx, name, cl, workspaceConfig); err != nil && ctx.Err() == nil {
			log.Error(err, "workspace runtime reconciliation failed")
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

func (r *workspaceRuntime) reconcile(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster, workspaceConfig *rest.Config) error {
	binding, err := r.workspaceBinding(ctx, cl.GetClient())
	if err != nil {
		return err
	}
	owner := workspaceBindingOwnerReference(binding)
	workspaceNamespace := workspaceControlPlaneNamespace(name)
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, cl.GetClient(), workspaceNamespace, owner); err != nil {
		return err
	}
	if r.disconnectGuard != nil {
		if err := r.ensureDisconnectWebhook(ctx, name, cl.GetClient(), binding); err != nil {
			return err
		}
	}
	platformNamespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, workspaceNamespace)
	if err != nil {
		return err
	}
	if err := r.ensurePlatformRuntime(ctx, name, platformNamespace, workspaceConfig.Host); err != nil {
		return err
	}
	if err := r.reconcileClusterRequests(ctx, platformNamespace); err != nil {
		return err
	}
	return r.reconcileAccessRequests(ctx, platformNamespace, cl.GetClient(), workspaceConfig, owner)
}

func directWorkspaceConfig(base *rest.Config, name multicluster.ClusterName) (*rest.Config, error) {
	if base == nil {
		return nil, fmt.Errorf("kcp configuration is required")
	}
	parsed, err := url.Parse(base.Host)
	if err != nil {
		return nil, fmt.Errorf("parse kcp host: %w", err)
	}
	const marker = "/clusters/"
	index := strings.Index(parsed.Path, marker)
	if parsed.Scheme == "" || parsed.Host == "" || index < 0 || name == "" {
		return nil, fmt.Errorf("kcp host %q cannot address workspace %q", base.Host, name)
	}
	parsed.Path = parsed.Path[:index] + marker + url.PathEscape(string(name))
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	config := rest.CopyConfig(base)
	config.Host = parsed.String()
	return config, nil
}

func (r *workspaceRuntime) workspaceBinding(ctx context.Context, c client.Client) (*kcpapisv1alpha1.APIBinding, error) {
	if r.bindingName != "" {
		binding := &kcpapisv1alpha1.APIBinding{}
		err := c.Get(ctx, client.ObjectKey{Name: r.bindingName}, binding)
		if err == nil {
			if !r.matchesWorkspaceExport(binding) {
				return nil, fmt.Errorf("preferred APIBinding %q does not reference APIExport %q", binding.Name, r.bindingExport.Name)
			}
			return binding, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get preferred workspace APIBinding: %w", err)
		}
	}
	bindings := &kcpapisv1alpha1.APIBindingList{}
	if err := c.List(ctx, bindings); err != nil {
		return nil, fmt.Errorf("list workspace APIBindings: %w", err)
	}
	var match *kcpapisv1alpha1.APIBinding
	for i := range bindings.Items {
		binding := &bindings.Items[i]
		if !r.matchesWorkspaceExport(binding) {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("multiple APIBindings reference APIExport %q", r.bindingExport.Name)
		}
		match = binding
	}
	if match == nil {
		return nil, fmt.Errorf("no APIBinding references APIExport %q", r.bindingExport.Name)
	}
	return match, nil
}

func (r *workspaceRuntime) matchesWorkspaceExport(binding *kcpapisv1alpha1.APIBinding) bool {
	export := binding.Spec.Reference.Export
	return export != nil && export.Name == r.bindingExport.Name && export.Path == r.bindingExport.Path
}

func workspaceBindingOwnerReference(binding *kcpapisv1alpha1.APIBinding) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: kcpapisv1alpha1.SchemeGroupVersion.String(),
		Kind:       "APIBinding",
		Name:       binding.Name,
		UID:        binding.UID,
	}
}

func (r *workspaceRuntime) ensurePlatformRuntime(ctx context.Context, name multicluster.ClusterName, namespace, endpoint string) error {
	c := r.platform.Client()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, ns, func() error {
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[workspaceRuntimeLabel] = workspaceControlPlaneNamespace(name)
		return nil
	}); err != nil {
		return fmt.Errorf("ensure platform namespace: %w", err)
	}

	workspaceCluster := &clustersv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: workspaceClusterName, Namespace: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, workspaceCluster, func() error {
		workspaceCluster.Labels = map[string]string{
			workspaceRuntimeLabel:          workspaceControlPlaneNamespace(name),
			clustersv1alpha1.ProviderLabel: workspaceProviderName,
			clustersv1alpha1.ProfileLabel:  workspaceClusterProfile,
		}
		workspaceCluster.Spec = clustersv1alpha1.ClusterSpec{
			Profile:  workspaceClusterProfile,
			Purposes: []string{clustersv1alpha1.PURPOSE_ONBOARDING, clustersv1alpha1.PURPOSE_MCP},
			Tenancy:  clustersv1alpha1.TENANCY_SHARED,
		}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure workspace Cluster: %w", err)
	}
	oldCluster := workspaceCluster.DeepCopy()
	workspaceCluster.Status.Phase = commonapi.StatusPhaseReady
	workspaceCluster.Status.ObservedGeneration = workspaceCluster.Generation
	workspaceCluster.Status.Endpoints.Set(clustersv1alpha1.APISERVER_ENDPOINT_EXTERNAL, endpoint)
	if err := c.Status().Patch(ctx, workspaceCluster, client.MergeFrom(oldCluster)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("mark workspace Cluster ready: %w", err)
	}

	if len(r.providers) > 0 {
		if err := r.ensureProviderRBAC(ctx, namespace); err != nil {
			return err
		}
		for _, provider := range r.providers {
			if err := r.ensureServiceProvider(ctx, provider); err != nil {
				return err
			}
			if err := r.ensureProviderDeployment(ctx, namespace, provider); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *workspaceRuntime) ensureServiceProvider(ctx context.Context, provider workspaceProvider) error {
	c := r.platform.Client()
	sp := &providerv1alpha1.ServiceProvider{ObjectMeta: metav1.ObjectMeta{Name: provider.ProviderName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, sp, func() error {
		if sp.Annotations == nil {
			sp.Annotations = map[string]string{}
		}
		sp.Annotations[apiconst.OperationAnnotation] = apiconst.OperationAnnotationValueIgnore
		sp.Spec.Image = provider.Image
		return nil
	}); err != nil {
		return fmt.Errorf("ensure %s ServiceProvider: %w", provider.Name, err)
	}
	old := sp.DeepCopy()
	sp.Status.ObservedGeneration = sp.Generation
	sp.Status.Resources = []metav1.GroupVersionKind{provider.Resource}
	if err := c.Status().Patch(ctx, sp, client.MergeFrom(old)); err != nil {
		return fmt.Errorf("register %s service resource: %w", provider.Name, err)
	}
	return nil
}

func (r *workspaceRuntime) ensureProviderRBAC(ctx context.Context, namespace string) error {
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
	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName, Labels: map[string]string{workspaceRuntimeLabel: namespace}}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, cr, func() error {
		cr.Rules = []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{verbGet}}}
		for _, provider := range r.providers {
			cr.Rules = append(cr.Rules, provider.ClusterRoleRules...)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure provider ClusterRole: %w", err)
	}
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName, Labels: map[string]string{workspaceRuntimeLabel: namespace}}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, crb, func() error {
		crb.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: clusterRoleKind, Name: clusterRoleName}
		crb.Subjects = []rbacv1.Subject{{Kind: serviceAccountKind, Name: providerServiceAccount, Namespace: namespace}}
		return nil
	}); err != nil {
		return fmt.Errorf("ensure provider ClusterRoleBinding: %w", err)
	}
	return nil
}

func (r *workspaceRuntime) ensureProviderDeployment(ctx context.Context, namespace string, provider workspaceProvider) error {
	c := r.platform.Client()
	labels := map[string]string{"app.kubernetes.io/name": provider.Name, workspaceRuntimeLabel: namespace}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: provider.Name, Namespace: namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, dep, func() error {
		one := int32(1)
		dep.Spec.Replicas = &one
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": provider.Name}}
		dep.Spec.Template.Labels = labels
		dep.Spec.Template.Spec.ServiceAccountName = providerServiceAccount
		dep.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true), RunAsUser: int64Ptr(65532), RunAsGroup: int64Ptr(65532), FSGroup: int64Ptr(65532),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		}
		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            controllerName,
			Image:           provider.Image,
			Args:            []string{"run", "--environment", r.environment, "--provider-name", provider.ProviderName, "--metrics-bind-address", "0", "--health-probe-bind-address", ":8081"},
			Env:             []corev1.EnvVar{{Name: apiconst.EnvVariablePodNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}},
			Ports:           []corev1.ContainerPort{{Name: "health", ContainerPort: 8081}},
			ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("health")}}},
			LivenessProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("health")}}},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: boolPtr(false), ReadOnlyRootFilesystem: boolPtr(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensure %s Deployment: %w", provider.Name, err)
	}
	return nil
}

func (r *workspaceRuntime) reconcileClusterRequests(ctx context.Context, namespace string) error {
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
		workspaceCluster := &clustersv1alpha1.Cluster{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspaceClusterName}, workspaceCluster); err != nil {
			return err
		}
		if !cr.DeletionTimestamp.IsZero() {
			if controllerutil.RemoveFinalizer(workspaceCluster, cr.FinalizerForCluster()) {
				if err := c.Update(ctx, workspaceCluster); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
			if controllerutil.RemoveFinalizer(cr, workspaceRequestFinalizer) {
				if err := c.Update(ctx, cr); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
			continue
		}
		if controllerutil.AddFinalizer(cr, workspaceRequestFinalizer) {
			if err := c.Update(ctx, cr); err != nil {
				return err
			}
		}
		if controllerutil.AddFinalizer(workspaceCluster, cr.FinalizerForCluster()) {
			if err := c.Update(ctx, workspaceCluster); err != nil {
				return err
			}
		}
		old := cr.DeepCopy()
		cr.Status.Phase = clustersv1alpha1.REQUEST_GRANTED
		cr.Status.ObservedGeneration = cr.Generation
		cr.Status.Cluster = &commonapi.ObjectReference{Name: workspaceClusterName, Namespace: namespace}
		if err := c.Status().Patch(ctx, cr, client.MergeFrom(old)); err != nil {
			return err
		}
	}
	return nil
}

func (r *workspaceRuntime) scheduleCleanup(name multicluster.ClusterName, generation uint64, workspaceClient client.Client, log logging.Logger) {
	time.AfterFunc(r.cleanupDelay, func() {
		r.mu.Lock()
		current := r.generations[name]
		if current != generation {
			r.mu.Unlock()
			return
		}
		delete(r.generations, name)
		delete(r.workspaces, name)
		r.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := r.cleanupPlatform(ctx, name); err != nil {
			log.Error(err, "workspace runtime cleanup failed")
			return
		}
		if err := r.finalizeWorkspaceControlPlanes(ctx, name, workspaceClient); err != nil {
			log.Error(err, "workspace ControlPlane cleanup failed")
			return
		}
		r.cleanupWorkspaceAccess(name, workspaceClient, log)
	})
}

func (r *workspaceRuntime) finalizeWorkspaceControlPlanes(ctx context.Context, name multicluster.ClusterName, workspaceClient client.Client) error {
	platformNamespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, workspaceControlPlaneNamespace(name))
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ns := &corev1.Namespace{}
		err := r.platform.Client().Get(ctx, client.ObjectKey{Name: platformNamespace}, ns)
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			return fmt.Errorf("get platform namespace %q: %w", platformNamespace, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for platform namespace %q: %w", platformNamespace, ctx.Err())
		case <-ticker.C:
		}
	}

	controlPlanes := &corev2alpha1.ControlPlaneList{}
	if err := workspaceClient.List(ctx, controlPlanes, client.InNamespace(workspaceControlPlaneNamespace(name))); err != nil {
		return fmt.Errorf("list workspace ControlPlanes: %w", err)
	}
	for i := range controlPlanes.Items {
		controlPlane := &controlPlanes.Items[i]
		if controlPlane.DeletionTimestamp.IsZero() {
			continue
		}
		old := controlPlane.DeepCopy()
		finalizers := controlPlane.Finalizers[:0]
		for _, finalizer := range controlPlane.Finalizers {
			if finalizer == corev2alpha1.MCPFinalizer || strings.HasPrefix(finalizer, corev2alpha1.ClusterRequestFinalizerPrefix) {
				continue
			}
			finalizers = append(finalizers, finalizer)
		}
		controlPlane.Finalizers = finalizers
		if err := workspaceClient.Patch(ctx, controlPlane, client.MergeFrom(old)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("finalize workspace ControlPlane %s/%s: %w", controlPlane.Namespace, controlPlane.Name, err)
		}
	}
	return nil
}

func (r *workspaceRuntime) cleanupPlatform(ctx context.Context, name multicluster.ClusterName) error {
	namespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, workspaceControlPlaneNamespace(name))
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

func (r *workspaceRuntime) releasePlatformRequests(ctx context.Context, c client.Client, namespace string) error {
	accessRequests := &clustersv1alpha1.AccessRequestList{}
	if err := c.List(ctx, accessRequests, client.InNamespace(namespace)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("list runtime AccessRequests: %w", err)
	}
	for i := range accessRequests.Items {
		request := &accessRequests.Items[i]
		if controllerutil.RemoveFinalizer(request, workspaceAccessFinalizer) {
			if err := c.Update(ctx, request); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("release AccessRequest %s: %w", request.Name, err)
			}
		}
		if err := c.Delete(ctx, request); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete AccessRequest %s: %w", request.Name, err)
		}
	}

	cluster := &clustersv1alpha1.Cluster{}
	clusterErr := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: workspaceClusterName}, cluster)
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
		if controllerutil.RemoveFinalizer(request, workspaceRequestFinalizer) {
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
