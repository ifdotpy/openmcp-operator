package mcp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

const (
	workspaceClusterName      = "workspace"
	workspaceClusterProfile   = "kcp-workspace"
	workspaceProviderName     = "openmcp-workspace-runtime"
	workspaceRuntimeLabel     = "openmcp.cloud/workspace-runtime"
	labelValueTrue            = "true"
	workspaceRequestFinalizer = "workspace.openmcp.cloud/request"
	workspaceAccessFinalizer  = "workspace.openmcp.cloud/access"
	clusterRoleKind           = "ClusterRole"
	controllerName            = "controller"
	credentialIssuerName      = "credential-issuer"
	roleKind                  = "Role"
	serviceAccountKind        = "ServiceAccount"
	verbCreate                = "create"
)

type workspaceRuntime struct {
	log                logging.Logger
	platform           *controllerclusters.Cluster
	bindingName        string
	bindingExport      kcpapisv1alpha1.ExportBindingReference
	reconcileInterval  time.Duration
	cleanupDelay       time.Duration
	tokenLifetime      time.Duration
	consumerBaseConfig *rest.Config
	clientsetForConfig func(*rest.Config) (kubernetes.Interface, error)

	mu          sync.Mutex
	generations map[multicluster.ClusterName]uint64
	workspaces  map[multicluster.ClusterName]client.Client
	cleanups    map[multicluster.ClusterName]context.CancelFunc
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
	if r.cleanups == nil {
		r.cleanups = map[multicluster.ClusterName]context.CancelFunc{}
	}
	if cancel := r.cleanups[name]; cancel != nil {
		cancel()
		delete(r.cleanups, name)
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
	workspaceConfig, err := r.consumerWorkspaceConfig(name, cl.GetConfig())
	if err != nil {
		log.Error(err, "cannot address consumer workspace")
		return
	}
	reconcile := func() {
		if err := r.reconcile(ctx, name, cl.GetClient(), workspaceConfig); err != nil && ctx.Err() == nil {
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

// Provider requests use the APIExport virtual workspace. Consumer ServiceAccounts
// use the native workspace endpoint for TokenRequests and their kubeconfigs.
func (r *workspaceRuntime) consumerWorkspaceConfig(name multicluster.ClusterName, fallback *rest.Config) (*rest.Config, error) {
	if r.consumerBaseConfig == nil {
		return rest.CopyConfig(fallback), nil
	}
	return directWorkspaceConfig(r.consumerBaseConfig, name)
}

func (r *workspaceRuntime) reconcile(ctx context.Context, name multicluster.ClusterName, workspaceClient client.Client, workspaceConfig *rest.Config) error {
	binding, err := r.workspaceBinding(ctx, workspaceClient)
	if err != nil {
		return err
	}
	if !binding.DeletionTimestamp.IsZero() {
		return r.cleanupWorkspace(ctx, name, workspaceClient)
	}
	owner := workspaceBindingOwnerReference(binding)
	workspaceNamespace := workspaceControlPlaneNamespace(name)
	bootstrap := &defaultControlPlaneBootstrapper{log: r.log}
	if err := bootstrap.ensureDefault(ctx, workspaceClient, workspaceNamespace, owner); err != nil {
		return err
	}
	runtimeNamespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, workspaceNamespace)
	if err != nil {
		return err
	}
	if err := r.ensureWorkspaceRuntime(ctx, name, runtimeNamespace, workspaceConfig.Host); err != nil {
		return err
	}
	if err := r.reconcileClusterRequests(ctx, runtimeNamespace); err != nil {
		return err
	}
	return r.reconcileAccessRequests(ctx, runtimeNamespace, workspaceClient, workspaceConfig, owner)
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

func (r *workspaceRuntime) ensureWorkspaceRuntime(ctx context.Context, name multicluster.ClusterName, namespace, endpoint string) error {
	c := r.platform.Client()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, ns, func() error {
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[workspaceRuntimeLabel] = workspaceControlPlaneNamespace(name)
		return nil
	}); err != nil {
		return fmt.Errorf("ensure runtime namespace: %w", err)
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
			Purposes: []string{clustersv1alpha1.PURPOSE_MCP},
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
		if cr.Spec.Purpose != clustersv1alpha1.PURPOSE_MCP || cr.Labels[apiconst.ManagedByLabel] != controlplane.ControllerName {
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
	retryDelay := r.reconcileInterval
	if retryDelay <= 0 {
		retryDelay = time.Second
	}
	go func() {
		timer := time.NewTimer(r.cleanupDelay)
		defer timer.Stop()
		for {
			<-timer.C
			r.mu.Lock()
			current := r.generations[name]
			r.mu.Unlock()
			if current != generation {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			r.mu.Lock()
			if r.generations[name] != generation {
				r.mu.Unlock()
				cancel()
				return
			}
			if r.cleanups == nil {
				r.cleanups = map[multicluster.ClusterName]context.CancelFunc{}
			}
			r.cleanups[name] = cancel
			r.mu.Unlock()
			err := r.cleanupWorkspace(ctx, name, workspaceClient)
			cancel()

			r.mu.Lock()
			if r.generations[name] != generation {
				r.mu.Unlock()
				return
			}
			delete(r.cleanups, name)
			if err != nil {
				r.mu.Unlock()
				log.Error(err, "workspace runtime cleanup failed; retrying")
				timer.Reset(retryDelay)
				continue
			}
			delete(r.generations, name)
			delete(r.workspaces, name)
			r.mu.Unlock()
			return
		}
	}()
}

func (r *workspaceRuntime) cleanupWorkspace(ctx context.Context, name multicluster.ClusterName, workspaceClient client.Client) error {
	if err := r.cleanupWorkspaceRuntime(ctx, name); err != nil {
		return err
	}
	if err := r.finalizeWorkspaceControlPlanes(ctx, name, workspaceClient); err != nil {
		return err
	}
	return r.cleanupWorkspaceAccess(name, workspaceClient)
}

func (r *workspaceRuntime) finalizeWorkspaceControlPlanes(ctx context.Context, name multicluster.ClusterName, workspaceClient client.Client) error {
	runtimeNamespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, workspaceControlPlaneNamespace(name))
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ns := &corev1.Namespace{}
		err := r.platform.Client().Get(ctx, client.ObjectKey{Name: runtimeNamespace}, ns)
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			return fmt.Errorf("get runtime namespace %q: %w", runtimeNamespace, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for runtime namespace %q: %w", runtimeNamespace, ctx.Err())
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

func (r *workspaceRuntime) cleanupWorkspaceRuntime(ctx context.Context, name multicluster.ClusterName) error {
	namespace, err := libutils.StableMCPNamespace(defaultControlPlaneName, workspaceControlPlaneNamespace(name))
	if err != nil {
		return err
	}
	c := r.platform.Client()
	if err := r.releaseRuntimeRequests(ctx, c, namespace); err != nil {
		return err
	}
	if err := c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete workspace runtime namespace %q: %w", namespace, err)
	}
	return nil
}

func (r *workspaceRuntime) releaseRuntimeRequests(ctx context.Context, c client.Client, namespace string) error {
	accessRequests := &clustersv1alpha1.AccessRequestList{}
	if err := c.List(ctx, accessRequests, client.InNamespace(namespace), client.MatchingLabels{apiconst.ManagedByLabel: controlplane.ControllerName}); err != nil && !apierrors.IsNotFound(err) {
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
	if err := c.List(ctx, clusterRequests, client.InNamespace(namespace), client.MatchingLabels{apiconst.ManagedByLabel: controlplane.ControllerName}); err != nil && !apierrors.IsNotFound(err) {
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
