package controlplane

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strings"
	"time"

	"github.com/openmcp-project/controller-utils/pkg/resources"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/collections"
	"github.com/openmcp-project/controller-utils/pkg/collections/filters"
	"github.com/openmcp-project/controller-utils/pkg/conditions"
	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"
	"github.com/openmcp-project/controller-utils/pkg/controller/smartrequeue"
	errutils "github.com/openmcp-project/controller-utils/pkg/errors"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	"github.com/openmcp-project/controller-utils/pkg/pairs"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	cconst "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1/constants"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	"github.com/openmcp-project/openmcp-operator/internal/config"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

const ControllerName = "ManagedControlPlane"

type OnboardingCluster interface {
	Client() client.Client
	Scheme() *runtime.Scheme
}

func NewManagedControlPlaneReconciler(platformCluster *clusters.Cluster, onboardingCluster OnboardingCluster, eventRecorder events.EventRecorder, configGetter config.ManagedControlPlaneConfigGetter, configMapName string) (*ManagedControlPlaneReconciler, error) {
	if configGetter == nil {
		return nil, fmt.Errorf("managed control plane config getter must not be nil")
	}
	return &ManagedControlPlaneReconciler{
		PlatformCluster:   platformCluster,
		OnboardingCluster: onboardingCluster,
		GetConfigFunc:     configGetter,
		ConfigMapName:     configMapName,
		eventRecorder:     eventRecorder,
		sr:                smartrequeue.NewStore(5*time.Second, 24*time.Hour, 1.5),
	}, nil
}

type ManagedControlPlaneReconciler struct {
	PlatformCluster   *clusters.Cluster
	OnboardingCluster OnboardingCluster
	GetConfigFunc     config.ManagedControlPlaneConfigGetter
	ConfigMapName     string
	eventRecorder     events.EventRecorder
	sr                *smartrequeue.Store
}

var _ reconcile.Reconciler = &ManagedControlPlaneReconciler{}

type ReconcileResult = ctrlutils.ReconcileResult[*corev2alpha1.ControlPlane]

func (r *ManagedControlPlaneReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx).WithName(ControllerName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Starting reconcile")
	rr := r.reconcile(ctx, req)

	// status update
	return ctrlutils.NewOpenMCPStatusUpdaterBuilder[*corev2alpha1.ControlPlane]().
		WithNestedStruct("Status").
		WithPhaseUpdateFunc(func(obj *corev2alpha1.ControlPlane, rr ctrlutils.ReconcileResult[*corev2alpha1.ControlPlane]) (string, error) {
			if rr.Object != nil {
				if !rr.Object.DeletionTimestamp.IsZero() {
					return commonapi.StatusPhaseTerminating, nil
				}
				if conditions.AllConditionsHaveStatus(metav1.ConditionTrue, rr.Object.Status.Conditions...) {
					return commonapi.StatusPhaseReady, nil
				}
			}
			return commonapi.StatusPhaseProgressing, nil
		}).
		WithConditionUpdater(false).
		WithConditionEvents(r.eventRecorder, conditions.EventPerChange).
		WithSmartRequeue(r.sr, func(rr ReconcileResult) ctrlutils.SmartRequeueAction {
			if rr.SmartRequeue == ctrlutils.SR_NO_REQUEUE && rr.Object.Status.Phase != commonapi.StatusPhaseReady {
				// requeue if the phase is not 'Ready' if a requeue is not already planned
				return ctrlutils.SR_BACKOFF
			}
			return rr.SmartRequeue
		}).
		Build().
		UpdateStatus(ctx, r.OnboardingCluster.Client(), rr)
}

func (r *ManagedControlPlaneReconciler) reconcile(ctx context.Context, req reconcile.Request) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	// get ManagedControlPlane resource
	mcp := &corev2alpha1.ControlPlane{}
	if err := r.OnboardingCluster.Client().Get(ctx, req.NamespacedName, mcp); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Resource not found")
			return ReconcileResult{}
		}
		return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("unable to get resource '%s' from cluster: %w", req.String(), err), cconst.ReasonOnboardingClusterInteractionProblem)}
	}

	// handle operation annotation
	if mcp.GetAnnotations() != nil {
		op, ok := mcp.GetAnnotations()[apiconst.OperationAnnotation]
		if ok {
			switch op {
			case apiconst.OperationAnnotationValueIgnore:
				log.Info("Ignoring resource due to ignore operation annotation")
				return ReconcileResult{}
			case apiconst.OperationAnnotationValueReconcile:
				log.Debug("Removing reconcile operation annotation from resource")
				if err := ctrlutils.EnsureAnnotation(ctx, r.OnboardingCluster.Client(), mcp, apiconst.OperationAnnotation, "", true, ctrlutils.DELETE); err != nil {
					return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("error removing operation annotation: %w", err), cconst.ReasonOnboardingClusterInteractionProblem)}
				}
			}
		}
	}

	var rr ReconcileResult
	if mcp.DeletionTimestamp.IsZero() {
		rr = r.handleCreateOrUpdate(ctx, mcp)
	} else {
		rr = r.handleDelete(ctx, mcp)
	}

	return rr
}

// SetupWithManager sets up the controller with the Manager.
func (r *ManagedControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		For(&corev2alpha1.ControlPlane{}).
		WithEventFilter(predicate.And(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				ctrlutils.DeletionTimestampChangedPredicate{},
				ctrlutils.GotAnnotationPredicate(apiconst.OperationAnnotation, apiconst.OperationAnnotationValueReconcile),
				ctrlutils.LostAnnotationPredicate(apiconst.OperationAnnotation, apiconst.OperationAnnotationValueIgnore),
			),
			predicate.Not(
				ctrlutils.HasAnnotationPredicate(apiconst.OperationAnnotation, apiconst.OperationAnnotationValueIgnore),
			),
		))

	// add a watch for config ConfigMap if it's passed to the operator
	if r.ConfigMapName != "" {
		podNamespace := os.Getenv(apiconst.EnvVariablePodNamespace)
		ctrlBuilder = ctrlBuilder.Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				cm, ok := obj.(*corev1.ConfigMap)
				if !ok || cm.Name != r.ConfigMapName || cm.Namespace != podNamespace {
					return nil
				}
				// requeue all ManagedControlPlanes
				mcpList := &corev2alpha1.ControlPlaneList{}
				if err := r.PlatformCluster.Client().List(ctx, mcpList); err != nil {
					logging.FromContextOrPanic(ctx).Error(err, "failed to list ManagedControlPlanes for ConfigMap change")
					return nil
				}
				requests := make([]reconcile.Request, len(mcpList.Items))
				for i, mcp := range mcpList.Items {
					requests[i] = reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      mcp.Name,
							Namespace: mcp.Namespace,
						},
					}
				}
				return requests
			}),
		)
	}

	return ctrlBuilder.Complete(r)
}

func (r *ManagedControlPlaneReconciler) handleCreateOrUpdate(ctx context.Context, mcp *corev2alpha1.ControlPlane) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Handling creation or update of ManagedControlPlane resource")

	cfg, err := r.GetConfigFunc(ctx)
	if err != nil {
		return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("error fetching managedcontrolplane config: %w", err), cconst.ReasonConfigurationProblem)}
	}

	rr := ReconcileResult{
		Result: ctrl.Result{
			RequeueAfter: time.Duration(cfg.ReconcileMCPEveryXDays) * 24 * time.Hour,
		},
		Object:     mcp,
		OldObject:  mcp.DeepCopy(),
		Conditions: []metav1.Condition{},
	}
	createCon := ctrlutils.GenerateCreateConditionFunc(&rr)

	// ensure MCP and ClusterRequest finalizers on the MCP
	changed := controllerutil.AddFinalizer(mcp, corev2alpha1.MCPFinalizer)
	changed = controllerutil.AddFinalizer(mcp, corev2alpha1.ClusterRequestFinalizerPrefix+mcp.Name) || changed
	if changed {
		log.Debug("Adding finalizers to MCP")
		if err := r.OnboardingCluster.Client().Patch(ctx, mcp, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error adding finalizers to MCP: %w", err), cconst.ReasonOnboardingClusterInteractionProblem)
			createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
	}

	// ensure that the MCP namespace on the platform cluster exists
	mcpLabels := map[string]string{
		corev2alpha1.MCPNameLabel:      mcp.Name,
		corev2alpha1.MCPNamespaceLabel: mcp.Namespace,
		apiconst.ManagedByLabel:        ControllerName,
	}
	platformNamespace, err := libutils.StableMCPNamespace(mcp.Name, mcp.Namespace)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(err, cconst.ReasonInternalError)
		createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	// TODO: changed from clusteraccess.EnsureNamespace to a namespace mutator
	// because the MCP namespace might be created by another controller first (e.g. another service provider).
	// Failing in this case is not desired as the MCP reconciliation would be stuck.
	// Instead, we ensure that the labels are correct on the namespace.gi
	// In the future, we might want to refactor this further to have a more generic way of ensuring that the namespace exists with the correct labels,
	// regardless of which controller created it first.
	nsMutator := resources.NewNamespaceMutator(platformNamespace)
	nsMutator.MetadataMutator().WithLabels(mcpLabels)
	err = resources.CreateOrUpdateResource(ctx, r.PlatformCluster.Client(), nsMutator)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("error ensuring namespace '%s' on platform cluster: %w", platformNamespace, err), cconst.ReasonPlatformClusterInteractionProblem)
		createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	createCon(corev2alpha1.ConditionMeta, metav1.ConditionTrue, "", "")

	metadataLabels := map[string]string{
		apiconst.MetadataAnnotationLabelPrefix + "cp-name":      mcp.Name,
		apiconst.MetadataAnnotationLabelPrefix + "cp-namespace": mcp.Namespace,
	}
	// try to fetch the CP's namespace to identify project and workspace, if any
	cpNs := &corev1.Namespace{}
	cpNs.Name = mcp.Namespace
	if err := r.OnboardingCluster.Client().Get(ctx, client.ObjectKeyFromObject(cpNs), cpNs); err != nil {
		// this is not critical, so just log the error and ignore it
		log.Error(err, "Failed to get MCP namespace to fetch project and workspace labels", "namespace", mcp.Namespace)
	} else {
		// the hard-coded strings are ugly, but we don't want a dependency on the project-workspace operator in here
		if project, ok := cpNs.Labels["core.openmcp.cloud/project"]; ok {
			metadataLabels[apiconst.MetadataAnnotationLabelPrefix+"project"] = project
		}
		if workspace, ok := cpNs.Labels["core.openmcp.cloud/workspace"]; ok {
			metadataLabels[apiconst.MetadataAnnotationLabelPrefix+"workspace"] = workspace
		}
	}

	// ensure that the ClusterRequest exists
	cr := &clustersv1alpha1.ClusterRequest{}
	cr.Name = mcp.Name
	cr.Namespace = platformNamespace
	// determine cluster request purpose
	purpose := cfg.MCPClusterPurpose
	if override, ok := mcp.Labels[corev2alpha1.MCPPurposeOverrideLabel]; ok && override != "" {
		log.Info("Using purpose override from MCP label", "purposeOverride", override)
		purpose = override
	}
	if err := r.PlatformCluster.Client().Get(ctx, client.ObjectKeyFromObject(cr), cr); err != nil {
		if !apierrors.IsNotFound(err) {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("unable to get ClusterRequest '%s/%s': %w", cr.Namespace, cr.Name, err), cconst.ReasonPlatformClusterInteractionProblem)
			createCon(corev2alpha1.ConditionClusterRequestReady, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}

		log.Info("ClusterRequest not found, creating it", "clusterRequestName", cr.Name, "clusterRequestNamespace", cr.Namespace, "purpose", purpose)
		cr.Labels = mcpLabels
		maps.Copy(cr.Labels, metadataLabels)
		cr.Spec = clustersv1alpha1.ClusterRequestSpec{
			Purpose:                purpose,
			WaitForClusterDeletion: new(true),
		}
		if err := r.PlatformCluster.Client().Create(ctx, cr); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error creating ClusterRequest '%s/%s': %w", cr.Namespace, cr.Name, err), cconst.ReasonPlatformClusterInteractionProblem)
			createCon(corev2alpha1.ConditionClusterRequestReady, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
	} else {
		log.Debug("ClusterRequest found", "clusterRequestName", cr.Name, "clusterRequestNamespace", cr.Namespace, "configuredPurpose", purpose, "purposeInClusterRequest", cr.Spec.Purpose)

		// check if the metadata labels need to be updated
		// this should not really happen, unless the CP's namespace could not be fetched during the initial creation
		// The spec of the ClusterRequest is basically immutable, updating it is not possible.
		update := false
		for k, expectedValue := range metadataLabels {
			if cr.Labels[k] != expectedValue {
				update = true
				cr.Labels[k] = expectedValue
			}
		}
		if update {
			log.Info("Updating ClusterRequest to add missing or outdated metadata labels", "clusterRequestName", cr.Name, "clusterRequestNamespace", cr.Namespace)
			if err := r.PlatformCluster.Client().Update(ctx, cr); err != nil {
				rr.ReconcileError = errutils.WithReason(fmt.Errorf("error updating ClusterRequest '%s/%s' to add missing or outdated metadata labels: %w", cr.Namespace, cr.Name, err), cconst.ReasonPlatformClusterInteractionProblem)
				createCon(corev2alpha1.ConditionClusterRequestReady, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
				return rr
			}
		}
	}

	// check if the ClusterRequest is ready
	if !cr.Status.IsGranted() {
		log.Info("Waiting for ClusterRequest to become ready", "clusterRequestName", cr.Name, "clusterRequestNamespace", cr.Namespace, "phase", cr.Status.Phase)
		createCon(corev2alpha1.ConditionClusterRequestReady, metav1.ConditionFalse, cconst.ReasonWaitingForClusterRequest, "ClusterRequest is not ready yet")
		rr.SmartRequeue = ctrlutils.SR_BACKOFF
		return rr
	}
	log.Debug("ClusterRequest is ready", "clusterRequestName", cr.Name, "clusterRequestNamespace", cr.Namespace)
	createCon(corev2alpha1.ConditionClusterRequestReady, metav1.ConditionTrue, "", "ClusterRequest is ready")

	// fetch Cluster conditions and endpoints to display them on the MCP
	cluster := &clustersv1alpha1.Cluster{}
	if cr.Status.Cluster == nil {
		// should not happen if the ClusterRequest is granted
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("ClusterRequest '%s/%s' does not have a ClusterRef set", cr.Namespace, cr.Name), cconst.ReasonInternalError)
		createCon(corev2alpha1.ConditionClusterConditionsSynced, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	cluster.Name = cr.Status.Cluster.Name
	cluster.Namespace = cr.Status.Cluster.Namespace
	if err := r.PlatformCluster.Client().Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
		rr.ReconcileError = errutils.WithReason(fmt.Errorf("unable to get Cluster '%s/%s': %w", cluster.Namespace, cluster.Name, err), cconst.ReasonPlatformClusterInteractionProblem)
		createCon(corev2alpha1.ConditionClusterConditionsSynced, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	for _, con := range cluster.Status.Conditions {
		createCon(corev2alpha1.ConditionPrefixClusterCondition+con.Type, con.Status, con.Reason, con.Message)
	}
	createCon(corev2alpha1.ConditionClusterConditionsSynced, metav1.ConditionTrue, "", "Cluster conditions have been synced to MCP")
	mcp.Status.Endpoints = cfg.ExposedEndpoints.Apply(cluster.Status.Endpoints)
	log.Debug("Exposing endpoints", "endpoints", mcp.Status.Endpoints)

	// manage AccessRequests
	allAccessReady, removeConditions, rerr := r.manageAccessRequests(ctx, mcp, platformNamespace, cr, createCon)
	rr.ConditionsToRemove = removeConditions.UnsortedList()
	if rerr != nil {
		rr.ReconcileError = rerr
		return rr
	}

	if allAccessReady {
		rr.SmartRequeue = ctrlutils.SR_NO_REQUEUE
	} else {
		rr.SmartRequeue = ctrlutils.SR_BACKOFF
	}

	return rr
}

//nolint:gocyclo
func (r *ManagedControlPlaneReconciler) handleDelete(ctx context.Context, mcp *corev2alpha1.ControlPlane) ReconcileResult {
	log := logging.FromContextOrPanic(ctx)
	log.Info("Handling deletion of ManagedControlPlane resource")

	cfg, err := r.GetConfigFunc(ctx)
	if err != nil {
		return ReconcileResult{ReconcileError: errutils.WithReason(fmt.Errorf("error fetching managedcontrolplane config: %w", err), cconst.ReasonConfigurationProblem)}
	}

	rr := ReconcileResult{
		Result: ctrl.Result{
			RequeueAfter: time.Duration(cfg.ReconcileMCPEveryXDays) * 24 * time.Hour,
		},
		Object:     mcp,
		OldObject:  mcp.DeepCopy(),
		Conditions: []metav1.Condition{},
	}
	createCon := ctrlutils.GenerateCreateConditionFunc(&rr)

	// delete services
	remainingResources, rerr := r.deleteDependingServices(ctx, mcp)
	if rerr != nil {
		rr.ReconcileError = rerr
		createCon(corev2alpha1.ConditionAllServicesDeleted, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	if len(remainingResources) > 0 {
		serviceResourceCount := collections.AggregateMap(remainingResources, func(service string, resources []*unstructured.Unstructured, agg pairs.Pair[*[]string, int]) pairs.Pair[*[]string, int] {
			*agg.Key = append(*agg.Key, service)
			agg.Value += len(resources)
			return agg
		}, pairs.New(new([]string{}), 0))
		log.Info("Waiting for service resources to be deleted", "services", strings.Join(*serviceResourceCount.Key, ", "), "remainingResourcesCount", serviceResourceCount.Value)
		msg := strings.Builder{}
		msg.WriteString("Waiting for the following service resources to be deleted: ")
		for providerName, resources := range remainingResources {
			for _, res := range resources {
				fmt.Fprintf(&msg, "[%s]%s.%s, ", providerName, res.GetKind(), res.GetAPIVersion())
			}
		}
		createCon(corev2alpha1.ConditionAllServicesDeleted, metav1.ConditionFalse, cconst.ReasonWaitingForServiceDeletion, strings.TrimSuffix(msg.String(), ", "))
		rr.SmartRequeue = ctrlutils.SR_BACKOFF
		return rr
	}
	createCon(corev2alpha1.ConditionAllServicesDeleted, metav1.ConditionTrue, "", "All service resources have been deleted")
	log.Debug("All service resources deleted")

	// delete AccessRequests and related secrets
	platformNamespace, err := libutils.StableMCPNamespace(mcp.Name, mcp.Namespace)
	if err != nil {
		rr.ReconcileError = errutils.WithReason(err, cconst.ReasonInternalError)
		createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	accessReady, removeConditions, rerr := r.manageAccessRequests(ctx, mcp, platformNamespace, nil, createCon)
	rr.ConditionsToRemove = removeConditions.UnsortedList()
	if rerr != nil {
		rr.ReconcileError = rerr
		createCon(corev2alpha1.ConditionAllAccessReady, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	if !accessReady {
		log.Info("Waiting for AccessRequests to be deleted")
		createCon(corev2alpha1.ConditionAllAccessReady, metav1.ConditionFalse, cconst.ReasonWaitingForAccessRequestDeletion, "Waiting for AccessRequests to be deleted")
		rr.SmartRequeue = ctrlutils.SR_BACKOFF
		return rr
	}
	createCon(corev2alpha1.ConditionAllAccessReady, metav1.ConditionTrue, "", "All AccessRequests have been deleted")
	log.Debug("All AccessRequests deleted")

	// delete cluster requests related to this MCP
	remainingCRs, primaryCluster, rerr := r.deleteRelatedClusterRequests(ctx, mcp, platformNamespace)
	if rerr != nil {
		rr.ReconcileError = rerr
		createCon(corev2alpha1.ConditionAllClusterRequestsDeleted, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
		return rr
	}
	if primaryCluster != nil {
		// sync Cluster conditions and endpoints to the MCP
		for _, con := range primaryCluster.Status.Conditions {
			createCon(corev2alpha1.ConditionPrefixClusterCondition+con.Type, con.Status, con.Reason, con.Message)
		}
		createCon(corev2alpha1.ConditionClusterConditionsSynced, metav1.ConditionTrue, "", "Cluster conditions have been synced to MCP")
		mcp.Status.Endpoints = cfg.ExposedEndpoints.Apply(primaryCluster.Status.Endpoints)
		log.Debug("Exposing endpoints", "endpoints", mcp.Status.Endpoints)
	} else {
		// since this point is only reached if no error occurred during r.deleteRelatedClusterRequests, we can assume that the primaryCluster is nil because it does not exist
		for _, con := range mcp.Status.Conditions {
			// remove all conditions that were synced from the Cluster from the MCP to avoid having unhealthy leftovers
			if strings.HasPrefix(con.Type, corev2alpha1.ConditionPrefixClusterCondition) {
				rr.ConditionsToRemove = append(rr.ConditionsToRemove, con.Type)
			}
		}
		createCon(corev2alpha1.ConditionClusterConditionsSynced, metav1.ConditionTrue, "", "Primary Cluster for MCP does not exist anymore")
	}
	finalizersToRemove := sets.New(filters.FilterSlice(mcp.Finalizers, func(args ...any) bool {
		fin, ok := args[0].(string)
		if !ok {
			return false
		}
		crName, ok := strings.CutPrefix(fin, corev2alpha1.ClusterRequestFinalizerPrefix)
		return ok && !remainingCRs.Has(crName)
	})...)
	if len(finalizersToRemove) > 0 {
		log.Debug("Removing ClusterRequest finalizers for deleted ClusterRequests from MCP", "finalizers", strings.Join(sets.List(finalizersToRemove), ", "))
		old := mcp.DeepCopy()
		newFinalizers := []string{}
		for _, fin := range mcp.Finalizers {
			if !finalizersToRemove.Has(fin) {
				newFinalizers = append(newFinalizers, fin)
			}
		}
		mcp.Finalizers = newFinalizers
		if err := r.OnboardingCluster.Client().Patch(ctx, mcp, client.MergeFrom(old)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error removing ClusterRequest finalizers from MCP: %w", err), cconst.ReasonOnboardingClusterInteractionProblem)
			createCon(corev2alpha1.ConditionAllClusterRequestsDeleted, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
		rr.OldObject = mcp.DeepCopy()
	}
	if remainingCRs.Len() > 0 {
		tmp := strings.Join(sets.List(remainingCRs), ", ")
		log.Info("Waiting for ClusterRequests to be deleted", "remainingClusterRequests", tmp)
		createCon(corev2alpha1.ConditionAllClusterRequestsDeleted, metav1.ConditionFalse, cconst.ReasonWaitingForClusterRequestDeletion, fmt.Sprintf("Waiting for the following ClusterRequests to be deleted: %s", tmp))
		rr.SmartRequeue = ctrlutils.SR_BACKOFF
		return rr
	}
	createCon(corev2alpha1.ConditionAllClusterRequestsDeleted, metav1.ConditionTrue, "", "All ClusterRequests have been deleted")
	log.Debug("All ClusterRequests deleted")

	// delete MCP namespace on the platform cluster
	ns := &corev1.Namespace{}
	ns.Name = platformNamespace
	if err := r.PlatformCluster.Client().Get(ctx, client.ObjectKeyFromObject(ns), ns); err != nil {
		if !apierrors.IsNotFound(err) {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error getting namespace '%s' on platform cluster: %w", platformNamespace, err), cconst.ReasonPlatformClusterInteractionProblem)
			createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
		log.Debug("Namespace already deleted", "namespace", platformNamespace)
		ns = nil
	} else {
		if ns.Labels[corev2alpha1.MCPNameLabel] != mcp.Name || ns.Labels[corev2alpha1.MCPNamespaceLabel] != mcp.Namespace || ns.Labels[apiconst.ManagedByLabel] != ControllerName {
			log.Debug("Labels on MCP namespace on platform cluster do not match expected labels, skipping deletion", "platformNamespace", ns.Name)
		} else {
			if !ns.DeletionTimestamp.IsZero() {
				log.Debug("MCP namespace already marked for deletion", "platformNamespace", ns.Name)
			} else {
				log.Debug("Deleting MCP namespace on platform cluster", "platformNamespace", ns.Name)
				if err := r.PlatformCluster.Client().Delete(ctx, ns); err != nil {
					rr.ReconcileError = errutils.WithReason(fmt.Errorf("error deleting namespace '%s' on platform cluster: %w", platformNamespace, err), cconst.ReasonPlatformClusterInteractionProblem)
					createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
					return rr
				}
			}
		}
	}
	if ns != nil {
		log.Info("Waiting for MCP namespace to be deleted", "platformNamespace", ns.Name)
		createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, cconst.ReasonWaitingForNamespaceDeletion, fmt.Sprintf("Waiting for namespace '%s' to be deleted", platformNamespace))
		rr.SmartRequeue = ctrlutils.SR_BACKOFF
		return rr
	}

	// remove MCP finalizer
	if controllerutil.RemoveFinalizer(mcp, corev2alpha1.MCPFinalizer) {
		log.Debug("Removing MCP finalizer")
		if err := r.OnboardingCluster.Client().Patch(ctx, mcp, client.MergeFrom(rr.OldObject)); err != nil {
			rr.ReconcileError = errutils.WithReason(fmt.Errorf("error removing MCP finalizer: %w", err), cconst.ReasonOnboardingClusterInteractionProblem)
			createCon(corev2alpha1.ConditionMeta, metav1.ConditionFalse, rr.ReconcileError.Reason(), rr.ReconcileError.Error())
			return rr
		}
	}
	createCon(corev2alpha1.ConditionMeta, metav1.ConditionTrue, "", "MCP finalizer removed")
	rr.Result.RequeueAfter = 0
	if len(mcp.Finalizers) == 0 {
		// if we just removed the last finalizer on the MCP
		// (which should usually be the case, unless something external added one)
		// the MCP is now gone and updating the status will fail
		rr.Object = nil
	}

	return rr
}
