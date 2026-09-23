//nolint:revive
package advanced

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	maputils "github.com/openmcp-project/controller-utils/pkg/collections/maps"
	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	openmcpconst "github.com/openmcp-project/openmcp-operator/api/constants"
)

const caControllerName = "ClusterAccess"
const Finalizer = clustersv1alpha1.GroupName + "/clusteraccess"

//////////////////
/// INTERFACES ///
//////////////////

// ClusterAccessReconciler reconciles access to Kubernetes clusters through the open control plane Cluster API.
// It can create ClusterRequests and/or AccessRequests for an amount of clusters.
type ClusterAccessReconciler interface {
	// Register registers a cluster to be managed by the reconciler.
	// No-op if reg is nil.
	// Overwrites any previous registration with the same ID.
	Register(reg ClusterRegistration) ClusterAccessReconciler
	// Unregister unregisters a cluster from being managed by the reconciler.
	// No-op if no registration with the given ID exists.
	Unregister(id string) ClusterAccessReconciler
	// WithRetryInterval sets the retry interval.
	WithRetryInterval(interval time.Duration) ClusterAccessReconciler
	// WithManagedLabels allows to overwrite the managed-by and managed-purpose labels that are set on the created resources.
	// Note that the implementation might depend on these labels to identify the resources it created,
	// so changing them might lead to unexpected behavior. They also must be unique within the context of this reconciler.
	// Use with caution.
	// Note that all label values are put through a function which replaces a part of them with a unique and deterministic hash if they exceed the maximum allowed length for Kubernetes label values.
	WithManagedLabels(gen ManagedLabelGenerator) ClusterAccessReconciler

	// Access returns an internal Cluster object granting access to the cluster for the specified request with the specified id.
	// Will fail if the cluster is not registered or no AccessRequest is registered for the cluster, or if some other error occurs.
	Access(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clusters.Cluster, error)
	// AccessRequest fetches the AccessRequest object for the cluster for the specified request with the specified id.
	// Will fail if the cluster is not registered or no AccessRequest is registered for the cluster, or if some other error occurs.
	// The same additionalData must be passed into all methods of this ClusterAccessReconciler for the same request and id.
	AccessRequest(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.AccessRequest, error)
	// ClusterRequest fetches the ClusterRequest object for the cluster for the specified request with the specified id.
	// Will fail if the cluster is not registered or no ClusterRequest is registered for the cluster, or if some other error occurs.
	// The same additionalData must be passed into all methods of this ClusterAccessReconciler for the same request and id.
	ClusterRequest(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.ClusterRequest, error)
	// Cluster fetches the external Cluster object for the cluster for the specified request with the specified id.
	// Will fail if the cluster is not registered or no Cluster can be determined, or if some other error occurs.
	// The same additionalData must be passed into all methods of this ClusterAccessReconciler for the same request and id.
	Cluster(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.Cluster, error)

	// Reconcile creates the ClusterRequests and/or AccessRequests for the registered clusters.
	// This function should be called during all reconciliations of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled.
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	// The duration is set by the WithRetryInterval method.
	// Any additional arguments provided are passed into all methods of the ClusterRegistration objects that are called.
	//
	// Note that Reconcile will not create any new resources if the current request is in deletion.
	// A request is considered to be in deletion if ReconcileDelete has been called for it at least once and not successfully finished (= with RequeueAfter == 0 and no error) since then.
	// This means that Reconcile can safely be called at the beginning of a deletion reconciliation without having to worry about re-creating already deleted resources.
	Reconcile(ctx context.Context, request reconcile.Request, additionalData ...any) (reconcile.Result, error)
	// ReconcileDelete deletes the ClusterRequests and/or AccessRequests for the registered clusters.
	// This function should be called during the deletion of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled.
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	// The duration is set by the WithRetryInterval method.
	// Any additional arguments provided are passed into all methods of the ClusterRegistration objects that are called.
	ReconcileDelete(ctx context.Context, request reconcile.Request, additionalData ...any) (reconcile.Result, error)

	// WithFakingCallback passes a callback function with a specific key.
	// The available keys depend on the implementation.
	// The key determines when the callback function is executed.
	// This feature is meant for unit testing, where usually no ClusterProvider, which could answer ClusterRequests and AccessRequests, is running.
	WithFakingCallback(key string, callback FakingCallback) ClusterAccessReconciler
	// WithFakeClientGenerator sets a fake client generator function.
	// If non-nil, the function will be called during the Access method with the kubeconfig data retrieved from the AccessRequest secret and the scheme configured for the corresponding cluster registration.
	// This allows injecting custom behavior for generating the client from the kubeconfig data, e.g. returning a standard fake client if the kubeconfig data is just 'fake' or something similar.
	// Note that this is unset by default and must be set explicitly.
	WithFakeClientGenerator(f FakeClientGenerator) ClusterAccessReconciler

	// Update allows to update some aspects of a cluster registration.
	// Returns an error if no registration with the given ID exists or the registration does not support updates.
	// Updates take effect after the next reconciliation.
	Update(id string, updates ...ClusterRegistrationUpdate) error
}

// FakingCallback is a function that allows to mock a desired state for unit testing.
//
// The idea behind this is: Before running any reconciliation logic in a unit tests, make sure to pass these callback functions to the cluster access reconciler implementation.
// During the reocncile, the cluster access reconciler will call these functions at specific points, e.g. when it is waiting for an AccessRequest to be approved.
// The time of the execution depends on the key and the implementation of the cluster access reconciler.
// In the callback function, modify the cluster state as desired by mocking actions usually performed by external operators, e.g. set the AccessRequest to approved.
//
// Note that most of the arguments can be nil, depending on when the callback is executed.
// The arguments are:
// - ctx is the context, which is required for interacting with the cluster.
// - platformClusterClient is the client for the platform cluster.
// - key is the key that determined the execution of the callback.
// - req is the reconcile.Request that triggered the reconciliation, if known.
// - cr is the ClusterRequest related to the cluster access reconciliation, if known.
// - ar is the AccessRequest related to the cluster access reconciliation, if known.
// - c is the Cluster related to the cluster access reconciliation, if known.
// - access is the access to the cluster, if already retrieved.
type FakingCallback func(ctx context.Context, platformClusterClient client.Client, key string, req *reconcile.Request, cr *clustersv1alpha1.ClusterRequest, ar *clustersv1alpha1.AccessRequest, c *clustersv1alpha1.Cluster, access *clusters.Cluster) error

// ManagedLabelGenerator is a function that generates the managed-by and managed-purpose labels for the created resources.
// The first return value is the value for the managed-by label, the second one is the value for the managed-purpose label.
// The third return value can be nil, or it can contain additional labels to be set on the created resources.
type ManagedLabelGenerator func(controllerName string, req reconcile.Request, reg ClusterRegistration) (string, string, map[string]string)

func ensureManagedLabelLength(gen ManagedLabelGenerator) ManagedLabelGenerator {
	return func(controllerName string, req reconcile.Request, reg ClusterRegistration) (string, string, map[string]string) {
		mb, mp, add := gen(controllerName, req, reg)
		mb = ctrlutils.ShortenToXCharactersUnsafe(mb, ctrlutils.K8sMaxNameLength)
		mp = ctrlutils.ShortenToXCharactersUnsafe(mp, ctrlutils.K8sMaxNameLength)
		for k, v := range add {
			add[k] = ctrlutils.ShortenToXCharactersUnsafe(v, ctrlutils.K8sMaxNameLength)
		}
		return mb, mp, add
	}
}

func DefaultManagedLabelGenerator(controllerName string, req reconcile.Request, reg ClusterRegistration) (string, string, map[string]string) {
	sb := strings.Builder{}
	if req.Namespace != "" {
		sb.WriteString(req.Namespace)
		sb.WriteString(".")
	}
	sb.WriteString(req.Name)
	sb.WriteString(".")
	sb.WriteString(reg.ID())
	return controllerName, strings.Trim(sb.String(), ". "), nil
}

// FakeClientGenerator is a function that generates a fake client.Client.
// It is used as an argument for the ClusterAccessReconciler's WithFakeClientGenerator method.
type FakeClientGenerator func(ctx context.Context, kcfgData []byte, scheme *runtime.Scheme, additionalData ...any) (client.Client, error)

//////////////////////////////////
/// IMPLEMENTATION: Reconciler ///
//////////////////////////////////

type reconcilerImpl struct {
	controllerName        string
	platformClusterClient client.Client
	requestsInDeletion    sets.Set[reconcile.Request]
	ridLock               *sync.RWMutex

	interval      time.Duration
	registrations map[string]ClusterRegistration
	managedBy     ManagedLabelGenerator

	fakingCallbacks    map[string]FakingCallback
	generateFakeClient FakeClientGenerator
}

// NewClusterAccessReconciler creates a new Cluster Access Reconciler.
// Note that it needs to be configured further by calling its Register method and optionally its builder-like With* methods.
// This is meant to be instantiated and configured once during controller setup and then its Reconcile or ReconcileDelete methods should be called during each reconciliation of the controller.
func NewClusterAccessReconciler(platformClusterClient client.Client, controllerName string) ClusterAccessReconciler {
	return &reconcilerImpl{
		controllerName:        controllerName,
		platformClusterClient: platformClusterClient,
		requestsInDeletion:    sets.New[reconcile.Request](),
		ridLock:               &sync.RWMutex{},
		interval:              5 * time.Second,
		registrations:         map[string]ClusterRegistration{},
		managedBy:             ensureManagedLabelLength(DefaultManagedLabelGenerator),
	}
}

var _ ClusterAccessReconciler = &reconcilerImpl{}

// Access implements Reconciler.
func (r *reconcilerImpl) Access(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clusters.Cluster, error) {
	reg, ok := r.registrations[id]
	if !ok {
		return nil, fmt.Errorf("no registration found for request '%s' with id '%s'", request.String(), id)
	}
	ar, err := r.AccessRequest(ctx, request, id, additionalData...)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch AccessRequest for request '%s' with id '%s': %w", request.String(), id, err)
	}
	if !ar.Status.IsGranted() {
		return nil, fmt.Errorf("AccessRequest '%s/%s' for request '%s' with id '%s' is not granted", ar.Namespace, ar.Name, request.String(), id)
	}
	access, err := accessFromAccessRequest(ctx, r.platformClusterClient, id, reg.Scheme(), ar, r.generateFakeClient, additionalData...)
	if err != nil {
		return nil, fmt.Errorf("unable to get access for request '%s' with id '%s': %w", request.String(), id, err)
	}
	return access, nil
}

// AccessRequest implements Reconciler.
func (r *reconcilerImpl) AccessRequest(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.AccessRequest, error) {
	rawReg, ok := r.registrations[id]
	if !ok {
		return nil, fmt.Errorf("no registration found for request '%s' with id '%s'", request.String(), id)
	}
	if !rawReg.AccessRequestAvailable() {
		return nil, fmt.Errorf("no AccessRequest configured for request '%s' with id '%s'", request.String(), id)
	}
	suffix := rawReg.Suffix()
	if suffix == "" {
		suffix = rawReg.ID()
	}
	reg := rawReg.Parameterize(request, additionalData...)
	namespace, err := reg.Namespace()
	if err != nil {
		return nil, fmt.Errorf("unable to generate name of platform cluster namespace for request '%s': %w", request.String(), err)
	}
	ar := &clustersv1alpha1.AccessRequest{}
	ar.Name = StableRequestName(r.controllerName, request, suffix)
	ar.Namespace = namespace
	if err := r.platformClusterClient.Get(ctx, client.ObjectKeyFromObject(ar), ar); err != nil {
		return nil, fmt.Errorf("unable to get AccessRequest '%s/%s' for request '%s' with id '%s': %w", ar.Namespace, ar.Name, request.String(), id, err)
	}
	return ar, nil
}

// Cluster implements Reconciler.
func (r *reconcilerImpl) Cluster(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.Cluster, error) {
	rawReg, ok := r.registrations[id]
	if !ok {
		return nil, fmt.Errorf("no registration found for request '%s' with id '%s'", request.String(), id)
	}
	reg := rawReg.Parameterize(request, additionalData...)
	clusterRef, err := reg.ClusterReference()
	if err != nil {
		return nil, fmt.Errorf("unable to generate Cluster reference from registration for request '%s' with id '%s': %w", request.String(), id, err)
	}
	if clusterRef == nil {
		// if no ClusterReference is specified, try to get it from the ClusterRequest
		if reg.ClusterRequestAvailable() {
			cr, err := r.ClusterRequest(ctx, request, id)
			if err != nil {
				return nil, fmt.Errorf("unable to fetch ClusterRequest for request '%s' with id '%s': %w", request.String(), id, err)
			}
			if cr.Status.Cluster == nil {
				return nil, fmt.Errorf("ClusterRequest '%s/%s' for request '%s' with id '%s' has no cluster reference", cr.Namespace, cr.Name, request.String(), id)
			}
			clusterRef = cr.Status.Cluster.DeepCopy()
		} else {
			// try to get the Cluster from the AccessRequest
			ar, err := r.AccessRequest(ctx, request, id)
			if err != nil {
				return nil, fmt.Errorf("unable to fetch AccessRequest for request '%s' with id '%s': %w", request.String(), id, err)
			}
			if ar.Spec.ClusterRef == nil {
				return nil, fmt.Errorf("AccessRequest '%s/%s' for request '%s' with id '%s' has no cluster reference", ar.Namespace, ar.Name, request.String(), id)
			}
			clusterRef = ar.Spec.ClusterRef.DeepCopy()
		}
	}

	c := &clustersv1alpha1.Cluster{}
	if err := r.platformClusterClient.Get(ctx, client.ObjectKey{Name: clusterRef.Name, Namespace: clusterRef.Namespace}, c); err != nil {
		return nil, fmt.Errorf("unable to get Cluster '%s/%s' for request '%s' with id '%s': %w", clusterRef.Namespace, clusterRef.Name, request.String(), id, err)
	}
	return c, nil
}

// ClusterRequest implements Reconciler.
func (r *reconcilerImpl) ClusterRequest(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.ClusterRequest, error) {
	rawReg, ok := r.registrations[id]
	if !ok {
		return nil, fmt.Errorf("no registration found for request '%s' with id '%s'", request.String(), id)
	}
	reg := rawReg.Parameterize(request, additionalData...)
	crRef, err := reg.ClusterRequestReference()
	if err != nil {
		return nil, fmt.Errorf("unable to generate ClusterRequest reference from registration for request '%s' with id '%s': %w", request.String(), id, err)
	}
	if crRef == nil {
		// if the ClusterRequest is not referenced directly, check if was created or can be retrieved via an AccessRequest
		crSpec, err := reg.ClusterRequestSpec()
		if err != nil {
			return nil, fmt.Errorf("unable to generate ClusterRequest spec from registration for request '%s' with id '%s': %w", request.String(), id, err)
		}
		if crSpec != nil {
			// the ClusterRequest was created by this library
			suffix := reg.Suffix()
			if suffix == "" {
				suffix = reg.ID()
			}
			namespace, err := reg.Namespace()
			if err != nil {
				return nil, fmt.Errorf("unable to generate name of platform cluster namespace for request '%s': %w", request.String(), err)
			}
			crRef = &commonapi.ObjectReference{
				Name:      StableRequestName(r.controllerName, request, suffix),
				Namespace: namespace,
			}
		} else {
			// check if an AccessRequest can be found and if it contains a ClusterRequest reference
			ar, err := r.AccessRequest(ctx, request, id)
			if err != nil {
				return nil, fmt.Errorf("unable to fetch AccessRequest for request '%s' with id '%s': %w", request.String(), id, err)
			}
			if ar.Spec.RequestRef == nil {
				return nil, fmt.Errorf("no ClusterRequest configured or referenced for request '%s' with id '%s'", request.String(), id)
			}
			crRef = ar.Spec.RequestRef.DeepCopy()
		}
	}

	cr := &clustersv1alpha1.ClusterRequest{}
	if err := r.platformClusterClient.Get(ctx, client.ObjectKey{Name: crRef.Name, Namespace: crRef.Namespace}, cr); err != nil {
		return nil, fmt.Errorf("unable to get ClusterRequest '%s/%s' for request '%s' with id '%s': %w", crRef.Namespace, crRef.Name, request.String(), id, err)
	}
	return cr, nil
}

// Reconcile implements Reconciler.
//
//nolint:gocyclo
func (r *reconcilerImpl) Reconcile(ctx context.Context, request reconcile.Request, additionalData ...any) (reconcile.Result, error) {
	log := logging.FromContextOrDiscard(ctx).WithName(caControllerName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Reconciling cluster access")
	inDeletion := r.isInDeletion(request)
	if inDeletion {
		log.Info("Request is in deletion, missing resources will not be created")
	}

	// iterate over registrations and ensure that the corresponding resources are ready
	res := reconcile.Result{}
	for _, rawReg := range r.registrations {
		rlog := log.WithValues("id", rawReg.ID())
		rctx := logging.NewContext(ctx, rlog)
		rlog.Debug("Processing registration")
		suffix := rawReg.Suffix()
		if suffix == "" {
			suffix = rawReg.ID()
		}
		reg := rawReg.Parameterize(request, additionalData...)
		managedBy, managedPurpose, additionalLabels := r.managedBy(r.controllerName, request, reg)
		expectedLabels := map[string]string{}
		expectedLabels[openmcpconst.ManagedByLabel] = managedBy
		expectedLabels[openmcpconst.ManagedPurposeLabel] = managedPurpose
		expectedLabels = maputils.Merge(additionalLabels, expectedLabels)

		// ensure namespace exists
		namespace, err := reg.Namespace()
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("unable to generate name of platform cluster namespace for request '%s': %w", request.String(), err)
		}
		rlog.Debug("Ensuring platform cluster namespace exists", "namespaceName", namespace)
		ns := &corev1.Namespace{}
		ns.Name = namespace
		if err := r.platformClusterClient.Get(rctx, client.ObjectKeyFromObject(ns), ns); err != nil {
			if !apierrors.IsNotFound(err) {
				return reconcile.Result{}, fmt.Errorf("unable to get platform cluster namespace '%s': %w", ns.Name, err)
			}
			if inDeletion {
				rlog.Debug("Skipping platform cluster namespace creation because this request is in deletion")
				continue
			}
			rlog.Info("Creating platform cluster namespace", "namespaceName", ns.Name)
			if err := r.platformClusterClient.Create(rctx, ns); err != nil {
				return reconcile.Result{}, fmt.Errorf("unable to create platform cluster namespace '%s': %w", ns.Name, err)
			}
		}

		// if a ClusterRequest is requested, ensure it exists and is ready
		var cr *clustersv1alpha1.ClusterRequest
		crSpec, err := reg.ClusterRequestSpec()
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("unable to generate ClusterRequest spec from registration for request '%s' with id '%s': %w", request.String(), reg.ID(), err)
		}
		if crSpec != nil {
			rlog.Debug("Ensuring ClusterRequest exists")
			cr = &clustersv1alpha1.ClusterRequest{}
			cr.Name = StableRequestName(r.controllerName, request, suffix)
			cr.Namespace = namespace
			// check if ClusterRequest exists
			exists := true
			if err := r.platformClusterClient.Get(rctx, client.ObjectKeyFromObject(cr), cr); err != nil {
				if !apierrors.IsNotFound(err) {
					return reconcile.Result{}, fmt.Errorf("unable to get ClusterRequest '%s/%s': %w", cr.Namespace, cr.Name, err)
				}
				exists = false
			}
			if !exists {
				if inDeletion {
					rlog.Debug("Skipping ClusterRequest creation because this request is in deletion", "crName", cr.Name, "crNamespace", cr.Namespace)
					continue
				}
				rlog.Info("Creating ClusterRequest", "crName", cr.Name, "crNamespace", cr.Namespace)
				cr.Labels = map[string]string{}
				maps.Copy(cr.Labels, expectedLabels)
				controllerutil.AddFinalizer(cr, Finalizer)
				cr.Spec = *crSpec
				if err := r.platformClusterClient.Create(rctx, cr); err != nil {
					return reconcile.Result{}, fmt.Errorf("unable to create ClusterRequest '%s/%s': %w", cr.Namespace, cr.Name, err)
				}
			} else {
				for k, v := range expectedLabels {
					if v2, ok := cr.Labels[k]; !ok || v2 != v {
						if !ok {
							v2 = "<null>"
						}
						return reconcile.Result{}, fmt.Errorf("ClusterRequest '%s/%s' already exists but is not managed by this controller (label '%s' is expected to have value '%s', but is '%s')", cr.Namespace, cr.Name, k, v, v2)
					}
				}
			}
			if !cr.Status.IsGranted() {
				rlog.Info("Waiting for ClusterRequest to be granted", "crName", cr.Name, "crNamespace", cr.Namespace)
				if fc := r.fakingCallbacks[FakingCallback_WaitingForClusterRequestReadiness]; fc != nil {
					rlog.Info("Executing faking callback, this message should only appear in unit tests", "key", FakingCallback_WaitingForClusterRequestReadiness)
					if err := fc(rctx, r.platformClusterClient, FakingCallback_WaitingForClusterRequestReadiness, &request, cr, nil, nil, nil); err != nil {
						return reconcile.Result{}, fmt.Errorf("faking callback for key '%s' failed: %w", FakingCallback_WaitingForClusterRequestReadiness, err)
					}
				}
				return reconcile.Result{RequeueAfter: r.interval}, nil
			}
			rlog.Debug("ClusterRequest is granted", "crName", cr.Name, "crNamespace", cr.Namespace)
		}

		// if an AccessRequest is requested, ensure it exists and is ready
		ar := &clustersv1alpha1.AccessRequest{}
		ar.Name = StableRequestName(r.controllerName, request, suffix)
		ar.Namespace = namespace
		if reg.AccessRequestAvailable() {
			rlog.Debug("Ensuring AccessRequest exists")
			// check if AccessRequest exists
			exists := true
			if err := r.platformClusterClient.Get(rctx, client.ObjectKeyFromObject(ar), ar); err != nil {
				if !apierrors.IsNotFound(err) {
					return reconcile.Result{}, fmt.Errorf("unable to get AccessRequest '%s/%s': %w", ar.Namespace, ar.Name, err)
				}
				exists = false
			}
			if !exists {
				if inDeletion {
					rlog.Debug("Skipping AccessRequest creation because this request is in deletion", "arName", ar.Name, "arNamespace", ar.Namespace)
					continue
				}
				rlog.Info("Creating AccessRequest", "arName", ar.Name, "arNamespace", ar.Namespace)
				// cluster and cluster request references are immutable, so set them only when creating new AccessRequests
				referenced := false
				if cr != nil {
					ar.Spec.RequestRef = &commonapi.ObjectReference{
						Name:      cr.Name,
						Namespace: cr.Namespace,
					}
					referenced = true
				}
				if !referenced {
					cRef, err := reg.ClusterReference()
					if err != nil {
						return reconcile.Result{}, fmt.Errorf("unable to generate Cluster reference from registration for request '%s' with id '%s': %w", request.String(), reg.ID(), err)
					}
					if cRef != nil {
						ar.Spec.ClusterRef = cRef
						referenced = true
					}
				}
				if !referenced {
					crRef, err := reg.ClusterRequestReference()
					if err != nil {
						return reconcile.Result{}, fmt.Errorf("unable to generate ClusterRequest reference from registration for request '%s' with id '%s': %w", request.String(), reg.ID(), err)
					}
					if crRef != nil {
						ar.Spec.RequestRef = crRef
						referenced = true
					}
				}
				if !referenced {
					return reconcile.Result{}, fmt.Errorf("no ClusterRequestSpec, ClusterReference or ClusterRequestReference specified for registration with id '%s'", reg.ID())
				}
			} else {
				for k, v := range expectedLabels {
					if v2, ok := ar.Labels[k]; !ok || v2 != v {
						if !ok {
							v2 = "<null>"
						}
						return reconcile.Result{}, fmt.Errorf("AccessRequest '%s/%s' already exists but is not managed by this controller (label '%s' is expected to have value '%s', but is '%s')", ar.Namespace, ar.Name, k, v, v2)
					}
				}
				rlog.Debug("Updating AccessRequest", "arName", ar.Name, "arNamespace", ar.Namespace)
			}
			if exists && !ar.DeletionTimestamp.IsZero() {
				rlog.Info("Not updating AccessRequest because it is being deleted", "arName", ar.Name, "arNamespace", ar.Namespace)
			} else {
				if _, err := controllerutil.CreateOrUpdate(rctx, r.platformClusterClient, ar, func() error {
					ar.Labels = maputils.Merge(ar.Labels, expectedLabels)
					controllerutil.AddFinalizer(ar, Finalizer)
					var err error
					ar.Spec.Token, err = reg.AccessRequestTokenConfig()
					if err != nil {
						return fmt.Errorf("unable to generate AccessRequest token config from registration: %w", err)
					}
					ar.Spec.OIDC, err = reg.AccessRequestOIDCConfig()
					if err != nil {
						return fmt.Errorf("unable to generate AccessRequest OIDC config from registration: %w", err)
					}
					return nil
				}); err != nil {
					return reconcile.Result{}, fmt.Errorf("unable to create or update AccessRequest '%s/%s': %w", ar.Namespace, ar.Name, err)
				}
				if !ar.Status.IsGranted() || ar.Status.ObservedGeneration != ar.Generation {
					rlog.Info("Waiting for AccessRequest to be granted and up-to-date", "arName", ar.Name, "arNamespace", ar.Namespace)
					if fc := r.fakingCallbacks[FakingCallback_WaitingForAccessRequestReadiness]; fc != nil {
						rlog.Info("Executing faking callback, this message should only appear in unit tests", "key", FakingCallback_WaitingForAccessRequestReadiness)
						if err := fc(rctx, r.platformClusterClient, FakingCallback_WaitingForAccessRequestReadiness, &request, cr, ar, nil, nil); err != nil {
							return reconcile.Result{}, fmt.Errorf("faking callback for key '%s' failed: %w", FakingCallback_WaitingForAccessRequestReadiness, err)
						}
					}
					res.RequeueAfter = r.interval
					continue
				}
			}
		} else {
			// no AccessRequest requested
			// check if there was a previously requested one that needs to be cleaned up
			// this can happen if the registration was updated
			rlog.Info("Deleting previously created AccessRequest that is no longer required, if any", "arName", ar.Name, "arNamespace", ar.Namespace)
			res2, err2 := r.ensureAccessRequestDeletion(rctx, ar, request, expectedLabels)
			if err2 != nil {
				return reconcile.Result{}, err2
			}
			if res2.RequeueAfter > 0 {
				if res2.RequeueAfter < res.RequeueAfter || res.RequeueAfter == 0 {
					res.RequeueAfter = res2.RequeueAfter
				}
				continue
			}
		}
	}
	log.Info("Successfully reconciled cluster access")

	return res, nil
}

// ReconcileDelete implements Reconciler.
//
//nolint:gocyclo
func (r *reconcilerImpl) ReconcileDelete(ctx context.Context, request reconcile.Request, additionalData ...any) (reconcile.Result, error) {
	log := logging.FromContextOrDiscard(ctx).WithName(caControllerName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Reconciling cluster access")
	r.setInDeletion(request)

	// iterate over registrations and ensure that the corresponding resources are being deleted
	res := reconcile.Result{}
	for _, rawReg := range r.registrations {
		rlog := log.WithValues("id", rawReg.ID())
		rctx := logging.NewContext(ctx, rlog)
		rlog.Debug("Processing registration")
		suffix := rawReg.Suffix()
		if suffix == "" {
			suffix = rawReg.ID()
		}
		reg := rawReg.Parameterize(request, additionalData...)
		managedBy, managedPurpose, additionalLabels := r.managedBy(r.controllerName, request, reg)
		expectedLabels := map[string]string{}
		expectedLabels[openmcpconst.ManagedByLabel] = managedBy
		expectedLabels[openmcpconst.ManagedPurposeLabel] = managedPurpose
		expectedLabels = maputils.Merge(additionalLabels, expectedLabels)

		namespace, err := reg.Namespace()
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("unable to generate name of platform cluster namespace for request '%s': %w", request.String(), err)
		}

		// if an AccessRequest is requested, ensure it is deleted
		if reg.AccessRequestAvailable() { //nolint:dupl
			rlog.Debug("Ensuring AccessRequest is deleted")
			ar := &clustersv1alpha1.AccessRequest{}
			ar.Name = StableRequestName(r.controllerName, request, suffix)
			ar.Namespace = namespace
			// check if AccessRequest exists
			res2, err := r.ensureAccessRequestDeletion(rctx, ar, request, expectedLabels)
			if err != nil {
				return reconcile.Result{}, err
			}
			if res2.RequeueAfter > 0 {
				if res2.RequeueAfter < res.RequeueAfter || res.RequeueAfter == 0 {
					res.RequeueAfter = res2.RequeueAfter
				}
				continue
			}
		}

		// if a ClusterRequest is requested, ensure it is deleted
		if reg.ClusterRequestAvailable() { //nolint:dupl
			rlog.Debug("Ensuring ClusterRequest is deleted")
			cr := &clustersv1alpha1.ClusterRequest{}
			cr.Name = StableRequestName(r.controllerName, request, suffix)
			cr.Namespace = namespace
			// check if ClusterRequest exists
			if err := r.platformClusterClient.Get(rctx, client.ObjectKeyFromObject(cr), cr); err != nil {
				if !apierrors.IsNotFound(err) {
					return reconcile.Result{}, fmt.Errorf("unable to get ClusterRequest '%s/%s': %w", cr.Namespace, cr.Name, err)
				}
				rlog.Debug("ClusterRequest already deleted", "crName", cr.Name, "crNamespace", cr.Namespace)
			} else {
				managed := true
				for k, v := range expectedLabels {
					if v2, ok := cr.Labels[k]; !ok || v2 != v {
						managed = false
						break
					}
				}
				if !managed {
					rlog.Info("Skipping ClusterRequest '%s/%s' deletion because it is not managed by this controller", cr.Namespace, cr.Name)
				} else {
					if cr.DeletionTimestamp.IsZero() {
						rlog.Info("Deleting ClusterRequest", "crName", cr.Name, "crNamespace", cr.Namespace)
						if err := r.platformClusterClient.Delete(rctx, cr); err != nil {
							return reconcile.Result{}, fmt.Errorf("unable to delete ClusterRequest '%s/%s': %w", cr.Namespace, cr.Name, err)
						}
					} else {
						if fc := r.fakingCallbacks[FakingCallback_WaitingForClusterRequestDeletion]; fc != nil {
							rlog.Info("Executing faking callback, this message should only appear in unit tests", "key", FakingCallback_WaitingForClusterRequestDeletion)
							if err := fc(rctx, r.platformClusterClient, FakingCallback_WaitingForClusterRequestDeletion, &request, cr, nil, nil, nil); err != nil {
								return reconcile.Result{}, fmt.Errorf("faking callback for key '%s' failed: %w", FakingCallback_WaitingForClusterRequestDeletion, err)
							}
						}
					}
					if len(cr.Finalizers) == 1 && cr.Finalizers[0] == Finalizer {
						// remove finalizer to allow deletion
						rlog.Info("Removing clusteraccess finalizer from ClusterRequest to allow deletion", "crName", cr.Name, "crNamespace", cr.Namespace, "finalizer", Finalizer)
						if err := r.platformClusterClient.Patch(rctx, cr, client.RawPatch(types.JSONPatchType, []byte(`[{"op": "remove", "path": "/metadata/finalizers"}]`))); err != nil {
							return reconcile.Result{}, fmt.Errorf("error removing finalizer from ClusterRequest '%s/%s': %w", cr.Namespace, cr.Name, err)
						}
					} else {
						rlog.Info("Waiting for finalizers to be removed from ClusterRequest", "crName", cr.Name, "crNamespace", cr.Namespace, "finalizers", cr.Finalizers)
						res.RequeueAfter = r.interval
						continue
					}
				}
			}
		}

		// don't delete the namespace, because it might contain other resources
	}
	log.Info("Successfully reconciled cluster access", "requeueAfter", res.RequeueAfter)
	if res.RequeueAfter == 0 {
		r.unsetInDeletion(request)
	}

	return res, nil
}

// Register implements Reconciler.
func (r *reconcilerImpl) Register(reg ClusterRegistration) ClusterAccessReconciler {
	if reg == nil {
		return r
	}
	r.registrations[reg.ID()] = reg
	return r
}

// Unregister implements Reconciler.
func (r *reconcilerImpl) Unregister(id string) ClusterAccessReconciler {
	delete(r.registrations, id)
	return r
}

// WithManagedLabels implements Reconciler.
func (r *reconcilerImpl) WithManagedLabels(gen ManagedLabelGenerator) ClusterAccessReconciler {
	if gen == nil {
		gen = DefaultManagedLabelGenerator
	}
	r.managedBy = ensureManagedLabelLength(gen)
	return r
}

// WithRetryInterval implements Reconciler.
func (r *reconcilerImpl) WithRetryInterval(interval time.Duration) ClusterAccessReconciler {
	r.interval = interval
	return r
}

// Update implements Reconciler.
func (r *reconcilerImpl) Update(id string, updates ...ClusterRegistrationUpdate) error {
	reg, ok := r.registrations[id]
	if !ok {
		return fmt.Errorf("no registration found with id '%s'", id)
	}
	ureg, ok := reg.(UpdatableClusterRegistration)
	if !ok {
		return fmt.Errorf("registration with id '%s' does not support updates", id)
	}
	var errs error
	for _, update := range updates {
		if err := update.Apply(ureg); err != nil {
			errs = errors.Join(errs, fmt.Errorf("unable to apply update to registration with id '%s': %w", id, err))
		}
	}
	return errs
}

// WithFakingCallback implements Reconciler.
func (r *reconcilerImpl) WithFakingCallback(key string, callback FakingCallback) ClusterAccessReconciler {
	if r.fakingCallbacks == nil {
		r.fakingCallbacks = map[string]FakingCallback{}
	}
	r.fakingCallbacks[key] = callback
	return r
}

// WithFakeClientGenerator implements Reconciler.
func (r *reconcilerImpl) WithFakeClientGenerator(gen FakeClientGenerator) ClusterAccessReconciler {
	r.generateFakeClient = gen
	return r
}

const (
	// FakingCallback_WaitingForAccessRequestReadiness is a key for a faking callback that is called when the reconciler is waiting for the AccessRequest to be granted.
	// Note that the execution happens directly before the return of the reconcile function (with a requeue). This means that the reconciliation needs to run a second time to pick up the changes made in the callback.
	FakingCallback_WaitingForAccessRequestReadiness = "WaitingForAccessRequestReadiness"
	// FakingCallback_WaitingForClusterRequestReadiness is a key for a faking callback that is called when the reconciler is waiting for the ClusterRequest to be granted.
	// Note that the execution happens directly before the return of the reconcile function (with a requeue). This means that the reconciliation needs to run a second time to pick up the changes made in the callback.
	FakingCallback_WaitingForClusterRequestReadiness = "WaitingForClusterRequestReadiness"
	// FakingCallback_WaitingForAccessRequestDeletion is a key for a faking callback that is called when the reconciler is waiting for the AccessRequest to be deleted.
	// Note that the execution happens directly before the return of the reconcile function (with a requeue). This means that the reconciliation needs to run a second time to pick up the changes made in the callback.
	FakingCallback_WaitingForAccessRequestDeletion = "WaitingForAccessRequestDeletion"
	// FakingCallback_WaitingForClusterRequestDeletion is a key for a faking callback that is called when the reconciler is waiting for the ClusterRequest to be deleted.
	// Note that the execution happens directly before the return of the reconcile function (with a requeue). This means that the reconciliation needs to run a second time to pick up the changes made in the callback.
	FakingCallback_WaitingForClusterRequestDeletion = "WaitingForClusterRequestDeletion"
)

func (r *reconcilerImpl) isInDeletion(req reconcile.Request) bool {
	r.ridLock.RLock()
	defer r.ridLock.RUnlock()
	return r.requestsInDeletion.Has(req)
}

func (r *reconcilerImpl) setInDeletion(req reconcile.Request) {
	r.ridLock.Lock()
	defer r.ridLock.Unlock()
	r.requestsInDeletion.Insert(req)
}

func (r *reconcilerImpl) unsetInDeletion(req reconcile.Request) {
	r.ridLock.Lock()
	defer r.ridLock.Unlock()
	r.requestsInDeletion.Delete(req)
}

func (r *reconcilerImpl) ensureAccessRequestDeletion(ctx context.Context, ar *clustersv1alpha1.AccessRequest, request reconcile.Request, expectedLabels map[string]string) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx)
	res := reconcile.Result{}
	// check if AccessRequest exists
	if err := r.platformClusterClient.Get(ctx, client.ObjectKeyFromObject(ar), ar); err != nil {
		if !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("unable to get AccessRequest '%s/%s': %w", ar.Namespace, ar.Name, err)
		}
		log.Debug("AccessRequest already deleted", "arName", ar.Name, "arNamespace", ar.Namespace)
	} else {
		managed := true
		for k, v := range expectedLabels {
			if v2, ok := ar.Labels[k]; !ok || v2 != v {
				managed = false
				break
			}
		}
		if !managed {
			log.Info("Skipping AccessRequest '%s/%s' deletion because it is not managed by this controller", ar.Namespace, ar.Name)
		} else {
			if ar.DeletionTimestamp.IsZero() {
				log.Info("Deleting AccessRequest", "arName", ar.Name, "arNamespace", ar.Namespace)
				if err := r.platformClusterClient.Delete(ctx, ar); err != nil {
					return reconcile.Result{}, fmt.Errorf("unable to delete AccessRequest '%s/%s': %w", ar.Namespace, ar.Name, err)
				}
			} else {
				if fc := r.fakingCallbacks[FakingCallback_WaitingForAccessRequestDeletion]; fc != nil {
					log.Info("Executing faking callback, this message should only appear in unit tests", "key", FakingCallback_WaitingForAccessRequestDeletion)
					if err := fc(ctx, r.platformClusterClient, FakingCallback_WaitingForAccessRequestDeletion, &request, nil, ar, nil, nil); err != nil {
						return reconcile.Result{}, fmt.Errorf("faking callback for key '%s' failed: %w", FakingCallback_WaitingForAccessRequestDeletion, err)
					}
				}
			}
			if len(ar.Finalizers) == 1 && ar.Finalizers[0] == Finalizer {
				// remove finalizer to allow deletion
				log.Info("Removing clusteraccess finalizer from AccessRequest to allow deletion", "arName", ar.Name, "arNamespace", ar.Namespace, "finalizer", Finalizer)
				if err := r.platformClusterClient.Patch(ctx, ar, client.RawPatch(types.JSONPatchType, []byte(`[{"op": "remove", "path": "/metadata/finalizers"}]`))); err != nil {
					return reconcile.Result{}, fmt.Errorf("error removing finalizer from AccessRequest '%s/%s': %w", ar.Namespace, ar.Name, err)
				}
			} else {
				log.Info("Waiting for finalizers to be removed from AccessRequest", "arName", ar.Name, "arNamespace", ar.Namespace, "finalizers", ar.Finalizers)
				res.RequeueAfter = r.interval
			}
		}
	}
	return res, nil
}
