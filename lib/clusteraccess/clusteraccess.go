package clusteraccess

import (
	"context"
	"fmt"
	"time"

	"github.com/openmcp-project/controller-utils/pkg/logging"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/resources"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	constv1alpha1 "github.com/openmcp-project/openmcp-operator/api/constants"
	"github.com/openmcp-project/openmcp-operator/lib/clusteraccess/advanced"
	libutils "github.com/openmcp-project/openmcp-operator/lib/utils"
)

const (
	idMCP          = "mcp"
	suffixMCP      = "mcp"
	idWorkload     = "workload"
	suffixWorkload = "wl"
)

// Reconciler is an interface for reconciling access to open control plane clusters.
// It creates and manages AccessRequests and ClusterRequests for MCP and Workload clusters.
// The Reconciler is being used by service providers to manager the access to an MCP and Workload cluster
// that are being manged for an instance of a service provider resource.
type Reconciler interface {
	// WithRetryInterval sets the retry interval
	WithRetryInterval(interval time.Duration) Reconciler
	// WithMCPPermissions sets the permissions for the MCP AccessRequest.
	WithMCPPermissions(permissions []clustersv1alpha1.PermissionsRequest) Reconciler
	// WithMCPRoleRefs sets the RoleRefs for the MCP AccessRequest.
	WithMCPRoleRefs(roleRefs []commonapi.RoleRef) Reconciler
	// WithWorkloadPermissions sets the permissions for the Workload AccessRequest.
	WithWorkloadPermissions(permissions []clustersv1alpha1.PermissionsRequest) Reconciler
	// WithWorkloadRoleRefs sets the RoleRefs for the Workload AccessRequest.
	WithWorkloadRoleRefs(roleRefs []commonapi.RoleRef) Reconciler
	// WithMCPScheme sets the scheme for the MCP Kubernetes client.
	WithMCPScheme(scheme *runtime.Scheme) Reconciler
	// WithWorkloadScheme sets the scheme for the Workload Kubernetes client.
	WithWorkloadScheme(scheme *runtime.Scheme) Reconciler

	// SkipWorkloadCluster disables the request of a Workload cluster.
	SkipWorkloadCluster() Reconciler

	// MCPCluster creates a Cluster for the MCP AccessRequest.
	// This function will only be successful if the MCP AccessRequest is granted and Reconcile returned without an error
	// and a reconcile.Result with no RequeueAfter value.
	MCPCluster(ctx context.Context, request reconcile.Request) (*clusters.Cluster, error)
	// MCPAccessRequest returns the AccessRequest for the MCP cluster.
	MCPAccessRequest(ctx context.Context, request reconcile.Request) (*clustersv1alpha1.AccessRequest, error)
	// WorkloadCluster creates a Cluster for the Workload AccessRequest.
	// This function will only be successful if the Workload AccessRequest is granted and Reconcile returned without an error
	// and a reconcile.Result with no RequeueAfter value.
	WorkloadCluster(ctx context.Context, request reconcile.Request) (*clusters.Cluster, error)
	// WorkloadAccessRequest returns the AccessRequest for the Workload cluster.
	WorkloadAccessRequest(ctx context.Context, request reconcile.Request) (*clustersv1alpha1.AccessRequest, error)

	// Reconcile creates the ClusterRequests and AccessRequests for the MCP and Workload clusters based on the reconciled object.
	// This function should be called during all reconciliations of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	// The duration is set by the WithRetryInterval method.
	Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error)
	// ReconcileDelete deletes the AccessRequests and ClusterRequests for the MCP and Workload clusters based on the reconciled object.
	// This function should be called during the deletion of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled.
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	// The duration is set by the WithRetryInterval method.
	ReconcileDelete(ctx context.Context, request reconcile.Request) (reconcile.Result, error)
}

type reconcilerImpl struct {
	internal            advanced.ClusterAccessReconciler
	controllerName      string
	mcpPermissions      []clustersv1alpha1.PermissionsRequest
	mcpRoleRefs         []commonapi.RoleRef
	workloadPermissions []clustersv1alpha1.PermissionsRequest
	workloadRoleRefs    []commonapi.RoleRef
	mcpScheme           *runtime.Scheme
	workloadScheme      *runtime.Scheme
}

// NewClusterAccessReconciler creates a new ClusterAccessReconciler with the given parameters.
// platformClusterClient is the client to the platform cluster where the AccessRequests and ClusterRequests are created.
// controllerName is the name of the Kubernetes controller, used to create stable request names.
func NewClusterAccessReconciler(platformClusterClient client.Client, controllerName string) Reconciler {
	rec := advanced.NewClusterAccessReconciler(platformClusterClient, controllerName).WithManagedLabels(func(controllerName string, req reconcile.Request, reg advanced.ClusterRegistration) (string, string, map[string]string) {
		_, managedPurpose, _ := advanced.DefaultManagedLabelGenerator(controllerName, req, reg)
		return controllerName, managedPurpose, map[string]string{
			constv1alpha1.OnboardingNameLabel:      req.Name,
			constv1alpha1.OnboardingNamespaceLabel: req.Namespace,
		}
	})
	return &reconcilerImpl{
		internal:            rec,
		controllerName:      controllerName,
		mcpPermissions:      []clustersv1alpha1.PermissionsRequest{},
		mcpRoleRefs:         []commonapi.RoleRef{},
		workloadPermissions: []clustersv1alpha1.PermissionsRequest{},
		workloadRoleRefs:    []commonapi.RoleRef{},
		mcpScheme:           runtime.NewScheme(),
		workloadScheme:      runtime.NewScheme(),
	}
}

func (r *reconcilerImpl) registerMCP() *reconcilerImpl {
	token := &clustersv1alpha1.TokenConfig{
		Permissions: make([]clustersv1alpha1.PermissionsRequest, len(r.mcpPermissions)),
		RoleRefs:    make([]commonapi.RoleRef, len(r.mcpRoleRefs)),
	}
	copy(token.Permissions, r.mcpPermissions)
	copy(token.RoleRefs, r.mcpRoleRefs)
	r.internal.Register(advanced.ExistingClusterRequest(idMCP, suffixMCP, func(req reconcile.Request, _ ...any) (*commonapi.ObjectReference, error) {
		namespace, err := libutils.StableMCPNamespace(req.Name, req.Namespace)
		if err != nil {
			return nil, err
		}
		return &commonapi.ObjectReference{
			Name:      req.Name,
			Namespace: namespace,
		}, nil
	}).
		WithNamespaceGenerator(advanced.DefaultNamespaceGeneratorForMCP).
		WithTokenAccess(token).
		WithScheme(r.mcpScheme).
		Build())
	return r
}

func (r *reconcilerImpl) registerWorkload() *reconcilerImpl {
	token := &clustersv1alpha1.TokenConfig{
		Permissions: make([]clustersv1alpha1.PermissionsRequest, len(r.workloadPermissions)),
		RoleRefs:    make([]commonapi.RoleRef, len(r.workloadRoleRefs)),
	}
	copy(token.Permissions, r.workloadPermissions)
	copy(token.RoleRefs, r.workloadRoleRefs)
	r.internal.Register(advanced.NewClusterRequest(idWorkload, suffixWorkload, advanced.StaticClusterRequestSpecGenerator(&clustersv1alpha1.ClusterRequestSpec{
		Purpose: clustersv1alpha1.PURPOSE_WORKLOAD,
	})).
		WithNamespaceGenerator(advanced.DefaultNamespaceGeneratorForMCP).
		WithTokenAccess(token).
		WithScheme(r.workloadScheme).
		Build())
	return r
}

func (r *reconcilerImpl) WithRetryInterval(interval time.Duration) Reconciler {
	r.internal.WithRetryInterval(interval)
	return r
}

func (r *reconcilerImpl) WithMCPPermissions(permissions []clustersv1alpha1.PermissionsRequest) Reconciler {
	r.mcpPermissions = permissions
	return r.registerMCP()
}

func (r *reconcilerImpl) WithMCPRoleRefs(roleRefs []commonapi.RoleRef) Reconciler {
	r.mcpRoleRefs = roleRefs
	return r.registerMCP()
}

func (r *reconcilerImpl) WithWorkloadPermissions(permissions []clustersv1alpha1.PermissionsRequest) Reconciler {
	r.workloadPermissions = permissions
	return r.registerWorkload()
}

func (r *reconcilerImpl) WithWorkloadRoleRefs(roleRefs []commonapi.RoleRef) Reconciler {
	r.workloadRoleRefs = roleRefs
	return r.registerWorkload()
}

func (r *reconcilerImpl) WithMCPScheme(scheme *runtime.Scheme) Reconciler {
	r.mcpScheme = scheme
	return r.registerMCP()
}

func (r *reconcilerImpl) WithWorkloadScheme(scheme *runtime.Scheme) Reconciler {
	r.workloadScheme = scheme
	return r.registerWorkload()
}

func (r *reconcilerImpl) SkipWorkloadCluster() Reconciler {
	r.internal.Unregister(idWorkload)
	return r
}

func (r *reconcilerImpl) MCPCluster(ctx context.Context, request reconcile.Request) (*clusters.Cluster, error) {
	return r.internal.Access(ctx, request, idMCP)
}

func (r *reconcilerImpl) MCPAccessRequest(ctx context.Context, request reconcile.Request) (*clustersv1alpha1.AccessRequest, error) {
	return r.internal.AccessRequest(ctx, request, idMCP)
}

func (r *reconcilerImpl) WorkloadCluster(ctx context.Context, request reconcile.Request) (*clusters.Cluster, error) {
	return r.internal.Access(ctx, request, idWorkload)
}

func (r *reconcilerImpl) WorkloadAccessRequest(ctx context.Context, request reconcile.Request) (*clustersv1alpha1.AccessRequest, error) {
	return r.internal.AccessRequest(ctx, request, idWorkload)
}

func (r *reconcilerImpl) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	return r.internal.Reconcile(ctx, request)
}

func (r *reconcilerImpl) ReconcileDelete(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	return r.internal.ReconcileDelete(ctx, request)
}

// Manager is an interface for managing cluster access.
type Manager interface {
	// WithTimeout sets the timeout for operations.
	WithTimeout(timeout time.Duration) Manager
	// WithInterval sets the interval for polling operations.
	WithInterval(interval time.Duration) Manager
	// WithLogger sets the logger for the manager.
	WithLogger(log *logging.Logger) Manager

	// CreateAndWaitForCluster creates a new ClusterRequest/AccessRequest and waits for it to be ready.
	// It returns the created Cluster if the AccessRequest is granted.
	// ctx is the context for the operation.
	// clusterName is the name of the cluster to create.
	// purpose is the purpose of the cluster (e.g., "onboarding", "mcp", "workload").
	// scheme is the runtime scheme to use for the cluster.
	// permissions are the permissions to request for the AccessRequest.
	CreateAndWaitForCluster(ctx context.Context, clusterName, purpose string,
		scheme *runtime.Scheme, permissions []clustersv1alpha1.PermissionsRequest) (*clusters.Cluster, error)

	// WaitForClusterAccess creates or updates an AccessRequest for the given reference and waits until it is granted.
	// localName is used to derive the name of the AccessRequest, which will be "<controllerName>--<localName>".
	// ref is the reference to either a Cluster or a ClusterRequest, depending on refType.
	// refType is either 'Cluster' or 'ClusterRequest' and determines what ref references.
	// permissions are the permissions to request for the AccessRequest.
	WaitForClusterAccess(ctx context.Context, localName string, scheme *runtime.Scheme, ref *commonapi.ObjectReference, refType ClusterReferenceType, permissions []clustersv1alpha1.PermissionsRequest) (*clusters.Cluster, *clustersv1alpha1.AccessRequest, error)

	// Access returns an internal Cluster object granting access to the cluster.
	// Only works after CreateAndWaitForCluster or WaitForClusterAccess returned successfully for the given clusterName.
	Access(ctx context.Context, clusterName string, scheme *runtime.Scheme) (*clusters.Cluster, error)
	// AccessRequest fetches the AccessRequest object for the cluster.
	// Only works after CreateAndWaitForCluster or WaitForClusterAccess returned successfully for the given clusterName.
	AccessRequest(ctx context.Context, clusterName string) (*clustersv1alpha1.AccessRequest, error)
	// ClusterRequest fetches the ClusterRequest object for the cluster.
	// Only works after CreateAndWaitForCluster or WaitForClusterAccess returned successfully for the given clusterName.
	ClusterRequest(ctx context.Context, clusterName string) (*clustersv1alpha1.ClusterRequest, error)
	// Cluster fetches the external Cluster object for the cluster.
	// Only works after CreateAndWaitForCluster or WaitForClusterAccess returned successfully for the given clusterName.
	Cluster(ctx context.Context, clusterName string) (*clustersv1alpha1.Cluster, error)
}

type managerImpl struct {
	platformClusterClient client.Client
	controllerName        string
	controllerNamespace   string // Namespace where the controller is running, used for stable resource names
	timeout               time.Duration
	interval              time.Duration
	log                   *logging.Logger
}

// NewClusterAccessManager creates a new Manager with the given parameters.
func NewClusterAccessManager(platformClusterClient client.Client, controllerName, controllerNamespace string) Manager {
	return &managerImpl{
		platformClusterClient: platformClusterClient,
		controllerName:        controllerName,
		controllerNamespace:   controllerNamespace,
		timeout:               5 * time.Minute,
		interval:              10 * time.Second,
		log:                   nil, // Default to no logging
	}
}

func (m *managerImpl) WithTimeout(timeout time.Duration) Manager {
	m.timeout = timeout
	return m
}

func (m *managerImpl) WithInterval(interval time.Duration) Manager {
	m.interval = interval
	return m
}

func (m *managerImpl) WithLogger(log *logging.Logger) Manager {
	m.log = log
	return m
}

// CreateAndWaitForCluster creates a new ClusterRequest and AccessRequest, waits for it to be ready, and returns the created Cluster.
// The name of the ClusterRequest and AccessRequest is derived from the controller name and the given localName.
// For example, if a provider needs access to the onboarding cluster, it could use the localName "onboarding" (or "onboarding-init").
// The resulting ClusterRequest and AccessRequest would then be named "<controllerName>--onboarding" (or "<controllerName>--onboarding-init").
func (m *managerImpl) CreateAndWaitForCluster(ctx context.Context, localName, purpose string,
	scheme *runtime.Scheme, permissions []clustersv1alpha1.PermissionsRequest) (*clusters.Cluster, error) {

	cr := &clustersv1alpha1.ClusterRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      StableRequestNameFromLocalName(m.controllerName, localName),
			Namespace: m.controllerNamespace,
		},
	}

	clusterRequestMutator := newClusterRequestMutator(cr.Name, cr.Namespace, purpose)
	clusterRequestMutator.WithMetadata(resources.NewMetadataMutator().WithLabels(map[string]string{
		constv1alpha1.ManagedByLabel: m.controllerName,
	}))

	if err := resources.CreateOrUpdateResource(ctx, m.platformClusterClient, clusterRequestMutator); err != nil {
		return nil, fmt.Errorf("failed to create/update ClusterRequest: %w", err)
	}

	err := m.wait(ctx, func(ctx context.Context) (bool, error) {
		if err := m.platformClusterClient.Get(ctx, client.ObjectKeyFromObject(cr), cr); err != nil {
			return false, fmt.Errorf("failed to get ClusterRequest: %w", err)
		}

		if m.log != nil {
			m.log.Info("Waiting for ClusterRequest", "name", cr.Name, "phase", cr.Status.Phase)
		}

		return cr.Status.IsGranted() || cr.Status.IsDenied(), nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to wait for ClusterRequest: %w", err)
	}

	cl, _, err := m.WaitForClusterAccess(ctx, localName, scheme, &commonapi.ObjectReference{
		Name:      cr.Name,
		Namespace: cr.Namespace,
	}, ReferenceToClusterRequest, permissions)
	return cl, err
}

type ClusterReferenceType string

const (
	ReferenceToCluster        ClusterReferenceType = "Cluster"
	ReferenceToClusterRequest ClusterReferenceType = "ClusterRequest"
)

// WaitForClusterAccess creates or updates an AccessRequest for the given reference and waits until it is granted.
// localName is used to derive the name of the AccessRequest, which will be "<controllerName>--<localName>".
// ref is the reference to either a Cluster or a ClusterRequest, depending on refType.
// refType is either 'Cluster' or 'ClusterRequest' and determines what ref references.
// permissions are the permissions to request for the AccessRequest.
func (m *managerImpl) WaitForClusterAccess(ctx context.Context, localName string, scheme *runtime.Scheme, ref *commonapi.ObjectReference, refType ClusterReferenceType, permissions []clustersv1alpha1.PermissionsRequest) (*clusters.Cluster, *clustersv1alpha1.AccessRequest, error) {
	ar := &clustersv1alpha1.AccessRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      StableRequestNameFromLocalName(m.controllerName, localName),
			Namespace: m.controllerNamespace,
		},
	}

	accessRequestMutator := newAccessRequestMutator(ar.Name, ar.Namespace)
	switch refType {
	case ReferenceToCluster:
		accessRequestMutator.WithClusterRef(ref)
	case ReferenceToClusterRequest:
		accessRequestMutator.WithRequestRef(ref)
	default:
		return nil, nil, fmt.Errorf("invalid ClusterReferenceType: %s", refType)
	}
	accessRequestMutator.WithTokenPermissions(permissions)
	accessRequestMutator.WithMetadata(resources.NewMetadataMutator().WithLabels(map[string]string{
		constv1alpha1.ManagedByLabel: m.controllerName,
	}))

	if err := resources.CreateOrUpdateResource(ctx, m.platformClusterClient, accessRequestMutator); err != nil {
		return nil, nil, fmt.Errorf("failed to create/update AccessRequest: %w", err)
	}

	if err := m.wait(ctx, func(ctx context.Context) (bool, error) {
		if err := m.platformClusterClient.Get(ctx, client.ObjectKeyFromObject(ar), ar); err != nil {
			return false, fmt.Errorf("failed to get AccessRequest: %w", err)
		}

		if m.log != nil {
			m.log.Info("Waiting for AccessRequest", "name", ar.Name, "phase", ar.Status.Phase)
		}

		return ar.Status.IsGranted() || ar.Status.IsDenied(), nil
	}); err != nil {
		return nil, ar, fmt.Errorf("failed to wait for AccessRequest: %w", err)
	}

	cl, err := createClusterForAccessRequest(ctx, m.platformClusterClient, localName, scheme, ar)
	return cl, ar, err
}

func (m *managerImpl) wait(ctx context.Context, test func(ctx context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	return wait.PollUntilContextTimeout(ctx, m.interval, m.timeout, true, test)
}

func createClusterForAccessRequest(ctx context.Context, platformClusterClient client.Client,
	clusterName string, scheme *runtime.Scheme, accessRequest *clustersv1alpha1.AccessRequest) (*clusters.Cluster, error) {

	if accessRequest.Status.SecretRef == nil {
		return nil, fmt.Errorf("access request %q has no accessSecret reference", accessRequest.Name)
	}

	accessSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      accessRequest.Status.SecretRef.Name,
			Namespace: accessRequest.Namespace,
		},
	}

	if err := platformClusterClient.Get(ctx, client.ObjectKeyFromObject(accessSecret), accessSecret); err != nil {
		return nil, fmt.Errorf("failed to get access accessSecret: %w", err)
	}

	kubeconfigBytes, ok := accessSecret.Data["kubeconfig"]
	if !ok {
		return nil, fmt.Errorf("kubeconfig not found in accessSecret %s", accessSecret.Name)
	}

	config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create rest config from kubeconfig bytes: %w", err)
	}

	c := clusters.New(clusterName).WithRESTConfig(config)

	if err = c.InitializeClient(scheme); err != nil {
		return nil, fmt.Errorf("failed to initialize client for cluster %q: %w", clusterName, err)
	}

	return c, nil
}

func (m *managerImpl) Access(ctx context.Context, clusterName string, scheme *runtime.Scheme) (*clusters.Cluster, error) {
	ar, err := m.AccessRequest(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get AccessRequest: %w", err)
	}
	access, err := createClusterForAccessRequest(ctx, m.platformClusterClient, clusterName, scheme, ar)
	if err != nil {
		return nil, fmt.Errorf("failed to create access for AccessRequest: %w", err)
	}
	return access, nil
}

func (m *managerImpl) AccessRequest(ctx context.Context, clusterName string) (*clustersv1alpha1.AccessRequest, error) {
	ar := &clustersv1alpha1.AccessRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      StableRequestNameFromLocalName(m.controllerName, clusterName),
			Namespace: m.controllerNamespace,
		},
	}
	if err := m.platformClusterClient.Get(ctx, client.ObjectKeyFromObject(ar), ar); err != nil {
		return nil, fmt.Errorf("failed to get AccessRequest: %w", err)
	}
	return ar, nil
}

func (m *managerImpl) ClusterRequest(ctx context.Context, clusterName string) (*clustersv1alpha1.ClusterRequest, error) {
	cr := &clustersv1alpha1.ClusterRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      StableRequestNameFromLocalName(m.controllerName, clusterName),
			Namespace: m.controllerNamespace,
		},
	}
	if err := m.platformClusterClient.Get(ctx, client.ObjectKeyFromObject(cr), cr); err != nil {
		return nil, fmt.Errorf("failed to get ClusterRequest: %w", err)
	}
	return cr, nil
}

func (m *managerImpl) Cluster(ctx context.Context, clusterName string) (*clustersv1alpha1.Cluster, error) {
	cr, err := m.ClusterRequest(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClusterRequest: %w", err)
	}
	if cr.Status.Cluster == nil {
		return nil, fmt.Errorf("ClusterRequest '%s/%s' does not reference a Cluster", cr.Namespace, cr.Name)
	}
	cluster := &clustersv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Status.Cluster.Name,
			Namespace: cr.Status.Cluster.Namespace,
		},
	}
	if err := m.platformClusterClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
		return nil, fmt.Errorf("failed to get Cluster: %w", err)
	}
	return cluster, nil
}

type clusterRequestMutator struct {
	name      string
	namespace string
	purpose   string
	metadata  resources.MetadataMutator
}

func newClusterRequestMutator(name, namespace, purpose string) *clusterRequestMutator {
	return &clusterRequestMutator{
		name:      name,
		namespace: namespace,
		purpose:   purpose,
	}
}

func (m *clusterRequestMutator) WithMetadata(metadata resources.MetadataMutator) *clusterRequestMutator {
	m.metadata = metadata
	return m
}

func (m *clusterRequestMutator) String() string {
	return fmt.Sprint("ClusterRequest",
		", name ", m.name,
		", namespace", m.namespace,
		", purpose", m.purpose)
}

func (m *clusterRequestMutator) Empty() *clustersv1alpha1.ClusterRequest {
	return &clustersv1alpha1.ClusterRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.name,
			Namespace: m.namespace,
		},
	}
}

func (m *clusterRequestMutator) MetadataMutator() resources.MetadataMutator {
	return m.metadata
}

func (m *clusterRequestMutator) Mutate(clusterRequest *clustersv1alpha1.ClusterRequest) error {
	clusterRequest.Spec.Purpose = m.purpose

	if m.metadata != nil {
		return m.metadata.Mutate(clusterRequest)
	} else {
		return nil
	}
}

type accessRequestMutator struct {
	name                string
	namespace           string
	requestRef          *commonapi.ObjectReference
	clusterRef          *commonapi.ObjectReference
	tokenPermissions    []clustersv1alpha1.PermissionsRequest
	tokenRoleRefs       []commonapi.RoleRef
	oidcProvider        *commonapi.OIDCProviderConfig
	oidcAdditionalRoles []clustersv1alpha1.PermissionsRequest
	metadata            resources.MetadataMutator
}

func newAccessRequestMutator(name, namespace string) *accessRequestMutator {
	return &accessRequestMutator{
		name:      name,
		namespace: namespace,
	}
}

func (m *accessRequestMutator) WithRequestRef(requestRef *commonapi.ObjectReference) *accessRequestMutator {
	m.requestRef = requestRef
	return m
}

func (m *accessRequestMutator) WithClusterRef(clusterRef *commonapi.ObjectReference) *accessRequestMutator {
	m.clusterRef = clusterRef
	return m
}

func (m *accessRequestMutator) WithTokenPermissions(permissions []clustersv1alpha1.PermissionsRequest) *accessRequestMutator {
	m.tokenPermissions = permissions
	return m
}

func (m *accessRequestMutator) WithTokenRoleRefs(roleRefs []commonapi.RoleRef) *accessRequestMutator {
	m.tokenRoleRefs = roleRefs
	return m
}

func (m *accessRequestMutator) WithOIDCProvider(oidcProvider *commonapi.OIDCProviderConfig) *accessRequestMutator {
	m.oidcProvider = oidcProvider
	return m
}

func (m *accessRequestMutator) WithOIDCAdditionalRoles(permissions []clustersv1alpha1.PermissionsRequest) *accessRequestMutator {
	m.oidcAdditionalRoles = permissions
	return m
}

func (m *accessRequestMutator) WithMetadata(metadata resources.MetadataMutator) *accessRequestMutator {
	m.metadata = metadata
	return m
}

func (m *accessRequestMutator) String() string {
	requestRef := "<nil>"
	clusterRef := "<nil>"

	if m.requestRef != nil {
		requestRef = fmt.Sprintf("%s/%s", m.requestRef.Name, m.requestRef.Namespace)
	}

	if m.clusterRef != nil {
		clusterRef = fmt.Sprintf("%s/%s", m.clusterRef.Name, m.clusterRef.Namespace)
	}

	return fmt.Sprint("AccessRequest",
		", name ", m.name,
		", namespace ", m.namespace,
		", requestRef ", requestRef,
		", clusterRef ", clusterRef)
}

func (m *accessRequestMutator) Empty() *clustersv1alpha1.AccessRequest {
	return &clustersv1alpha1.AccessRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.name,
			Namespace: m.namespace,
		},
	}
}

func (m *accessRequestMutator) MetadataMutator() resources.MetadataMutator {
	return m.metadata
}

func (m *accessRequestMutator) Mutate(accessRequest *clustersv1alpha1.AccessRequest) error {
	if m.tokenPermissions != nil {
		if accessRequest.Spec.Token == nil {
			accessRequest.Spec.Token = &clustersv1alpha1.TokenConfig{}
		}
		accessRequest.Spec.Token.Permissions = make([]clustersv1alpha1.PermissionsRequest, len(m.tokenPermissions))
		copy(accessRequest.Spec.Token.Permissions, m.tokenPermissions)
	}
	if m.tokenRoleRefs != nil {
		if accessRequest.Spec.Token == nil {
			accessRequest.Spec.Token = &clustersv1alpha1.TokenConfig{}
		}
		accessRequest.Spec.Token.RoleRefs = make([]commonapi.RoleRef, len(m.tokenRoleRefs))
		copy(accessRequest.Spec.Token.RoleRefs, m.tokenRoleRefs)
	}

	if m.oidcProvider != nil {
		if accessRequest.Spec.OIDC == nil {
			accessRequest.Spec.OIDC = &clustersv1alpha1.OIDCConfig{}
		}
		accessRequest.Spec.OIDC.OIDCProviderConfig = *m.oidcProvider.DeepCopy()
	}
	if m.oidcAdditionalRoles != nil {
		if accessRequest.Spec.OIDC == nil {
			accessRequest.Spec.OIDC = &clustersv1alpha1.OIDCConfig{}
		}
		accessRequest.Spec.OIDC.Roles = make([]clustersv1alpha1.PermissionsRequest, len(m.oidcAdditionalRoles))
		copy(accessRequest.Spec.OIDC.Roles, m.oidcAdditionalRoles)
	}

	if m.requestRef != nil {
		accessRequest.Spec.RequestRef = m.requestRef.DeepCopy()
	}

	if m.clusterRef != nil {
		accessRequest.Spec.ClusterRef = m.clusterRef.DeepCopy()
	}

	if m.metadata != nil {
		return m.metadata.Mutate(accessRequest)
	} else {
		return nil
	}
}

// StableRequestName generates a stable name for a Cluster- or AccessRequest related to an MCP.
// This basically results in '<lowercase_controller_name>--<request_name>'.
// If the resulting string exceeds the Kubernetes name length limit of 63 characters, it will be truncated with the last characters replaced by a hash of what was removed.
func StableRequestName(controllerName string, request reconcile.Request) string {
	return advanced.StableRequestName(controllerName, request, "")
}

// StableRequestNameFromLocalName works like StableRequestName but takes a local name directly instead of a reconcile.Request.
func StableRequestNameFromLocalName(controllerName, localName string) string {
	return advanced.StableRequestNameFromLocalName(controllerName, localName, "")
}
