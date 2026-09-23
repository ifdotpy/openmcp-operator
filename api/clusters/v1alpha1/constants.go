package v1alpha1

const (
	// PURPOSE_PLATFORM means platform controllers will run on the cluster.
	PURPOSE_PLATFORM = "platform"
	// PURPOSE_WORKLOAD means workload controllers will run on the cluster.
	PURPOSE_WORKLOAD = "workload"
	// PURPOSE_ONBOARDING means the cluster is used for onboarding resources.
	// Onboarding clusters can be workerless.
	PURPOSE_ONBOARDING = "onboarding"
	// PURPOSE_MCP means the cluster is used as an MCP cluster.
	// MCP clusters can be workerless.
	PURPOSE_MCP = "mcp"
)

const (
	// PHASE_UNKNOWN represents an unknown phase for the cluster.
	PHASE_UNKNOWN string = "Unknown"
	// PHASE_PROGRESSING indicates that the cluster is being created or updated.
	PHASE_PROGRESSING string = "Progressing"
	// PHASE_SUCCEEDED indicates that the cluster is ready.
	PHASE_SUCCEEDED string = "Succeeded"
	// PHASE_FAILED indicates that an error occurred while creating or updating the cluster.
	PHASE_FAILED string = "Failed"
	// PHASE_DELETING indicates that the cluster is being deleted.
	PHASE_DELETING string = "Deleting"
	// PHASE_DELETION_FAILED indicates that an error occurred while deleting the cluster.
	PHASE_DELETION_FAILED string = "DeletionFailed"
)

const (
	// REQUEST_PENDING indicates that the request has neither been granted nor denied yet.
	REQUEST_PENDING string = "Pending"
	// REQUEST_GRANTED indicates that the request has been granted.
	REQUEST_GRANTED string = "Granted"
	// REQUEST_DENIED indicates that the request has been denied.
	REQUEST_DENIED string = "Denied"
)

type Tenancy string

const (
	// TENANCY_SHARED means the cluster is shared among multiple tenants.
	TENANCY_SHARED Tenancy = "Shared"
	// TENANCY_EXCLUSIVE means the cluster is dedicated to a single tenant.
	TENANCY_EXCLUSIVE Tenancy = "Exclusive"
)

const (
	// K8sVersionLabel can be used to display the k8s version of the cluster.
	// This is useful since the cluster spec can contain only a partial version (e.g. "1.23") or no version at all, so the actual version can not be determined from the spec alone.
	// The responsible ClusterProvider has to set this label.
	K8sVersionLabel = GroupName + "/k8sversion"
	// ProviderInfoAnnotation can be used to display provider-specific information about the cluster.
	// The responsible ClusterProvider can set this annotation to display additional information about the cluster.
	// The value will be shown in a column when 'kubectl get clusters -o wide' is used.
	ProviderInfoAnnotation = GroupName + "/providerinfo"
	// ProviderLabel can be used to display the provider of the cluster.
	// It is also used to indicate the provider that is responsible for an AccessRequest.
	// For clusters, the responsible ClusterProvider has to set this label.
	// For AccessRequests, the open control plane operator sets this label.
	ProviderLabel = GroupName + "/provider"

	// DeleteWithoutRequestsLabel marks that the corresponding cluster can be deleted if the scheduler removes the last request pointing to it.
	// Its value must be "true" for the label to take effect.
	DeleteWithoutRequestsLabel = GroupName + "/delete-without-requests"
	// ProfileLabel is used to make the profile information easily accessible on AccessRequests.
	ProfileLabel = GroupName + "/profile"
	// RandomizeClusterNameLabel can be set to "true" on ClusterRequests to have the corresponding Cluster get a randomized name.
	// This is meant as a tool for operators to resolve conflicts, which itself should be rare.
	RandomizeClusterNameLabel = GroupName + "/randomize-cluster-name"
)

const (
	// ClusterRequestFinalizer is the finalizer used on ClusterRequest resources
	ClusterRequestFinalizer = GroupName + "/request"
	// RequestFinalizerOnClusterPrefix is the prefix for the finalizers that mark a Cluster as being referenced by a ClusterRequest.
	RequestFinalizerOnClusterPrefix = "request." + GroupName + "/"
)

const (
	// SecretKeyKubeconfig is the name of the key in the AccessRequest secret that contains the kubeconfig.
	SecretKeyKubeconfig = "kubeconfig"
	// SecretKeyExpirationTimestamp is the name of the key in the AccessRequest secret that contains the expiration timestamp.
	// This value is optional and must not be set for non-expiring authentication methods.
	SecretKeyExpirationTimestamp = "expirationTimestamp"
	// SecretKeyCreationTimestamp is the name of the key in the AccessRequest secret that contains the creation timestamp.
	// This value is optional and must not be set for non-expiring authentication methods.
	SecretKeyCreationTimestamp = "creationTimestamp"
	// SecretKeyClientID is the name of the key in the AccessRequest secret that contains the client ID.
	// This value is optional and must not be set for non-OIDC-based authentication methods.
	SecretKeyClientID = "clientID"
	// SecretKeyHost is the name of the key in the AccessRequest secret that contains the host of the cluster.
	// This value is optional.
	SecretKeyHost = "host"
	// SecretKeyCA is the name of the key in the AccessRequest secret that contains the CA certificate of the cluster.
	// This value is optional.
	SecretKeyCA = "ca.crt"
	// SecretKeyToken is the name of the key in the AccessRequest secret that contains the token.
	// This value is optional.
	SecretKeyToken = "token"
)
