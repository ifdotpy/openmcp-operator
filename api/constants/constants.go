package constants

const (
	// OpenMCPGroupName is the base API group name for the open control plane.
	OpenMCPGroupName = "openmcp.cloud"
	// OpenControlPlaneGroup is the new group name.
	OpenControlPlaneGroup = "open-control-plane.io"

	// ClusterLabel can be used on CRDs to indicate onto which cluster they should be deployed.
	ClusterLabel = OpenMCPGroupName + "/cluster"

	// OperationAnnotation is used to trigger specific operations on resources.
	OperationAnnotation = OpenMCPGroupName + "/operation"
	// OperationAnnotationValueIgnore is used to ignore the resource.
	OperationAnnotationValueIgnore = "ignore"
	// OperationAnnotationValueReconcile is used to trigger a reconcile on the resource.
	OperationAnnotationValueReconcile = "reconcile"

	// ManagedByLabel is used to indicate which controller manages the resource.
	ManagedByLabel = OpenMCPGroupName + "/managed-by"
	// ManagedPurposeLabel is used to indicate the purpose of the resource.
	ManagedPurposeLabel = OpenMCPGroupName + "/managed-purpose"
	// EnvironmentLabel is used to indicate the environment of the resource.
	EnvironmentLabel = OpenMCPGroupName + "/environment"

	// OnboardingNameLabel is used to store the name on the onboarding cluster of a resource.
	OnboardingNameLabel = OpenMCPGroupName + "/onboarding-name"
	// OnboardingNamespaceLabel is used to store the namespace on the onboarding cluster of a resource.
	OnboardingNamespaceLabel = OpenMCPGroupName + "/onboarding-namespace"

	// TopologyLabel is used to indicate the topology name of the resource.
	TopologyLabel = OpenMCPGroupName + "/topology"
	// TopologyNamespaceLabel is used to indicate the topology namespace of the resource.
	TopologyNamespaceLabel = TopologyLabel + "-ns"

	// EnvVariablePodName is the name of an environment variable passed to providers.
	// Its value is the name of the pod in which the provider is running.
	EnvVariablePodName = "POD_NAME"
	// EnvVariablePodNamespace is the name of an environment variable passed to providers.
	// Its value is the namespace of the pod in which the pod is running.
	EnvVariablePodNamespace = "POD_NAMESPACE"
	// EnvVariablePodIP is the name of an environment variable passed to providers.
	// Its value is the IP address of the pod in which the provider is running.
	EnvVariablePodIP = "POD_IP"
	// EnvVariablePodServiceAccountName is the name of an environment variable passed to providers.
	// Its value is the name of the service account under which the pod is running.
	EnvVariablePodServiceAccountName = "POD_SERVICE_ACCOUNT_NAME"

	// MetricsPortName is the name of the controller-runtime metrics endpoint
	MetricsPortName = "metrics-http"
	// MetricsPathDefault is the name of the default controller-runtime metrics path
	MetricsPathDefault = "/metrics"
	// MetricsPortDefault is the default port used for the metrics endpoint
	MetricsPortDefault int32 = 8080
	// MetricsPrometheusAnnotation is the prefix for the metrics pod annotations
	MetricsPrometheusAnnotation = "prometheus.io"
	// MetricsEnabledAnnotation defines the metrics enabled annotation
	MetricsEnabledAnnotation = MetricsPrometheusAnnotation + "/scrape"
	// MetricsPortAnnotation defines the metrics port annotation
	MetricsPortAnnotation = MetricsPrometheusAnnotation + "/port"
	// MetricsPathAnnotation defines the metrics path annotation
	MetricsPathAnnotation = MetricsPrometheusAnnotation + "/path"

	// MetadataAnnotationLabelPrefix is a prefix for metadata annotations/labels.
	// Annotations and labels with this prefix are propagated between resources in some cases, e.g. from ClusterRequests to the corresponding Cluster resources (for exclusive clusters only).
	MetadataAnnotationLabelPrefix = "metadata." + OpenControlPlaneGroup + "/"
)
