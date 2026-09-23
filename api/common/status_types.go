package common

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	// StatusPhaseReady indicates that the resource is ready. All conditions are met and are in status "True".
	StatusPhaseReady = "Ready"
	// StatusPhaseProgressing indicates that the resource is not ready and being created or updated. At least one condition is not met and is in status "False".
	StatusPhaseProgressing = "Progressing"
	// StatusPhaseTerminating indicates that the resource is not ready and in deletion. At least one condition is not met and is in status "False".
	StatusPhaseTerminating = "Terminating"
)

// Status represents the status of an open control plane resource.
type Status struct {
	// ObservedGeneration is the generation of this resource that was last reconciled by the controller.
	ObservedGeneration int64 `json:"observedGeneration"`

	// Phase is the current phase of the resource.
	Phase string `json:"phase"`

	// Conditions contains the conditions.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
