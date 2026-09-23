package controlplane

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	errutils "github.com/openmcp-project/controller-utils/pkg/errors"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	cconst "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
)

// deleteDependingServices lists all service providers and checks for all of their registered service resources whether there exists one for the given MCP. If so, it triggers their deletion.
// It returns a set of service provider names for which still resources exist (should be in deletion by the time this function returns) and the total number of resources that are still left.
// Deletion of the MCP should wait until the set is empty and the count is zero.
func (r *ManagedControlPlaneReconciler) deleteDependingServices(ctx context.Context, mcp *corev2alpha1.ControlPlane) (map[string][]*unstructured.Unstructured, errutils.ReasonableError) {
	log := logging.FromContextOrPanic(ctx)

	// delete depending service resources, if any

	if mcp == nil {
		log.Debug("MCP is nil, no need to check for services")
		return nil, nil
	}

	// list all ServiceProvider resources to identify service resources
	sps := &providerv1alpha1.ServiceProviderList{}
	if err := r.PlatformCluster.Client().List(ctx, sps); err != nil {
		return nil, errutils.WithReason(fmt.Errorf("failed to list ServiceProviders: %w", err), cconst.ReasonPlatformClusterInteractionProblem)
	}

	// fetch service resources, if any exist
	resources := map[string][]*unstructured.Unstructured{}
	errs := errutils.NewReasonableErrorList()
	for _, sp := range sps.Items {
		if len(sp.Status.Resources) == 0 {
			log.Debug("ServiceProvider has no registered service resources", "providerName", sp.Name)
			continue
		}
		serviceResources := []*unstructured.Unstructured{}
		for _, resourceType := range sp.Status.Resources {
			res := &unstructured.Unstructured{}
			res.SetAPIVersion(resourceType.Group + "/" + resourceType.Version)
			res.SetKind(resourceType.Kind)
			res.SetName(mcp.Name)
			res.SetNamespace(mcp.Namespace)
			if err := r.OnboardingCluster.Client().Get(ctx, client.ObjectKeyFromObject(res), res); err != nil {
				// In the multicluster (kcp) mode a tenant workspace only serves the
				// APIs it has bound: an unbound service API cannot hold instances,
				// so a missing kind means "no service resources", not an error.
				if !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
					errs.Append(errutils.WithReason(fmt.Errorf("error getting service resource [%s.%s] '%s/%s' for ServiceProvider '%s': %w", res.GetKind(), res.GetAPIVersion(), res.GetNamespace(), res.GetName(), sp.Name, err), cconst.ReasonOnboardingClusterInteractionProblem))
				}
				continue
			}
			log.Debug("Found service resource for ServiceProvider", "resourceKind", res.GetKind(), "resourceAPIVersion", res.GetAPIVersion(), "providerName", sp.Name)
			serviceResources = append(serviceResources, res)
		}

		resources[sp.Name] = serviceResources
	}
	if rerr := errs.Aggregate(); rerr != nil {
		return nil, rerr
	}

	// delete service resources
	errs = errutils.NewReasonableErrorList()
	remainingResources := map[string][]*unstructured.Unstructured{}
	for providerName, serviceResources := range resources {
		if len(serviceResources) == 0 {
			log.Debug("No remaining service resources found for ServiceProvider", "providerName", providerName)
			continue
		}
		remainingServiceResources := []*unstructured.Unstructured{}
		for _, res := range serviceResources {
			if !res.GetDeletionTimestamp().IsZero() {
				log.Debug("Service resource already marked for deletion", "resourceKind", res.GetKind(), "resourceAPIVersion", res.GetAPIVersion(), "provider", providerName)
				remainingServiceResources = append(remainingServiceResources, res)
				continue
			}
			log.Info("Deleting service resource", "resourceKind", res.GetKind(), "resourceAPIVersion", res.GetAPIVersion(), "provider", providerName)
			if err := r.OnboardingCluster.Client().Delete(ctx, res); err != nil {
				if !apierrors.IsNotFound(err) {
					errs.Append(errutils.WithReason(fmt.Errorf("error deleting service resource [%s.%s] '%s/%s' for ServiceProvider '%s': %w", res.GetKind(), res.GetAPIVersion(), res.GetNamespace(), res.GetName(), providerName, err), cconst.ReasonOnboardingClusterInteractionProblem))
				} else {
					log.Debug("Service resource not found during deletion", "resourceKind", res.GetKind(), "resourceAPIVersion", res.GetAPIVersion(), "provider", providerName)
				}
				continue
			}
			remainingServiceResources = append(remainingServiceResources, res)
		}
		if len(remainingServiceResources) > 0 {
			remainingResources[providerName] = remainingServiceResources
		}
	}
	if rerr := errs.Aggregate(); rerr != nil {
		return remainingResources, rerr
	}

	return remainingResources, nil
}
