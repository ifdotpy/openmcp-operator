package mcp

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/openmcp-project/controller-utils/pkg/clusteraccess"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
)

const (
	workspaceProviderLabel    = "openmcp.cloud/workspace-provider"
	appNameLabel              = "app.kubernetes.io/name"
	providerClusterRolePrefix = "openmcp-workspace-"
	verbDelete                = "delete"
	verbGet                   = "get"
	verbList                  = "list"
	verbPatch                 = "patch"
	verbUpdate                = "update"
	verbWatch                 = "watch"
)

func (r *workspaceRuntime) ownsWorkspaceRequest(manager string) bool {
	if manager == controlplane.ControllerName {
		return true
	}
	for _, p := range r.providers {
		if manager == p.Resource.Kind || manager == strings.ToLower(p.Resource.Kind)+"."+p.Resource.Group {
			return true
		}
	}
	return false
}
func (r *workspaceRuntime) ensureWorkspaceProviders(ctx context.Context, namespace string) error {
	for _, provider := range r.providers {
		serviceAccount := providerRuntimeRBACName(namespace, provider.Name)
		if err := r.ensureProviderRBAC(ctx, namespace, serviceAccount, provider); err != nil {
			return err
		}
		if provider.RegistrationNamespace != "" {
			continue
		}
		if err := r.ensureProviderDeployment(ctx, namespace, serviceAccount, provider); err != nil {
			return err
		}
	}
	return nil
}

func (r *workspaceRuntime) reconcileServiceProviders(ctx context.Context) error {
	for _, provider := range r.providers {
		if err := r.ensureServiceProvider(ctx, provider); err != nil {
			return err
		}
	}
	// Retain old resource descriptors so restarted runtimes can wait for service
	// finalizers before revoking access. Native provisioning ignores these objects.
	return nil
}

func (r *workspaceRuntime) ensureServiceProvider(ctx context.Context, provider workspaceProvider) error {
	c := r.platform.Client()
	sp := &providerv1alpha1.ServiceProvider{ObjectMeta: metav1.ObjectMeta{Name: provider.ProviderName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, sp, func() error {
		if sp.Labels == nil {
			sp.Labels = map[string]string{}
		}
		sp.Labels[workspaceProviderLabel] = labelValueTrue
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

func providerRuntimeRBACName(namespace, provider string) string {
	digest := sha256.Sum256([]byte(provider))
	namespaceSuffix := namespace
	if len(namespaceSuffix) > 16 {
		namespaceSuffix = namespaceSuffix[len(namespaceSuffix)-16:]
	}
	return fmt.Sprintf("%s%s-%x", providerClusterRolePrefix, namespaceSuffix, digest[:4])
}

func (r *workspaceRuntime) ensureProviderRBAC(ctx context.Context, namespace, name string, provider workspaceProvider) error {
	c := r.platform.Client()
	labels := []clusteraccess.Label{{Key: workspaceRuntimeLabel, Value: namespace}, {Key: appNameLabel, Value: provider.Name}}
	if provider.RegistrationNamespace == "" {
		if _, err := clusteraccess.EnsureServiceAccount(ctx, c, name, namespace, labels...); err != nil {
			return fmt.Errorf("ensure provider ServiceAccount: %w", err)
		}
	}
	subjects := []rbacv1.Subject{{Kind: serviceAccountKind, Name: name, Namespace: namespace}}
	if provider.RegistrationNamespace != "" {
		subjects[0].Name = provider.Name
		subjects[0].Namespace = provider.RegistrationNamespace
	}
	rules := append([]rbacv1.PolicyRule{
		{APIGroups: []string{clustersv1alpha1.GroupVersion.Group}, Resources: []string{"clusters", "clusterrequests", "clusterrequests/status", "accessrequests", "accessrequests/status"}, Verbs: []string{verbGet, verbList, verbWatch, verbCreate, verbUpdate, verbPatch, verbDelete}},
		{APIGroups: []string{""}, Resources: []string{"secrets", "configmaps", "events"}, Verbs: []string{verbGet, verbList, verbWatch, verbCreate, verbUpdate, verbPatch, verbDelete}},
		{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{verbGet, verbList, verbWatch, verbCreate, verbUpdate, verbPatch, verbDelete}},
	}, provider.RoleRules...)
	if _, _, err := clusteraccess.EnsureRoleAndBinding(ctx, c, name, namespace, subjects, rules, labels...); err != nil {
		return fmt.Errorf("ensure provider Role and binding: %w", err)
	}
	if _, _, err := clusteraccess.EnsureClusterRoleAndBinding(ctx, c, name, subjects, provider.ClusterRoleRules, labels...); err != nil {
		return fmt.Errorf("ensure provider ClusterRole and binding: %w", err)
	}
	return nil
}

func (r *workspaceRuntime) ensureProviderDeployment(ctx context.Context, namespace, serviceAccount string, provider workspaceProvider) error {
	c := r.platform.Client()
	labels := map[string]string{appNameLabel: provider.Name, workspaceRuntimeLabel: namespace}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: provider.Name, Namespace: namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, dep, func() error {
		one := int32(1)
		dep.Labels = labels
		dep.Spec.Replicas = &one
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{appNameLabel: provider.Name}}
		dep.Spec.Template.Labels = labels
		dep.Spec.Template.Spec.ServiceAccountName = serviceAccount
		dep.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(65532)), RunAsGroup: ptr.To(int64(65532)), FSGroup: ptr.To(int64(65532)),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		}
		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            controllerName,
			Image:           provider.Image,
			Args:            append([]string{"run", "--environment", r.environment, "--provider-name", provider.ProviderName, "--metrics-bind-address", "0", "--health-probe-bind-address", ":8081"}, provider.Args...),
			Env:             []corev1.EnvVar{{Name: apiconst.EnvVariablePodNamespace, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}},
			Ports:           []corev1.ContainerPort{{Name: "health", ContainerPort: 8081}},
			ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("health")}}},
			LivenessProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("health")}}},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensure %s Deployment: %w", provider.Name, err)
	}
	return nil
}

func (r *workspaceRuntime) pruneProviderRuntime(ctx context.Context, namespace string, desiredProviders, desiredRBAC map[string]struct{}) error {
	c := r.platform.Client()
	deployments := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployments, client.InNamespace(namespace), client.MatchingLabels{workspaceRuntimeLabel: namespace}); err != nil {
		return fmt.Errorf("list provider Deployments: %w", err)
	}
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		if _, keep := desiredProviders[deployment.Name]; keep {
			continue
		}
		if err := c.Delete(ctx, deployment); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete stale provider Deployment %q: %w", deployment.Name, err)
		}
	}
	for _, list := range []client.ObjectList{&corev1.ServiceAccountList{}, &rbacv1.RoleList{}, &rbacv1.RoleBindingList{}, &rbacv1.ClusterRoleList{}, &rbacv1.ClusterRoleBindingList{}} {
		options := []client.ListOption{client.MatchingLabels{workspaceRuntimeLabel: namespace}}
		if _, clusterScoped := list.(*rbacv1.ClusterRoleList); !clusterScoped {
			if _, clusterScoped = list.(*rbacv1.ClusterRoleBindingList); !clusterScoped {
				options = append(options, client.InNamespace(namespace))
			}
		}
		if err := c.List(ctx, list, options...); err != nil {
			return fmt.Errorf("list provider RBAC: %w", err)
		}
		objects, err := meta.ExtractList(list)
		if err != nil {
			return fmt.Errorf("read provider RBAC: %w", err)
		}
		for _, item := range objects {
			object := item.(client.Object)
			if _, keep := desiredRBAC[object.GetName()]; keep {
				continue
			}
			if err := c.Delete(ctx, object); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("delete stale provider RBAC %q: %w", object.GetName(), err)
			}
		}
	}
	return nil
}
