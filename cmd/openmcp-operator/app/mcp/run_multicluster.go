package mcp

import (
	"context"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"

	"github.com/openmcp-project/controller-utils/pkg/logging"

	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	"github.com/openmcp-project/openmcp-operator/api/install"
	"github.com/openmcp-project/openmcp-operator/internal/config"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
)

type workspaceOnboardingCluster struct {
	client client.Client
	scheme *runtime.Scheme
}

func (c workspaceOnboardingCluster) Client() client.Client   { return c.client }
func (c workspaceOnboardingCluster) Scheme() *runtime.Scheme { return c.scheme }

// runMulticluster runs the ControlPlane controller in KCP workspace mode.
// Instead of watching a single onboarding cluster, the
// controller consumes the APIExport virtual workspace named by
// --kcp-endpoint-slice and reconciles ControlPlane objects in place, in every
// KCP workspace that bound the export.
//
// The reconciler code is unchanged: per request, a shallow copy of the
// reconciler is bound to the tenant workspace (its client becomes the
// OnboardingCluster of that copy). The workspace runtime gives every workspace a
// unique internal ControlPlane namespace. Service-provider placement is outside
// this mode: providers only consume the workspace credential produced by the
// standard ControlPlane lifecycle.
func (o *RunOptions) runMulticluster(ctx context.Context, setupLog logging.Logger) error {
	setupLog.Info("KCP workspace mode", "endpointSlice", o.KCPEndpointSlice)

	scheme := runtime.NewScheme()
	install.InstallOperatorAPIsOnboarding(scheme)
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kcpapisv1alpha1.AddToScheme(scheme))
	utilruntime.Must(kcpcorev1alpha1.AddToScheme(scheme))

	cfg, err := clientcmd.BuildConfigFromFlags("", o.KCPKubeconfig)
	if err != nil {
		return fmt.Errorf("unable to load kcp kubeconfig: %w", err)
	}
	discoveryClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("unable to create kcp discovery client: %w", err)
	}
	endpointSlice := &kcpapisv1alpha1.APIExportEndpointSlice{}
	if err := discoveryClient.Get(ctx, client.ObjectKey{Name: o.KCPEndpointSlice}, endpointSlice); err != nil {
		return fmt.Errorf("unable to read APIExportEndpointSlice %q: %w", o.KCPEndpointSlice, err)
	}
	if endpointSlice.Spec.APIExport.Name == "" {
		return fmt.Errorf("APIExportEndpointSlice %q has no APIExport name", o.KCPEndpointSlice)
	}

	provider := newKCPWorkspaceProvider(
		cfg,
		o.KCPEndpointSlice,
		scheme,
		o.KCPWorkspaceReconcileInterval,
		setupLog,
		discoveryClient,
	)

	providerSystemNamespace := os.Getenv(apiconst.EnvVariablePodNamespace)
	if providerSystemNamespace == "" {
		return fmt.Errorf("environment variable %s is not set", apiconst.EnvVariablePodNamespace)
	}

	// The local manager runs against the operator's host cluster. The provider
	// supplies the KCP workspace clients used by multicluster reconcilers. This
	// also keeps leader election, health endpoints, and events on the same
	// cluster as the operator deployment.
	mcMgr, err := mcmanager.New(o.PlatformCluster.RESTConfig(), provider, manager.Options{
		Scheme:                        scheme,
		Metrics:                       o.MetricsServerOptions,
		HealthProbeBindAddress:        o.ProbeAddr,
		PprofBindAddress:              o.PprofAddr,
		LeaderElection:                o.EnableLeaderElection,
		LeaderElectionID:              "github.com/openmcp-project/openmcp-operator--mcp-controller",
		LeaderElectionNamespace:       providerSystemNamespace,
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("unable to create multicluster manager: %w", err)
	}

	// same config resolution as the classic mode
	mcpConfigGetter := func(ctx context.Context) (*config.ManagedControlPlaneConfig, error) {
		return o.Config.ManagedControlPlane, nil
	}
	if o.ConfigMapName != "" {
		mcpConfigGetter = func(ctx context.Context) (*config.ManagedControlPlaneConfig, error) {
			providerConfig, err := config.LoadFromConfigMap(ctx, o.PlatformCluster.Client(), o.ConfigMapName, providerSystemNamespace)
			if err != nil {
				return nil, fmt.Errorf("failed to load config from ConfigMap: %w", err)
			}
			if providerConfig == nil {
				mcpConfig := &config.ManagedControlPlaneConfig{}
				err = mcpConfig.Default(nil)
				return mcpConfig, err
			}
			return providerConfig.ManagedControlPlane, nil
		}
	}

	mcpRec, err := controlplane.NewManagedControlPlaneReconciler(
		o.PlatformCluster,
		nil, // onboarding cluster is resolved per request in this mode
		mcMgr.GetLocalManager().GetEventRecorder(controlplane.ControllerName),
		mcpConfigGetter,
		o.ConfigMapName,
	)
	if err != nil {
		return fmt.Errorf("unable to create ManagedControlPlane reconciler: %w", err)
	}

	err = mcbuilder.ControllerManagedBy(mcMgr).
		Named(controlplane.ControllerName).
		For(&corev2alpha1.ControlPlane{}).
		Complete(mcreconcile.Func(func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
			cl, err := mcMgr.GetCluster(ctx, req.ClusterName)
			if err != nil {
				return ctrl.Result{}, err
			}
			// Bind a shallow copy of the reconciler to the engaged workspace.
			rc := *mcpRec
			rc.OnboardingCluster = workspaceOnboardingCluster{client: cl.GetClient(), scheme: cl.GetScheme()}
			return rc.Reconcile(ctx, req.Request)
		}))
	if err != nil {
		return fmt.Errorf("unable to build multicluster controller: %w", err)
	}

	// One runtime is created dynamically for every workspace that binds the APIExport.
	// It registers the workspace itself as the MCP cluster and resolves only the
	// ClusterRequest and AccessRequest resources owned by the ControlPlane controller.
	runtime := &workspaceRuntime{
		log:                setupLog,
		platform:           o.PlatformCluster,
		bindingName:        o.KCPBindingName,
		bindingExport:      endpointSlice.Spec.APIExport,
		reconcileInterval:  o.KCPWorkspaceReconcileInterval,
		cleanupDelay:       o.KCPWorkspaceCleanupDelay,
		tokenLifetime:      o.KCPWorkspaceTokenLifetime,
		consumerBaseConfig: cfg,
	}
	if err := mcMgr.Add(runtime); err != nil {
		return fmt.Errorf("unable to add workspace runtime: %w", err)
	}

	if err := mcMgr.GetLocalManager().AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mcMgr.GetLocalManager().AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("Starting KCP workspace manager")
	if err := mcMgr.Start(ctx); err != nil {
		return fmt.Errorf("error running multicluster manager: %w", err)
	}
	return nil
}
