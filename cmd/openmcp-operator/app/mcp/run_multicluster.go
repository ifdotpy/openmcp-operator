package mcp

import (
	"context"
	"fmt"
	"net/http"
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

	"github.com/kcp-dev/multicluster-provider/apiexport"
	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	kcpcorev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	"github.com/openmcp-project/openmcp-operator/api/install"
	"github.com/openmcp-project/openmcp-operator/internal/config"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
	"github.com/openmcp-project/openmcp-operator/internal/disconnectguard"
)

// runMulticluster runs the ControlPlane controller in the multicluster (kcp)
// deployment mode: instead of watching a single onboarding cluster, the
// controller consumes the APIExport virtual workspace named by
// --kcp-endpoint-slice and reconciles ControlPlane objects in place, in every
// kcp workspace that bound the export.
//
// The reconciler code is unchanged: per request, a shallow copy of the
// reconciler is bound to the tenant workspace (its client becomes the
// OnboardingCluster of that copy). The workspace runtime gives every workspace a
// unique internal ControlPlane namespace. Standard upstream providers derive
// the same unique platform namespace without any kcp-specific code.
func (o *RunOptions) runMulticluster(ctx context.Context, setupLog logging.Logger) error {
	setupLog.Info("Multicluster (kcp) mode", "endpointSlice", o.KCPEndpointSlice)

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

	logr := setupLog.Logr()
	provider, err := apiexport.New(cfg, o.KCPEndpointSlice, apiexport.Options{
		Scheme: scheme,
		Log:    &logr,
	})
	if err != nil {
		return fmt.Errorf("unable to construct apiexport provider: %w", err)
	}

	mcMgr, err := mcmanager.New(cfg, provider, manager.Options{
		Scheme:                 scheme,
		Metrics:                o.MetricsServerOptions,
		HealthProbeBindAddress: o.ProbeAddr,
		PprofBindAddress:       o.PprofAddr,
		// Leader election intentionally off in the first multicluster
		// increment; run a single replica.
	})
	if err != nil {
		return fmt.Errorf("unable to create multicluster manager: %w", err)
	}

	// same config resolution as the classic mode
	providerSystemNamespace := os.Getenv(apiconst.EnvVariablePodNamespace)
	if providerSystemNamespace == "" {
		return fmt.Errorf("environment variable %s is not set", apiconst.EnvVariablePodNamespace)
	}
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
			// bind a shallow copy of the reconciler to the tenant workspace
			rc := *mcpRec
			rc.OnboardingCluster = clusters.NewTestClusterFromClient(string(req.ClusterName), cl.GetClient())
			return rc.Reconcile(ctx, req.Request)
		}))
	if err != nil {
		return fmt.Errorf("unable to build multicluster controller: %w", err)
	}

	// One runtime is created dynamically for every workspace that binds the APIExport.
	// It registers the workspace itself as the onboarding and MCP cluster. Configured
	// service providers use their normal ClusterRequest/AccessRequest handshake and
	// receive credentials that are valid only in this workspace.
	runtime := &workspaceRuntime{
		log:               setupLog,
		platform:          o.PlatformCluster,
		environment:       o.Environment,
		bindingName:       o.KCPBindingName,
		bindingExport:     endpointSlice.Spec.APIExport,
		reconcileInterval: o.KCPWorkspaceReconcileInterval,
		cleanupDelay:      o.KCPWorkspaceCleanupDelay,
		tokenLifetime:     o.KCPWorkspaceTokenLifetime,
		providers:         o.KCPWorkspaceProviders,
		credentialIssuer:  o.KCPWorkspaceCredentialIssuer,
		kcpConfig:         cfg,
	}
	if o.KCPDisconnectGuardAddress != "" {
		var ca []byte
		if o.KCPDisconnectGuardCA != "" {
			ca, err = os.ReadFile(o.KCPDisconnectGuardCA)
			if err != nil {
				return fmt.Errorf("read disconnect guard CA: %w", err)
			}
		}
		runtime.disconnectGuard = &workspaceDisconnectGuard{url: o.KCPDisconnectGuardURL, caBundle: ca}
		mux := http.NewServeMux()
		mux.Handle("/disconnect", disconnectguard.Handler{Inspect: runtime.disconnectInspector().Check})
		if err := mcMgr.GetLocalManager().Add(manager.RunnableFunc(func(ctx context.Context) error {
			return disconnectguard.Serve(ctx, o.KCPDisconnectGuardAddress, o.KCPDisconnectGuardCert, o.KCPDisconnectGuardKey, mux)
		})); err != nil {
			return fmt.Errorf("unable to add disconnect guard: %w", err)
		}
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

	setupLog.Info("Starting multicluster manager")
	if err := mcMgr.Start(ctx); err != nil {
		return fmt.Errorf("error running multicluster manager: %w", err)
	}
	return nil
}
