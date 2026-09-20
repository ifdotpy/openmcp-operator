package mcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/yaml"

	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	"github.com/openmcp-project/openmcp-operator/api/install"
	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/options"
	"github.com/openmcp-project/openmcp-operator/internal/config"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
	"github.com/openmcp-project/openmcp-operator/lib/clusteraccess"
)

var setupLog logging.Logger

func NewRunCommand(po *options.PersistentOptions) *cobra.Command {
	opts := &RunOptions{
		PersistentOptions: po,
	}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the openMCP Operator",
		Run: func(cmd *cobra.Command, args []string) {
			opts.PrintRawOptions(cmd)
			if err := opts.Complete(cmd.Context()); err != nil {
				panic(fmt.Errorf("error completing options: %w", err))
			}
			opts.PrintCompletedOptions(cmd)
			if opts.DryRun {
				cmd.Println("=== END OF DRY RUN ===")
				return
			}
			if err := opts.Run(cmd.Context()); err != nil {
				panic(err)
			}
		},
	}
	opts.AddFlags(cmd)

	return cmd
}

func (o *RunOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.KCPEndpointSlice, "kcp-endpoint-slice", "", "KCP workspace mode: name of the APIExportEndpointSlice serving the tenant workspaces. Empty disables the mode.")
	cmd.Flags().StringVar(&o.KCPKubeconfig, "kcp-kubeconfig", "", "KCP workspace mode: kubeconfig path for the workspace holding the APIExportEndpointSlice.")
	cmd.Flags().DurationVar(&o.KCPWorkspaceReconcileInterval, "kcp-workspace-reconcile-interval", 5*time.Second, "KCP workspace mode: interval for workspace runtime reconciliation.")
	cmd.Flags().DurationVar(&o.KCPWorkspaceCleanupDelay, "kcp-workspace-cleanup-delay", time.Minute, "KCP workspace mode: delay before workspace runtime removal after disengagement.")
	cmd.Flags().DurationVar(&o.KCPWorkspaceTokenLifetime, "kcp-workspace-token-lifetime", time.Hour, "KCP workspace mode: lifetime of account workspace credentials.")
	cmd.Flags().StringVar(&o.KCPServiceProviders, "kcp-service-providers", "", "KCP workspace mode: JSON configuration of per-workspace service providers.")
	cmd.Flags().StringVar(&o.KCPBindingName, "kcp-binding-name", "", "KCP workspace mode: preferred APIBinding name. The operator otherwise discovers the binding from the endpoint slice export.")
	// kubebuilder default flags
	cmd.Flags().StringVar(&o.MetricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	cmd.Flags().StringVar(&o.ProbeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	cmd.Flags().StringVar(&o.PprofAddr, "pprof-bind-address", "", "The address the pprof endpoint binds to. Expected format is ':<port>'. Leave empty to disable pprof endpoint.")
	cmd.Flags().BoolVar(&o.EnableLeaderElection, "leader-elect", false, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	cmd.Flags().BoolVar(&o.SecureMetrics, "metrics-secure", true, "If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	cmd.Flags().StringVar(&o.WebhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	cmd.Flags().StringVar(&o.WebhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	cmd.Flags().StringVar(&o.WebhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	cmd.Flags().StringVar(&o.MetricsCertPath, "metrics-cert-path", "", "The directory that contains the metrics server certificate.")
	cmd.Flags().StringVar(&o.MetricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	cmd.Flags().StringVar(&o.MetricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	cmd.Flags().BoolVar(&o.EnableHTTP2, "enable-http2", false, "If set, HTTP/2 will be enabled for the metrics and webhook servers")
}

type RawRunOptions struct {
	KCPWorkspaceReconcileInterval time.Duration `json:"kcp-workspace-reconcile-interval"`
	KCPWorkspaceCleanupDelay      time.Duration `json:"kcp-workspace-cleanup-delay"`
	KCPWorkspaceTokenLifetime     time.Duration `json:"kcp-workspace-token-lifetime"`
	KCPBindingName                string        `json:"kcp-binding-name"`
	// kubebuilder default flags
	MetricsAddr          string `json:"metrics-bind-address"`
	MetricsCertPath      string `json:"metrics-cert-path"`
	MetricsCertName      string `json:"metrics-cert-name"`
	MetricsCertKey       string `json:"metrics-cert-key"`
	WebhookCertPath      string `json:"webhook-cert-path"`
	WebhookCertName      string `json:"webhook-cert-name"`
	WebhookCertKey       string `json:"webhook-cert-key"`
	EnableLeaderElection bool   `json:"leader-elect"`
	ProbeAddr            string `json:"health-probe-bind-address"`
	PprofAddr            string `json:"pprof-bind-address"`
	SecureMetrics        bool   `json:"metrics-secure"`
	EnableHTTP2          bool   `json:"enable-http2"`
}

type RunOptions struct {
	KCPServiceProviders   string              `json:"kcp-service-providers"`
	KCPWorkspaceProviders []workspaceProvider `json:"-"`
	// KCPEndpointSlice enables KCP workspace mode: the
	// name of the APIExportEndpointSlice whose virtual workspace serves the
	// bound workspaces. Empty = classic single-onboarding-cluster mode.
	KCPEndpointSlice string
	// KCPKubeconfig is the kubeconfig path for the kcp workspace holding the
	// APIExportEndpointSlice (KCP workspace mode only).
	KCPKubeconfig string

	*options.PersistentOptions
	RawRunOptions

	// fields filled in Complete()
	ProviderGVKList      []schema.GroupVersionKind
	TLSOpts              []func(*tls.Config)
	WebhookTLSOpts       []func(*tls.Config)
	MetricsServerOptions metricsserver.Options
	MetricsCertWatcher   *certwatcher.CertWatcher
	WebhookCertWatcher   *certwatcher.CertWatcher
}

func (o *RunOptions) PrintRaw(cmd *cobra.Command) {
	data, err := yaml.Marshal(o.RawRunOptions)
	if err != nil {
		cmd.Println(fmt.Errorf("error marshalling raw options: %w", err).Error())
		return
	}
	cmd.Print(string(data))
}

func (o *RunOptions) PrintRawOptions(cmd *cobra.Command) {
	cmd.Println("########## RAW OPTIONS START ##########")
	o.PersistentOptions.PrintRaw(cmd)
	o.PrintRaw(cmd)
	cmd.Println("########## RAW OPTIONS END ##########")
}

func (o *RunOptions) Complete(ctx context.Context) error {
	if err := o.PersistentOptions.Complete(ctx); err != nil {
		return err
	}
	if o.ProviderName == "" {
		return fmt.Errorf("provider-name must not be empty")
	}
	if o.KCPEndpointSlice != "" {
		if o.KCPKubeconfig == "" {
			return fmt.Errorf("kcp-kubeconfig must not be empty in KCP workspace mode")
		}
		providers, err := loadWorkspaceProviders(o.KCPServiceProviders)
		if err != nil {
			return err
		}
		o.KCPWorkspaceProviders = providers
		if o.KCPWorkspaceReconcileInterval <= 0 {
			return fmt.Errorf("kcp-workspace-reconcile-interval must be positive")
		}
		if o.KCPWorkspaceCleanupDelay < 0 {
			return fmt.Errorf("kcp-workspace-cleanup-delay must not be negative")
		}
		if o.KCPWorkspaceTokenLifetime < 10*time.Minute || o.KCPWorkspaceTokenLifetime > 24*time.Hour {
			return fmt.Errorf("kcp-workspace-token-lifetime must be between 10m and 24h")
		}
	}
	setupLog = o.Log.WithName("setup")
	ctrl.SetLogger(o.Log.Logr())

	// kubebuilder default stuff

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !o.EnableHTTP2 {
		o.TLSOpts = append(o.TLSOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	o.WebhookTLSOpts = o.TLSOpts

	if len(o.WebhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates", "webhook-cert-path", o.WebhookCertPath, "webhook-cert-name", o.WebhookCertName, "webhook-cert-key", o.WebhookCertKey)

		var err error
		o.WebhookCertWatcher, err = certwatcher.New(
			filepath.Join(o.WebhookCertPath, o.WebhookCertName),
			filepath.Join(o.WebhookCertPath, o.WebhookCertKey),
		)
		if err != nil {
			return fmt.Errorf("failed to initialize webhook certificate watcher: %w", err)
		}

		o.WebhookTLSOpts = append(o.WebhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = o.WebhookCertWatcher.GetCertificate
		})
	}

	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.2/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	o.MetricsServerOptions = metricsserver.Options{
		BindAddress:   o.MetricsAddr,
		SecureServing: o.SecureMetrics,
		TLSOpts:       o.TLSOpts,
	}

	if o.SecureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.2/pkg/metrics/filters#WithAuthenticationAndAuthorization
		o.MetricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(o.MetricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates", "metrics-cert-path", o.MetricsCertPath, "metrics-cert-name", o.MetricsCertName, "metrics-cert-key", o.MetricsCertKey)

		var err error
		o.MetricsCertWatcher, err = certwatcher.New(
			filepath.Join(o.MetricsCertPath, o.MetricsCertName),
			filepath.Join(o.MetricsCertPath, o.MetricsCertKey),
		)
		if err != nil {
			return fmt.Errorf("failed to initialize metrics certificate watcher: %w", err)
		}

		o.MetricsServerOptions.TLSOpts = append(o.MetricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = o.MetricsCertWatcher.GetCertificate
		})
	}

	return nil
}

func (o *RunOptions) PrintCompleted(cmd *cobra.Command) {}

func (o *RunOptions) PrintCompletedOptions(cmd *cobra.Command) {
	cmd.Println("########## COMPLETED OPTIONS START ##########")
	o.PersistentOptions.PrintCompleted(cmd)
	o.PrintCompleted(cmd)
	cmd.Println("########## COMPLETED OPTIONS END ##########")
}

func (o *RunOptions) Run(ctx context.Context) error {
	if err := o.PlatformCluster.InitializeClient(install.InstallOperatorAPIsPlatform(runtime.NewScheme())); err != nil {
		return err
	}

	setupLog = o.Log.WithName("setup")
	setupLog.Info("Environment", "value", o.Environment)
	setupLog.Info("Provider name", "value", o.ProviderName)
	ctx = logging.NewContext(ctx, setupLog)

	if o.KCPEndpointSlice != "" {
		return o.runMulticluster(ctx, setupLog)
	}

	// get access to the onboarding cluster
	setupLog.Info("Getting access to the onboarding cluster")
	onboardingScheme := runtime.NewScheme()
	install.InstallOperatorAPIsOnboarding(onboardingScheme)

	providerSystemNamespace := os.Getenv(apiconst.EnvVariablePodNamespace)
	if providerSystemNamespace == "" {
		return fmt.Errorf("environment variable %s is not set", apiconst.EnvVariablePodNamespace)
	}

	clusterAccessManager := clusteraccess.NewClusterAccessManager(o.PlatformCluster.Client(), controlplane.ControllerName, providerSystemNamespace)
	clusterAccessManager.WithLogger(&setupLog).
		WithInterval(10 * time.Second).
		WithTimeout(30 * time.Minute)

	onboardingCluster, err := clusterAccessManager.CreateAndWaitForCluster(ctx, clustersv1alpha1.PURPOSE_ONBOARDING, clustersv1alpha1.PURPOSE_ONBOARDING,
		onboardingScheme, []clustersv1alpha1.PermissionsRequest{
			{
				Rules: []rbacv1.PolicyRule{
					// It is hard to limit the permissions here, because the mcp controller needs to be able to fetch and delete all ServiceProvider-related resources.
					// These could be discovered through the ServiceProvider resources, but then we would need a mechanism to restart the MCP controller every time a new ServiceProvider is created.
					// For now, let's just go with full permissions to keep it simple.
					{
						APIGroups: []string{"*"},
						Resources: []string{"*"},
						Verbs:     []string{"*"},
					},
				},
			},
		})

	if err != nil {
		return fmt.Errorf("error creating/updating onboarding cluster: %w", err)
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: o.WebhookTLSOpts,
	})

	mgr, err := ctrl.NewManager(onboardingCluster.RESTConfig(), ctrl.Options{
		Scheme:                  onboardingCluster.Scheme(),
		Metrics:                 o.MetricsServerOptions,
		WebhookServer:           webhookServer,
		HealthProbeBindAddress:  o.ProbeAddr,
		PprofBindAddress:        o.PprofAddr,
		LeaderElection:          o.EnableLeaderElection,
		LeaderElectionID:        "github.com/openmcp-project/openmcp-operator--mcp-controller",
		LeaderElectionNamespace: providerSystemNamespace,
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("unable to create manager: %w", err)
	}

	// setup MCP controller
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

	mcpRec, err := controlplane.NewManagedControlPlaneReconciler(o.PlatformCluster, onboardingCluster, mgr.GetEventRecorder(controlplane.ControllerName), mcpConfigGetter, o.ConfigMapName)
	if err != nil {
		return fmt.Errorf("unable to create managedcontrolplane controller: %w", err)
	}
	if err := mcpRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to setup managedcontrolplane controller: %w", err)
	}

	if o.MetricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(o.MetricsCertWatcher); err != nil {
			return fmt.Errorf("unable to add metrics certificate watcher to manager: %w", err)
		}
	}

	if o.WebhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(o.WebhookCertWatcher); err != nil {
			return fmt.Errorf("unable to add webhook certificate watcher to manager: %w", err)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}

	return nil
}
