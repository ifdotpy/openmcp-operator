package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/openmcp-project/controller-utils/pkg/logging"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/yaml"

	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"

	"github.com/openmcp-project/openmcp-operator/api/install"
	"github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/options"
	"github.com/openmcp-project/openmcp-operator/internal/config"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/accessrequest"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/provider"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/scheduler"
)

var setupLog logging.Logger
var allControllers = []string{
	strings.ToLower(scheduler.ControllerName),
	strings.ToLower(provider.ControllerName),
	strings.ToLower(accessrequest.ControllerName),
}

func NewRunCommand(po *options.PersistentOptions) *cobra.Command {
	opts := &RunOptions{
		PersistentOptions: po,
	}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the open control plane operator",
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

	cmd.Flags().StringSliceVar(&o.Controllers, "controllers", allControllers, "List of active controllers. Separate with comma or specify flag multiple times to activate multiple controllers.")
}

type RawRunOptions struct {
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

	// custom raw options
	Controllers []string `json:"controllers"`
}

type RunOptions struct {
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

	o.ProviderGVKList = []schema.GroupVersionKind{
		v1alpha1.ClusterProviderGKV(),
		v1alpha1.PlatformServiceGKV(),
		v1alpha1.ServiceProviderGKV(),
	}

	return nil
}

func (o *RunOptions) PrintCompleted(cmd *cobra.Command) {
	rawData := map[string]any{}
	providerGVKs := make([]string, 0, len(o.ProviderGVKList))
	for _, gvk := range o.ProviderGVKList {
		providerGVKs = append(providerGVKs, gvk.String())
	}
	rawData["providerGVKs"] = providerGVKs
	data, err := yaml.Marshal(rawData)
	if err != nil {
		cmd.Println(fmt.Errorf("error marshalling completed options: %w", err).Error())
		return
	}
	cmd.Print(string(data))
}

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
	ctx = logging.NewContext(ctx, setupLog)

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: o.WebhookTLSOpts,
	})

	podNamespace := os.Getenv(apiconst.EnvVariablePodNamespace)
	if podNamespace == "" {
		return fmt.Errorf("environment variable %s is not set", apiconst.EnvVariablePodNamespace)
	}

	mgr, err := ctrl.NewManager(o.PlatformCluster.RESTConfig(), ctrl.Options{
		Scheme:                  install.InstallOperatorAPIsPlatform(runtime.NewScheme()),
		Metrics:                 o.MetricsServerOptions,
		WebhookServer:           webhookServer,
		HealthProbeBindAddress:  o.ProbeAddr,
		PprofBindAddress:        o.PprofAddr,
		LeaderElection:          o.EnableLeaderElection,
		LeaderElectionID:        "github.com/openmcp-project/openmcp-operator",
		LeaderElectionNamespace: podNamespace,
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

	// setup cluster scheduler
	if slices.Contains(o.Controllers, strings.ToLower(scheduler.ControllerName)) {
		// setup scheduler config fallback getter
		schedulerConfigGetter := func(ctx context.Context) (*config.SchedulerConfig, error) {
			return o.Config.Scheduler, nil
		}
		// dynamic config getter
		if o.ConfigMapName != "" {
			schedulerConfigGetter = func(ctx context.Context) (*config.SchedulerConfig, error) {
				providerConfig, err := config.LoadFromConfigMap(ctx, o.PlatformCluster.Client(), o.ConfigMapName, podNamespace)
				if err != nil {
					return nil, fmt.Errorf("failed to load config from ConfigMap: %w", err)
				}
				if providerConfig == nil {
					schedulerConfig := &config.SchedulerConfig{}
					if err := schedulerConfig.Default(nil); err != nil {
						return nil, fmt.Errorf("failed to default scheduler config: %w", err)
					}
					return schedulerConfig, nil
				}
				return providerConfig.Scheduler, nil
			}
		}
		sc, err := scheduler.NewClusterScheduler(ctx, &setupLog, o.PlatformCluster, schedulerConfigGetter, o.ConfigMapName)
		if err != nil {
			return fmt.Errorf("unable to initialize cluster scheduler: %w", err)
		}
		if err := sc.SetupWithManager(ctx, mgr); err != nil {
			return fmt.Errorf("unable to setup cluster scheduler with manager: %w", err)
		}
	}

	// setup accessrequest controller
	if slices.Contains(o.Controllers, strings.ToLower(accessrequest.ControllerName)) {
		// fallback config getter
		arConfigGetter := func(ctx context.Context) (*config.AccessRequestConfig, error) {
			if o.Config.AccessRequest == nil {
				o.Config.AccessRequest = &config.AccessRequestConfig{}
			}
			return o.Config.AccessRequest, nil
		}
		// dynamic config getter
		if o.ConfigMapName != "" {
			arConfigGetter = func(ctx context.Context) (*config.AccessRequestConfig, error) {
				providerConfig, err := config.LoadFromConfigMap(ctx, o.PlatformCluster.Client(), o.ConfigMapName, podNamespace)
				if err != nil {
					return nil, fmt.Errorf("failed to load config from ConfigMap: %w", err)
				}
				if providerConfig == nil || providerConfig.AccessRequest == nil {
					return &config.AccessRequestConfig{}, nil
				}
				return providerConfig.AccessRequest, nil
			}
		}
		arReconciler, err := accessrequest.NewAccessRequestReconciler(o.PlatformCluster, arConfigGetter, o.ConfigMapName)
		if err != nil {
			return fmt.Errorf("unable to create accessrequest reconciler: %w", err)
		}
		if err := arReconciler.SetupWithManager(ctx, mgr); err != nil {
			return fmt.Errorf("unable to setup accessrequest controller: %w", err)
		}
	}

	// setup deployment controller
	if slices.Contains(o.Controllers, strings.ToLower(provider.ControllerName)) {
		utilruntime.Must(clientgoscheme.AddToScheme(mgr.GetScheme()))
		utilruntime.Must(api.AddToScheme(mgr.GetScheme()))
		if err = provider.NewDeploymentController().SetupWithManager(&setupLog, mgr, o.ProviderGVKList, o.Environment); err != nil {
			return fmt.Errorf("unable to setup deployment controllers: %w", err)
		}
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
