package app

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	"github.com/openmcp-project/controller-utils/pkg/collections"
	crdutil "github.com/openmcp-project/controller-utils/pkg/crds"
	"github.com/openmcp-project/controller-utils/pkg/logging"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	"github.com/openmcp-project/openmcp-operator/api/crds"
	"github.com/openmcp-project/openmcp-operator/api/install"
	providerv1alpha1 "github.com/openmcp-project/openmcp-operator/api/provider/v1alpha1"
	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/options"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/helm"
)

const (
	OpenMCPOperatorName = "openmcp-operator"
	initCommand         = "init"
	runCommand          = "run"
)

func NewInitCommand(po *options.PersistentOptions) *cobra.Command {
	opts := &InitOptions{
		PersistentOptions: po,
	}
	cmd := &cobra.Command{
		Use:   initCommand,
		Short: "Initialize the open control plane operator",
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

type InitOptions struct {
	*options.PersistentOptions
	RawInitOptions
}

type RawInitOptions struct {
	SkipMCPPlatformService  bool `json:"skip-mcp-platform-service"`
	SkipHelmPlatformService bool `json:"skip-helm-platform-service"`
}

func (o *InitOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&o.SkipMCPPlatformService, "skip-mcp-platform-service", false, "If true, the PlatformService for the ManagedControlPlane controller will not be created/updated.")
	cmd.Flags().BoolVar(&o.SkipHelmPlatformService, "skip-helm-platform-service", false, "If true, the PlatformService for the helm deployer will not be created/updated.")
}

func (o *InitOptions) Complete(ctx context.Context) error {
	if err := o.PersistentOptions.Complete(ctx); err != nil {
		return err
	}
	return nil
}

//nolint:gocyclo
func (o *InitOptions) Run(ctx context.Context) error {
	if err := o.PlatformCluster.InitializeClient(install.InstallOperatorAPIsPlatform(install.InstallCRDAPIs(runtime.NewScheme()))); err != nil {
		return err
	}

	log := o.Log.WithName("main")
	log.Info("Environment", "value", o.Environment)

	// apply CRDs
	crdManager := crdutil.NewCRDManager(apiconst.ClusterLabel, crds.CRDs)
	crdManager.AddCRDLabelToClusterMapping(clustersv1alpha1.PURPOSE_PLATFORM, o.PlatformCluster)
	crdManager.SkipCRDsWithClusterLabel(clustersv1alpha1.PURPOSE_ONBOARDING)

	if err := crdManager.CreateOrUpdateCRDs(ctx, &log); err != nil {
		return fmt.Errorf("error creating/updating CRDs: %w", err)
	}

	// prepare platformservice deployment
	podName := os.Getenv(apiconst.EnvVariablePodName)
	if podName == "" {
		return fmt.Errorf("environment variable %s is not set", apiconst.EnvVariablePodName)
	}
	podNamespace := os.Getenv(apiconst.EnvVariablePodNamespace)
	if podNamespace == "" {
		return fmt.Errorf("environment variable %s is not set", apiconst.EnvVariablePodNamespace)
	}

	log.Info("Fetching own pod to determine image", "name", podName, "namespace", podNamespace)
	pod := &corev1.Pod{}
	pod.Name = podName
	pod.Namespace = podNamespace
	if err := o.PlatformCluster.Client().Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		return fmt.Errorf("error fetching own pod %s/%s: %w", podNamespace, podName, err)
	}
	var container *corev1.Container
	if len(pod.Spec.Containers) == 1 {
		container = &pod.Spec.Containers[0]
	} else {
		for _, c := range pod.Spec.Containers {
			if c.Name == OpenMCPOperatorName {
				container = &c
				break
			}
		}
	}
	if container == nil {
		return fmt.Errorf("unable to determine main container from pod %s/%s", podNamespace, podName)
	}
	verbosity := "INFO"
	if log.Enabled(logging.DEBUG) {
		verbosity = "DEBUG"
	}
	pullSecrets := pod.Spec.ImagePullSecrets
	psToDelete := []providerv1alpha1.PlatformService{}

	// create PlatformService for MCP controller (unless disabled)
	if o.SkipMCPPlatformService {
		log.Info("Skipping creation/update of PlatformService for ManagedControlPlane controller")
	} else {
		log.Info("Creating/updating PlatformService for ManagedControlPlane controller")

		// identify volumes that need to be mounted in order to have the config available
		configPaths := []string{}
		volumeMounts := []corev1.VolumeMount{}
		volumes := []corev1.Volume{}
		next := false
		for _, arg := range container.Args {
			if next {
				configPaths = append(configPaths, arg)
				next = false
			} else if arg == "--config" {
				next = true
			} else if suffix, ok := strings.CutPrefix(arg, "--config="); ok {
				configPaths = append(configPaths, suffix)
			}
		}
		if len(configPaths) > 0 {
			for _, vm := range container.VolumeMounts {
				for _, cp := range configPaths {
					if strings.HasPrefix(cp, vm.MountPath) {
						volumeMounts = append(volumeMounts, vm)
						break
					}
				}
			}
			for _, v := range pod.Spec.Volumes {
				for _, vm := range volumeMounts {
					if v.Name == vm.Name {
						volumes = append(volumes, v)
						break
					}
				}
			}
		}

		mcpPSName := o.ProviderName
		if mcpPSName == "" {
			mcpPSName = strings.ToLower(controlplane.ControllerName)
		}

		expectedLabels := map[string]string{
			apiconst.ManagedByLabel: OpenMCPOperatorName,
			"platformservice." + apiconst.OpenMCPGroupName + "/purpose": controlplane.ControllerName,
		}
		psl := &providerv1alpha1.PlatformServiceList{}
		if err := o.PlatformCluster.Client().List(ctx, psl, client.MatchingLabels(expectedLabels)); err != nil {
			return fmt.Errorf("error listing PlatformServices: %w", err)
		}
		var ps *providerv1alpha1.PlatformService
		for _, item := range psl.Items {
			if item.DeletionTimestamp.IsZero() { // ignore platform services in deletion
				if item.Name == mcpPSName {
					ps = &item
				} else {
					psToDelete = append(psToDelete, item)
				}
			}
		}
		if ps == nil {
			log.Info("Creating PlatformService for ManagedControlPlane controller", "name", mcpPSName)
			ps = &providerv1alpha1.PlatformService{}
			ps.Name = mcpPSName
		} else {
			log.Info("Updating PlatformService for ManagedControlPlane controller", "name", mcpPSName)
		}
		if _, err := controllerutil.CreateOrUpdate(ctx, o.PlatformCluster.Client(), ps, func() error {
			ps.Labels = expectedLabels
			ps.Spec.Image = container.Image
			ps.Spec.ImagePullSecrets = collections.ProjectSliceToSlice(pullSecrets, func(ref corev1.LocalObjectReference) commonapi.LocalObjectReference {
				return commonapi.LocalObjectReference{
					Name: ref.Name,
				}
			})
			ps.Spec.ExtraVolumes = volumes
			ps.Spec.ExtraVolumeMounts = volumeMounts
			ps.Spec.InitCommand = []string{"mcp", initCommand}
			ps.Spec.RunCommand = []string{"mcp", runCommand}
			for _, cp := range configPaths {
				ps.Spec.InitCommand = append(ps.Spec.InitCommand, "--config="+cp)
				ps.Spec.RunCommand = append(ps.Spec.RunCommand, "--config="+cp)
			}
			ps.Spec.Verbosity = verbosity
			ps.Spec.RunReplicas = *o.Config.ManagedControlPlane.PlatformService.Replicas
			ps.Spec.TopologySpreadConstraints = o.Config.ManagedControlPlane.PlatformService.TopologySpreadConstraints
			return nil
		}); err != nil {
			return fmt.Errorf("error creating/updating PlatformService %s: %w", ps.Name, err)
		}
	}

	if o.SkipHelmPlatformService {
		log.Info("Skipping creation/update of HelmDeployer PlatformService")
	} else {
		log.Info("Creating/updating HelmDeployer PlatformService")
		// create/update PlatformService helm
		psName := strings.ToLower(helm.ControllerName)

		expectedLabels := map[string]string{
			apiconst.ManagedByLabel: OpenMCPOperatorName,
			"platformservice." + apiconst.OpenMCPGroupName + "/purpose": helm.ControllerName,
		}
		psl := &providerv1alpha1.PlatformServiceList{}
		if err := o.PlatformCluster.Client().List(ctx, psl, client.MatchingLabels(expectedLabels)); err != nil {
			return fmt.Errorf("error listing PlatformServices: %w", err)
		}
		var ps *providerv1alpha1.PlatformService
		for _, item := range psl.Items {
			if item.DeletionTimestamp.IsZero() { // ignore platform services in deletion
				if item.Name == psName {
					ps = &item
				} else {
					psToDelete = append(psToDelete, item)
				}
			}
		}
		if ps == nil {
			log.Info("Creating PlatformService for helm deployer", "name", psName)
			ps = &providerv1alpha1.PlatformService{}
			ps.Name = psName
		} else {
			log.Info("Updating PlatformService for helm deployer", "name", psName)
		}
		if _, err := controllerutil.CreateOrUpdate(ctx, o.PlatformCluster.Client(), ps, func() error {
			ps.Labels = expectedLabels
			ps.Spec.Image = container.Image
			ps.Spec.ImagePullSecrets = collections.ProjectSliceToSlice(pullSecrets, func(ref corev1.LocalObjectReference) commonapi.LocalObjectReference {
				return commonapi.LocalObjectReference{
					Name: ref.Name,
				}
			})
			ps.Spec.InitCommand = []string{"helm", initCommand}
			ps.Spec.RunCommand = []string{"helm", runCommand}
			ps.Spec.Verbosity = verbosity
			ps.Spec.RunReplicas = *o.Config.HelmDeployer.PlatformService.Replicas
			ps.Spec.TopologySpreadConstraints = o.Config.HelmDeployer.PlatformService.TopologySpreadConstraints
			return nil
		}); err != nil {
			return fmt.Errorf("error creating/updating PlatformService %s: %w", ps.Name, err)
		}
	}

	if len(psToDelete) > 0 {
		log.Info("Deleting obsolete PlatformServices", "count", len(psToDelete))
		for _, psDel := range psToDelete {
			if err := o.PlatformCluster.Client().Delete(ctx, &psDel); err != nil {
				return fmt.Errorf("error deleting obsolete PlatformService %s: %w", psDel.Name, err)
			}
			log.Info("Deleted obsolete PlatformService", "name", psDel.Name)
		}
	}

	log.Info("Finished init command")
	return nil
}

func (o *InitOptions) PrintRaw(cmd *cobra.Command) {
	data, err := yaml.Marshal(o.RawInitOptions)
	if err != nil {
		cmd.Println(fmt.Errorf("error marshalling raw options: %w", err).Error())
		return
	}
	cmd.Print(string(data))
}

func (o *InitOptions) PrintRawOptions(cmd *cobra.Command) {
	cmd.Println("########## RAW OPTIONS START ##########")
	o.PersistentOptions.PrintRaw(cmd)
	o.PrintRaw(cmd)
	cmd.Println("########## RAW OPTIONS END ##########")
}

func (o *InitOptions) PrintCompleted(cmd *cobra.Command) {}

func (o *InitOptions) PrintCompletedOptions(cmd *cobra.Command) {
	cmd.Println("########## COMPLETED OPTIONS START ##########")
	o.PersistentOptions.PrintCompleted(cmd)
	o.PrintCompleted(cmd)
	cmd.Println("########## COMPLETED OPTIONS END ##########")
}
