package mcp

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	crdutil "github.com/openmcp-project/controller-utils/pkg/crds"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	"github.com/openmcp-project/controller-utils/pkg/resources"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev2alpha1 "github.com/openmcp-project/openmcp-operator/api/core/v2alpha1"
	"github.com/openmcp-project/openmcp-operator/api/crds"
	"github.com/openmcp-project/openmcp-operator/api/install"
	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/options"
	"github.com/openmcp-project/openmcp-operator/internal/config"
	"github.com/openmcp-project/openmcp-operator/internal/controllers/controlplane"
	"github.com/openmcp-project/openmcp-operator/lib/clusteraccess"
)

// currently hard-coded, can be made configurable in the future if needed
const (
	MCPPurposeOverrideValidationPolicyName         = "mcp-purpose-override-validation." + corev2alpha1.OldGroupName
	OIDCProviderNameUniquenessValidationPolicyName = "oidc-provider-name-uniqueness." + corev2alpha1.OldGroupName
)

func NewInitCommand(po *options.PersistentOptions) *cobra.Command {
	opts := &InitOptions{
		PersistentOptions: po,
	}
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize the MCP controller",
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
	// KCPMode skips the onboarding-cluster initialization entirely: in the
	// multicluster (kcp) deployment mode there is no onboarding cluster, and
	// the tenant-facing API surface is provisioned in kcp instead.
	KCPMode bool
}

func (o *InitOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&o.KCPMode, "kcp-mode", false, "Skip onboarding cluster initialization (multicluster/kcp deployment mode).")
}

func (o *InitOptions) Complete(ctx context.Context) error {
	if err := o.PersistentOptions.Complete(ctx); err != nil {
		return err
	}
	if o.ProviderName == "" {
		return fmt.Errorf("provider-name must not be empty")
	}
	return nil
}

func (o *InitOptions) Run(ctx context.Context) error {
	if o.KCPMode {
		o.Log.Info("kcp mode: skipping onboarding cluster initialization")
		return nil
	}
	if err := o.PlatformCluster.InitializeClient(install.InstallOperatorAPIsPlatform(runtime.NewScheme())); err != nil {
		return err
	}

	log := o.Log.WithName("main")

	log.Info("Getting access to the onboarding cluster")
	onboardingScheme := runtime.NewScheme()
	install.InstallCRDAPIs(onboardingScheme)

	providerSystemNamespace := os.Getenv(apiconst.EnvVariablePodNamespace)
	if providerSystemNamespace == "" {
		return fmt.Errorf("environment variable %s is not set", apiconst.EnvVariablePodNamespace)
	}

	clusterAccessManager := clusteraccess.NewClusterAccessManager(o.PlatformCluster.Client(), controlplane.ControllerName, providerSystemNamespace)
	clusterAccessManager.WithLogger(&log).
		WithInterval(10 * time.Second).
		WithTimeout(30 * time.Minute)

	onboardingCluster, err := clusterAccessManager.CreateAndWaitForCluster(ctx, clustersv1alpha1.PURPOSE_ONBOARDING+"-init", clustersv1alpha1.PURPOSE_ONBOARDING,
		onboardingScheme, []clustersv1alpha1.PermissionsRequest{
			{
				Rules: []rbacv1.PolicyRule{
					{
						APIGroups: []string{"apiextensions.k8s.io"},
						Resources: []string{"customresourcedefinitions"},
						Verbs:     []string{"*"},
					},
					{
						APIGroups: []string{"admissionregistration.k8s.io"},
						Resources: []string{"validatingadmissionpolicies", "validatingadmissionpolicybindings"},
						Verbs:     []string{"*"},
					},
				},
			},
		})

	if err != nil {
		return fmt.Errorf("error creating/updating onboarding cluster: %w", err)
	}

	crdManager := crdutil.NewCRDManager(apiconst.ClusterLabel, crds.CRDs)

	// deploy only onboarding CRDs here, because the openMCP operator already deployed the platform CRDs
	crdManager.AddCRDLabelToClusterMapping(clustersv1alpha1.PURPOSE_ONBOARDING, onboardingCluster)
	crdManager.SkipCRDsWithClusterLabel(clustersv1alpha1.PURPOSE_PLATFORM)

	if err := crdManager.CreateOrUpdateCRDs(ctx, &o.Log); err != nil {
		return fmt.Errorf("error creating/updating CRDs: %w", err)
	}

	// ensure ValidatingAdmissionPolicy to prevent removal or changes to the MCP purpose override label
	if err := o.ensureMCPPurposeOverrideImmutabilityValidationPolicy(ctx, log, onboardingCluster); err != nil {
		return fmt.Errorf("error ensuring MCP purpose override immutability ValidatingAdmissionPolicy: %w", err)
	}

	// ensure ValidatingAdmissionPolicy to prevent MCP resources to specify OIDC providers with the same name as the default OIDC provider
	var mcpConfig *config.ManagedControlPlaneConfig
	if o.Config != nil {
		mcpConfig = o.Config.ManagedControlPlane
	}
	if err := o.ensureDefaultOIDCProviderNameUniquenessValidationPolicy(ctx, log, onboardingCluster, mcpConfig); err != nil {
		return fmt.Errorf("error ensuring OIDC provider name uniqueness ValidatingAdmissionPolicy: %w", err)
	}

	log.Info("Finished init command")
	return nil
}

func (o *InitOptions) PrintRaw(cmd *cobra.Command) {}

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

func (o *InitOptions) ensureMCPPurposeOverrideImmutabilityValidationPolicy(ctx context.Context, log logging.Logger, onboardingCluster *clusters.Cluster) error {
	labelSelector := client.MatchingLabels{
		apiconst.ManagedByLabel:      controlplane.ControllerName,
		apiconst.ManagedPurposeLabel: corev2alpha1.ManagedPurposeMCPPurposeOverride,
	}
	evapbs := &admissionv1.ValidatingAdmissionPolicyBindingList{}
	if err := onboardingCluster.Client().List(ctx, evapbs, labelSelector); err != nil {
		return fmt.Errorf("error listing ValidatingAdmissionPolicyBindings: %w", err)
	}
	for _, evapb := range evapbs.Items {
		if evapb.Name != MCPPurposeOverrideValidationPolicyName {
			log.Info("Deleting existing ValidatingAdmissionPolicyBinding for MCP purpose immutability", "name", evapb.Name)
			if err := onboardingCluster.Client().Delete(ctx, &evapb); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("error deleting ValidatingAdmissionPolicyBinding '%s': %w", evapb.Name, err)
			}
		}
	}
	evaps := &admissionv1.ValidatingAdmissionPolicyList{}
	if err := onboardingCluster.Client().List(ctx, evaps, labelSelector); err != nil {
		return fmt.Errorf("error listing ValidatingAdmissionPolicies: %w", err)
	}
	for _, evap := range evaps.Items {
		if evap.Name != MCPPurposeOverrideValidationPolicyName {
			log.Info("Deleting existing ValidatingAdmissionPolicy for MCP purpose immutability", "name", evap.Name)
			if err := onboardingCluster.Client().Delete(ctx, &evap); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("error deleting ValidatingAdmissionPolicy '%s': %w", evap.Name, err)
			}
		}
	}
	log.Info("Creating/updating ValidatingAdmissionPolicies to prevent undesired changes to the MCP purpose override label ...")
	vapm := resources.NewValidatingAdmissionPolicyMutator(MCPPurposeOverrideValidationPolicyName, admissionv1.ValidatingAdmissionPolicySpec{
		FailurePolicy: ptr.To(admissionv1.Fail),
		MatchConstraints: &admissionv1.MatchResources{
			ResourceRules: []admissionv1.NamedRuleWithOperations{
				{
					RuleWithOperations: admissionv1.RuleWithOperations{
						Operations: []admissionv1.OperationType{
							admissionv1.Create,
							admissionv1.Update,
						},
						Rule: admissionv1.Rule{ // match all resources, actual restriction happens in the binding
							APIGroups:   []string{"*"},
							APIVersions: []string{"*"},
							Resources:   []string{"*"},
						},
					},
				},
			},
		},
		Variables: []admissionv1.Variable{
			{
				Name:       "purposeOverrideLabel",
				Expression: fmt.Sprintf(`(has(object.metadata.labels) && "%s" in object.metadata.labels) ? object.metadata.labels["%s"] : ""`, corev2alpha1.MCPPurposeOverrideLabel, corev2alpha1.MCPPurposeOverrideLabel),
			},
			{
				Name:       "oldPurposeOverrideLabel",
				Expression: fmt.Sprintf(`(oldObject != null && has(oldObject.metadata.labels) && "%s" in oldObject.metadata.labels) ? oldObject.metadata.labels["%s"] : ""`, corev2alpha1.MCPPurposeOverrideLabel, corev2alpha1.MCPPurposeOverrideLabel),
			},
		},
		Validations: []admissionv1.Validation{
			{
				Expression: `request.operation == "CREATE" || (variables.oldPurposeOverrideLabel == variables.purposeOverrideLabel)`,
				Message:    fmt.Sprintf(`The label "%s" is immutable, it cannot be added after creation and is not allowed to be changed or removed once set.`, corev2alpha1.MCPPurposeOverrideLabel),
			},
			{
				Expression: `(variables.purposeOverrideLabel == "") || variables.purposeOverrideLabel.contains("mcp")`,
				Message:    fmt.Sprintf(`The value of the label "%s" must contain "mcp".`, corev2alpha1.MCPPurposeOverrideLabel),
			},
		},
	})
	vapm.MetadataMutator().WithLabels(map[string]string{
		apiconst.ManagedByLabel:      controlplane.ControllerName,
		apiconst.ManagedPurposeLabel: corev2alpha1.ManagedPurposeMCPPurposeOverride,
	})
	if err := resources.CreateOrUpdateResource(ctx, onboardingCluster.Client(), vapm); err != nil {
		return fmt.Errorf("error creating/updating ValidatingAdmissionPolicy for MCP purpose immutability validation: %w", err)
	}

	vapbm := resources.NewValidatingAdmissionPolicyBindingMutator(MCPPurposeOverrideValidationPolicyName, admissionv1.ValidatingAdmissionPolicyBindingSpec{
		PolicyName: MCPPurposeOverrideValidationPolicyName,
		ValidationActions: []admissionv1.ValidationAction{
			admissionv1.Deny,
		},
		MatchResources: &admissionv1.MatchResources{
			ResourceRules: []admissionv1.NamedRuleWithOperations{
				{
					RuleWithOperations: admissionv1.RuleWithOperations{
						Operations: []admissionv1.OperationType{
							admissionv1.Create,
							admissionv1.Update,
						},
						Rule: admissionv1.Rule{
							APIGroups:   []string{corev2alpha1.GroupVersion.Group},
							APIVersions: []string{corev2alpha1.GroupVersion.Version},
							Resources: []string{
								"ControlPlanes",
							},
						},
					},
				},
			},
		},
	})
	vapbm.MetadataMutator().WithLabels(map[string]string{
		apiconst.ManagedByLabel:      controlplane.ControllerName,
		apiconst.ManagedPurposeLabel: corev2alpha1.ManagedPurposeMCPPurposeOverride,
	})
	if err := resources.CreateOrUpdateResource(ctx, onboardingCluster.Client(), vapbm); err != nil {
		return fmt.Errorf("error creating/updating ValidatingAdmissionPolicyBinding for MCP purpose immutability validation: %w", err)
	}
	log.Info("ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding for MCP purpose immutability validation created/updated")
	return nil
}

func (o *InitOptions) ensureDefaultOIDCProviderNameUniquenessValidationPolicy(ctx context.Context, log logging.Logger, onboardingCluster *clusters.Cluster, mcpConfig *config.ManagedControlPlaneConfig) error {
	if mcpConfig == nil {
		mcpConfig = &config.ManagedControlPlaneConfig{}
		if err := mcpConfig.Default(nil); err != nil {
			return fmt.Errorf("error defaulting ManagedControlPlane controller config: %w", err)
		}
	}
	defaultOIDCProvider := mcpConfig.DefaultOIDCProvider
	defaultOIDCProviderName := ""
	if defaultOIDCProvider != nil {
		defaultOIDCProvider.Default()
		defaultOIDCProviderName = defaultOIDCProvider.Name
	}

	labelSelector := client.MatchingLabels{
		apiconst.ManagedByLabel:      controlplane.ControllerName,
		apiconst.ManagedPurposeLabel: corev2alpha1.ManagedPurposeOIDCProviderNameUniqueness,
	}
	evapbs := &admissionv1.ValidatingAdmissionPolicyBindingList{}
	if err := onboardingCluster.Client().List(ctx, evapbs, labelSelector); err != nil {
		return fmt.Errorf("error listing ValidatingAdmissionPolicyBindings: %w", err)
	}
	for _, evapb := range evapbs.Items {
		if evapb.Name != OIDCProviderNameUniquenessValidationPolicyName {
			log.Info("Deleting existing ValidatingAdmissionPolicyBinding for OIDC provider name uniqueness", "name", evapb.Name)
			if err := onboardingCluster.Client().Delete(ctx, &evapb); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("error deleting ValidatingAdmissionPolicyBinding '%s': %w", evapb.Name, err)
			}
		}
	}
	evaps := &admissionv1.ValidatingAdmissionPolicyList{}
	if err := onboardingCluster.Client().List(ctx, evaps, labelSelector); err != nil {
		return fmt.Errorf("error listing ValidatingAdmissionPolicies: %w", err)
	}
	for _, evap := range evaps.Items {
		if evap.Name != OIDCProviderNameUniquenessValidationPolicyName {
			log.Info("Deleting existing ValidatingAdmissionPolicy for OIDC provider name uniqueness", "name", evap.Name)
			if err := onboardingCluster.Client().Delete(ctx, &evap); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("error deleting ValidatingAdmissionPolicy '%s': %w", evap.Name, err)
			}
		}
	}
	log.Info("Creating/updating ValidatingAdmissionPolicies to ensure that no MCP specifies duplicate OIDC provider names or an OIDC provider with the same name as the standard OIDC provider ...")
	vapm := resources.NewValidatingAdmissionPolicyMutator(OIDCProviderNameUniquenessValidationPolicyName, admissionv1.ValidatingAdmissionPolicySpec{
		FailurePolicy: ptr.To(admissionv1.Fail),
		MatchConstraints: &admissionv1.MatchResources{
			ResourceRules: []admissionv1.NamedRuleWithOperations{
				{
					RuleWithOperations: admissionv1.RuleWithOperations{
						Operations: []admissionv1.OperationType{
							admissionv1.Create,
							admissionv1.Update,
						},
						Rule: admissionv1.Rule{
							APIGroups:   []string{corev2alpha1.GroupVersion.Group},
							APIVersions: []string{corev2alpha1.GroupVersion.Version},
							Resources: []string{
								"ControlPlanes",
							},
						},
					},
				},
			},
		},
		Variables: []admissionv1.Variable{
			{
				Name:       "defaultOIDCProviderName",
				Expression: fmt.Sprintf(`"%s"`, defaultOIDCProviderName),
			},
		},
		Validations: []admissionv1.Validation{
			{
				Expression: `!(has(object.spec.iam.oidcProviders) && (
	object.spec.iam.oidcProviders.exists(elem, elem.name == variables.defaultOIDCProviderName) ||
  object.spec.iam.oidcProviders.exists(elem, !object.spec.iam.oidcProviders.exists_one(elem2, elem2.name == elem.name))
))
`,
				Message: fmt.Sprintf(`There are no duplicate names allowed in spec.iam.oidcProviders and no OIDC provider may have the same name the default OIDC provider, which is "%s"`, defaultOIDCProviderName),
			},
		},
	})
	vapm.MetadataMutator().WithLabels(map[string]string{
		apiconst.ManagedByLabel:      controlplane.ControllerName,
		apiconst.ManagedPurposeLabel: corev2alpha1.ManagedPurposeOIDCProviderNameUniqueness,
	})
	if err := resources.CreateOrUpdateResource(ctx, onboardingCluster.Client(), vapm); err != nil {
		return fmt.Errorf("error creating/updating ValidatingAdmissionPolicy for OIDC provider name uniqueness validation: %w", err)
	}

	vapbm := resources.NewValidatingAdmissionPolicyBindingMutator(OIDCProviderNameUniquenessValidationPolicyName, admissionv1.ValidatingAdmissionPolicyBindingSpec{
		PolicyName: OIDCProviderNameUniquenessValidationPolicyName,
		ValidationActions: []admissionv1.ValidationAction{
			admissionv1.Deny,
		},
	})
	vapbm.MetadataMutator().WithLabels(map[string]string{
		apiconst.ManagedByLabel:      controlplane.ControllerName,
		apiconst.ManagedPurposeLabel: corev2alpha1.ManagedPurposeOIDCProviderNameUniqueness,
	})
	if err := resources.CreateOrUpdateResource(ctx, onboardingCluster.Client(), vapbm); err != nil {
		return fmt.Errorf("error creating/updating ValidatingAdmissionPolicyBinding for OIDC provider name uniqueness validation: %w", err)
	}
	log.Info("ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding for OIDC provider name uniqueness validation created/updated")
	return nil
}
