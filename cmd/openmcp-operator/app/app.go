package app

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/credentialrenewal"
	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/helm"
	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/mcp"
	"github.com/openmcp-project/openmcp-operator/cmd/openmcp-operator/app/options"
)

func NewOpenMCPOperatorCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "openmcp-operator",
		Short: "Commands for interacting with the openmcp-operator",
	}
	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	po := options.NewPersistentOptions()
	po.AddPersistentFlags(cmd)
	cmd.AddCommand(NewInitCommand(po))
	cmd.AddCommand(NewRunCommand(po))
	cmd.AddCommand(mcp.NewMCPControllerSubcommand(ctx, po))
	cmd.AddCommand(helm.NewHelmDeployerSubcommand(ctx, po))
	cmd.AddCommand(credentialrenewal.NewCommand())

	return cmd
}
