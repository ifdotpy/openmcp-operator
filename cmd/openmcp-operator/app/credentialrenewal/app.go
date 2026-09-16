package credentialrenewal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	renewal "github.com/openmcp-project/openmcp-operator/internal/credentialrenewal"
)

func NewCommand() *cobra.Command {
	var file, namespace string
	cmd := &cobra.Command{
		Use:   "renew-kcp-credentials",
		Short: "Renew KCP credentials stored in Kubernetes Secrets",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), file, namespace)
		},
	}
	cmd.Flags().StringVar(&file, "targets", "", "Credential target JSON file")
	cmd.Flags().StringVar(&namespace, "namespace", "", "Hosting Secret namespace")
	return cmd
}

func run(ctx context.Context, file, namespace string) error {
	if file == "" || namespace == "" {
		return fmt.Errorf("targets and namespace are required")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var targets []renewal.Target
	if err := json.Unmarshal(data, &targets); err != nil {
		return err
	}
	if len(targets) == 0 {
		return fmt.Errorf("no credential targets")
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	cfg.Timeout = 30 * time.Second
	host, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	connect := func(cfg *rest.Config) (kubernetes.Interface, error) { return kubernetes.NewForConfig(cfg) }
	var failures int
	for _, target := range targets {
		renewCtx, cancel := context.WithTimeout(ctx, time.Minute)
		err := renewal.Renew(renewCtx, host, namespace, target, connect, time.Now())
		cancel()
		if err != nil {
			failures++
			fmt.Fprintf(os.Stderr, "%s: renewal failed: %v\n", target.Secret, err)
		}
	}
	if failures > 0 {
		return fmt.Errorf("%d credential renewals failed", failures)
	}
	return nil
}
