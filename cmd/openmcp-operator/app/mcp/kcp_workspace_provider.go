package mcp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/kcp-dev/logicalcluster/v3"
	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	kcpconditions "github.com/kcp-dev/sdk/apis/third_party/conditions/util/conditions"
	kcpclientset "github.com/kcp-dev/sdk/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	mcclusters "sigs.k8s.io/multicluster-runtime/pkg/clusters"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/openmcp-project/controller-utils/pkg/logging"
)

type workspaceClusterRegistry interface {
	Get(context.Context, multicluster.ClusterName) (cluster.Cluster, error)
	IndexField(context.Context, client.Object, string, client.IndexerFunc) error
	ClusterNames() []multicluster.ClusterName
	AddOrReplace(context.Context, multicluster.ClusterName, cluster.Cluster, multicluster.Aware) error
	Remove(multicluster.ClusterName)
}

// kcpWorkspaceProvider bridges KCP APIExport discovery to multicluster-runtime.
// Keep this adapter isolated: it can be replaced by kcp-dev/multicluster-provider
// after that project publishes a controller-runtime v0.25 compatible release.
type kcpWorkspaceProvider struct {
	baseConfig        *rest.Config
	endpointSlice     string
	scheme            *runtime.Scheme
	reconcileInterval time.Duration
	log               logging.Logger
	discoveryClient   client.Client
	clusters          workspaceClusterRegistry
	listBindings      func(context.Context, *rest.Config) ([]kcpapisv1alpha1.APIBinding, error)
	newCluster        func(*rest.Config) (cluster.Cluster, error)
}

var _ multicluster.Provider = (*kcpWorkspaceProvider)(nil)
var _ multicluster.ProviderRunnable = (*kcpWorkspaceProvider)(nil)

func newKCPWorkspaceProvider(
	baseConfig *rest.Config,
	endpointSlice string,
	scheme *runtime.Scheme,
	reconcileInterval time.Duration,
	log logging.Logger,
	discoveryClient client.Client,
) *kcpWorkspaceProvider {
	clusters := mcclusters.New[cluster.Cluster]()
	clusters.ErrorHandler = log.Error
	provider := &kcpWorkspaceProvider{
		baseConfig:        baseConfig,
		endpointSlice:     endpointSlice,
		scheme:            scheme,
		reconcileInterval: reconcileInterval,
		log:               log,
		discoveryClient:   discoveryClient,
		clusters:          &clusters,
	}
	provider.listBindings = listWorkspaceAPIBindings
	provider.newCluster = func(config *rest.Config) (cluster.Cluster, error) {
		return cluster.New(config, func(options *cluster.Options) {
			options.Scheme = scheme
			options.Logger = log.Logr()
		})
	}
	return provider
}

func (p *kcpWorkspaceProvider) Get(ctx context.Context, name multicluster.ClusterName) (cluster.Cluster, error) {
	return p.clusters.Get(ctx, name)
}

func (p *kcpWorkspaceProvider) IndexField(
	ctx context.Context,
	object client.Object,
	field string,
	extractValue client.IndexerFunc,
) error {
	return p.clusters.IndexField(ctx, object, field, extractValue)
}

func (p *kcpWorkspaceProvider) Start(ctx context.Context, aware multicluster.Aware) error {
	p.reconcileAndLog(ctx, aware)
	ticker := time.NewTicker(p.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.reconcileAndLog(ctx, aware)
		}
	}
}

func (p *kcpWorkspaceProvider) reconcileAndLog(ctx context.Context, aware multicluster.Aware) {
	if err := p.reconcile(ctx, aware); err != nil && ctx.Err() == nil {
		p.log.Error(err, "KCP workspace discovery failed")
	}
}

func (p *kcpWorkspaceProvider) reconcile(ctx context.Context, aware multicluster.Aware) error {
	desired, err := p.discover(ctx)
	if err != nil {
		return err
	}

	for name, config := range desired {
		existing, err := p.clusters.Get(ctx, name)
		if err == nil && existing != nil && existing.GetConfig().Host == config.Host {
			continue
		}
		if err != nil && !errors.Is(err, multicluster.ErrClusterNotFound) {
			return fmt.Errorf("read workspace cluster %q: %w", name, err)
		}
		workspace, err := p.newCluster(config)
		if err != nil {
			return fmt.Errorf("construct workspace cluster %q: %w", name, err)
		}
		if err := p.clusters.AddOrReplace(ctx, name, workspace, aware); err != nil {
			return fmt.Errorf("engage workspace cluster %q: %w", name, err)
		}
	}

	for _, name := range p.clusters.ClusterNames() {
		if _, exists := desired[name]; !exists {
			p.clusters.Remove(name)
		}
	}
	return nil
}

func (p *kcpWorkspaceProvider) discover(ctx context.Context) (map[multicluster.ClusterName]*rest.Config, error) {
	slice := &kcpapisv1alpha1.APIExportEndpointSlice{}
	if err := p.discoveryClient.Get(ctx, client.ObjectKey{Name: p.endpointSlice}, slice); err != nil {
		return nil, fmt.Errorf("read APIExportEndpointSlice %q: %w", p.endpointSlice, err)
	}
	if len(slice.Status.APIExportEndpoints) == 0 {
		return nil, fmt.Errorf("APIExportEndpointSlice %q has no endpoints", p.endpointSlice)
	}

	desired := map[multicluster.ClusterName]*rest.Config{}
	for _, endpoint := range slice.Status.APIExportEndpoints {
		endpointConfig := rest.CopyConfig(p.baseConfig)
		endpointConfig.Host = strings.TrimSuffix(endpoint.URL, "/") + "/clusters/*"
		bindings, err := p.listBindings(ctx, endpointConfig)
		if err != nil {
			return nil, fmt.Errorf("list APIBindings through endpoint %q: %w", endpoint.URL, err)
		}
		for i := range bindings {
			binding := &bindings[i]
			if !kcpconditions.IsTrue(binding, conditionsv1alpha1.ReadyCondition) {
				continue
			}
			name := multicluster.ClusterName(logicalcluster.From(binding).String())
			if name.Empty() {
				return nil, fmt.Errorf("bound APIBinding %q from endpoint %q has no logical cluster annotation", binding.Name, endpoint.URL)
			}
			// Keep workspace clients on the APIExport virtual workspace. The
			// provider identity is authorized there for exported resources and
			// accepted permission claims; it is intentionally not granted direct
			// access to every consumer workspace.
			config, err := directWorkspaceConfig(endpointConfig, name)
			if err != nil {
				return nil, err
			}
			desired[name] = config
		}
	}
	return desired, nil
}

func listWorkspaceAPIBindings(ctx context.Context, config *rest.Config) ([]kcpapisv1alpha1.APIBinding, error) {
	clientset, err := kcpclientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	bindings, err := clientset.ApisV1alpha1().APIBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return bindings.Items, nil
}

func directWorkspaceConfig(base *rest.Config, name multicluster.ClusterName) (*rest.Config, error) {
	if base == nil {
		return nil, fmt.Errorf("kcp configuration is required")
	}
	parsed, err := url.Parse(base.Host)
	if err != nil {
		return nil, fmt.Errorf("parse kcp host: %w", err)
	}
	const marker = "/clusters/"
	index := strings.Index(parsed.Path, marker)
	if parsed.Scheme == "" || parsed.Host == "" || index < 0 || name == "" {
		return nil, fmt.Errorf("kcp host %q cannot address workspace %q", base.Host, name)
	}
	parsed.Path = parsed.Path[:index] + marker + url.PathEscape(string(name))
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	config := rest.CopyConfig(base)
	config.Host = parsed.String()
	return config, nil
}
