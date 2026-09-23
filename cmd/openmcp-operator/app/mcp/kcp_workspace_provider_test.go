package mcp

import (
	"context"
	"errors"
	"testing"

	"github.com/kcp-dev/logicalcluster/v3"
	kcpapisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	conditionsv1alpha1 "github.com/kcp-dev/sdk/apis/third_party/conditions/apis/conditions/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

func testDiscoveryScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := kcpapisv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func TestKCPWorkspaceProviderReconcilesBoundWorkspaces(t *testing.T) {
	scheme := testDiscoveryScheme(t)
	discovery := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(&kcpapisv1alpha1.APIExportEndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "services"},
		Status: kcpapisv1alpha1.APIExportEndpointSliceStatus{APIExportEndpoints: []kcpapisv1alpha1.APIExportEndpoint{
			{URL: "https://virtual-workspace.example/services"},
		}},
	}).Build()
	registry := newFakeWorkspaceClusterRegistry("stale")
	provider := &kcpWorkspaceProvider{
		baseConfig:      &rest.Config{Host: "https://kcp.example/clusters/root:providers"},
		endpointSlice:   "services",
		discoveryClient: discovery,
		clusters:        registry,
	}
	provider.listBindings = func(_ context.Context, config *rest.Config) ([]kcpapisv1alpha1.APIBinding, error) {
		if config.Host != "https://virtual-workspace.example/services/clusters/*" {
			t.Fatalf("unexpected virtual workspace host %q", config.Host)
		}
		return []kcpapisv1alpha1.APIBinding{
			workspaceBinding("bound", "92xa9couy27y62m5", kcpapisv1alpha1.APIBindingPhaseBound),
			workspaceBinding("binding", "6s6jlxjbaw7l2m7b", kcpapisv1alpha1.APIBindingPhaseBinding),
		}, nil
	}
	var workspaceHost string
	provider.newCluster = func(config *rest.Config) (cluster.Cluster, error) {
		workspaceHost = config.Host
		return nil, nil
	}

	if err := provider.reconcile(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if workspaceHost != "https://virtual-workspace.example/services/clusters/92xa9couy27y62m5" {
		t.Fatalf("unexpected scoped virtual workspace host %q", workspaceHost)
	}
	if !registry.has("92xa9couy27y62m5") {
		t.Fatal("bound workspace was not engaged")
	}
	if registry.has("6s6jlxjbaw7l2m7b") {
		t.Fatal("workspace with a binding in progress was engaged")
	}
	if registry.has("stale") {
		t.Fatal("stale workspace was not removed")
	}
}

func TestKCPWorkspaceProviderRetainsClustersWhenDiscoveryFails(t *testing.T) {
	tests := []struct {
		name     string
		bindings []kcpapisv1alpha1.APIBinding
		listErr  error
	}{
		{name: "list failure", listErr: errors.New("unavailable")},
		{name: "missing cluster annotation", bindings: []kcpapisv1alpha1.APIBinding{
			workspaceBinding("bound", "", kcpapisv1alpha1.APIBindingPhaseBound),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, registry := testKCPWorkspaceProvider(t, "existing")
			provider.listBindings = func(context.Context, *rest.Config) ([]kcpapisv1alpha1.APIBinding, error) {
				return test.bindings, test.listErr
			}
			provider.newCluster = func(*rest.Config) (cluster.Cluster, error) { return nil, nil }

			if err := provider.reconcile(context.Background(), nil); err == nil {
				t.Fatal("expected discovery to fail")
			}
			if !registry.has("existing") {
				t.Fatal("existing workspace was removed after ambiguous discovery")
			}
		})
	}
}

func testKCPWorkspaceProvider(t *testing.T, names ...multicluster.ClusterName) (*kcpWorkspaceProvider, *fakeWorkspaceClusterRegistry) {
	t.Helper()
	scheme := testDiscoveryScheme(t)
	discovery := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(&kcpapisv1alpha1.APIExportEndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "services"},
		Status: kcpapisv1alpha1.APIExportEndpointSliceStatus{APIExportEndpoints: []kcpapisv1alpha1.APIExportEndpoint{
			{URL: "https://virtual-workspace.example/services"},
		}},
	}).Build()
	registry := newFakeWorkspaceClusterRegistry(names...)
	return &kcpWorkspaceProvider{
		baseConfig:      &rest.Config{Host: "https://kcp.example/clusters/root:providers"},
		endpointSlice:   "services",
		discoveryClient: discovery,
		clusters:        registry,
	}, registry
}

func workspaceBinding(name, logicalCluster string, phase kcpapisv1alpha1.APIBindingPhaseType) kcpapisv1alpha1.APIBinding {
	annotations := map[string]string{}
	if logicalCluster != "" {
		annotations[logicalcluster.AnnotationKey] = logicalCluster
	}
	binding := kcpapisv1alpha1.APIBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations},
		Status:     kcpapisv1alpha1.APIBindingStatus{Phase: phase},
	}
	if phase == kcpapisv1alpha1.APIBindingPhaseBound {
		binding.Status.Conditions = conditionsv1alpha1.Conditions{{Type: conditionsv1alpha1.ReadyCondition, Status: corev1.ConditionTrue}}
	}
	return binding
}

type fakeWorkspaceClusterRegistry struct {
	names map[multicluster.ClusterName]cluster.Cluster
}

func newFakeWorkspaceClusterRegistry(names ...multicluster.ClusterName) *fakeWorkspaceClusterRegistry {
	registry := &fakeWorkspaceClusterRegistry{names: map[multicluster.ClusterName]cluster.Cluster{}}
	for _, name := range names {
		registry.names[name] = &workspaceConfigCluster{config: &rest.Config{}}
	}
	return registry
}

func (r *fakeWorkspaceClusterRegistry) has(name multicluster.ClusterName) bool {
	_, exists := r.names[name]
	return exists
}

func (r *fakeWorkspaceClusterRegistry) Get(_ context.Context, name multicluster.ClusterName) (cluster.Cluster, error) {
	if r.has(name) {
		return r.names[name], nil
	}
	return nil, multicluster.ErrClusterNotFound
}

func (r *fakeWorkspaceClusterRegistry) IndexField(context.Context, client.Object, string, client.IndexerFunc) error {
	return nil
}

func (r *fakeWorkspaceClusterRegistry) ClusterNames() []multicluster.ClusterName {
	names := make([]multicluster.ClusterName, 0, len(r.names))
	for name := range r.names {
		names = append(names, name)
	}
	return names
}

func (r *fakeWorkspaceClusterRegistry) AddOrReplace(
	_ context.Context,
	name multicluster.ClusterName,
	cl cluster.Cluster,
	_ multicluster.Aware,
) error {
	r.names[name] = cl
	return nil
}

func (r *fakeWorkspaceClusterRegistry) Remove(name multicluster.ClusterName) {
	delete(r.names, name)
}

type workspaceConfigCluster struct {
	cluster.Cluster
	config *rest.Config
}

func (c *workspaceConfigCluster) GetConfig() *rest.Config { return c.config }

func TestKCPWorkspaceProviderUpdatesEndpoint(t *testing.T) {
	ctx := context.Background()
	provider, registry := testKCPWorkspaceProvider(t)
	provider.listBindings = func(context.Context, *rest.Config) ([]kcpapisv1alpha1.APIBinding, error) {
		return []kcpapisv1alpha1.APIBinding{workspaceBinding("bound", "tenant", kcpapisv1alpha1.APIBindingPhaseBound)}, nil
	}
	created := 0
	provider.newCluster = func(config *rest.Config) (cluster.Cluster, error) {
		created++
		return &workspaceConfigCluster{config: rest.CopyConfig(config)}, nil
	}
	for _, endpoint := range []string{"https://virtual-workspace.example/services", "https://replacement.example/services"} {
		slice := &kcpapisv1alpha1.APIExportEndpointSlice{}
		if err := provider.discoveryClient.Get(ctx, client.ObjectKey{Name: "services"}, slice); err != nil {
			t.Fatal(err)
		}
		slice.Status.APIExportEndpoints[0].URL = endpoint
		if err := provider.discoveryClient.Update(ctx, slice); err != nil {
			t.Fatal(err)
		}
		if err := provider.reconcile(ctx, nil); err != nil {
			t.Fatal(err)
		}
		current, err := registry.Get(ctx, "tenant")
		if err != nil {
			t.Fatal(err)
		}
		if current.GetConfig().Host != endpoint+"/clusters/tenant" {
			t.Fatalf("workspace retained endpoint %q", current.GetConfig().Host)
		}
		previous := created
		if err := provider.reconcile(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if created != previous {
			t.Fatal("unchanged endpoint recreated the workspace")
		}
	}
	if created != 2 {
		t.Fatalf("expected initial cluster and one replacement, got %d", created)
	}
	provider.newCluster = func(*rest.Config) (cluster.Cluster, error) { return nil, errors.New("invalid endpoint configuration") }
	slice := &kcpapisv1alpha1.APIExportEndpointSlice{}
	if err := provider.discoveryClient.Get(ctx, client.ObjectKey{Name: "services"}, slice); err != nil {
		t.Fatal(err)
	}
	slice.Status.APIExportEndpoints[0].URL = "https://invalid.example/services"
	if err := provider.discoveryClient.Update(ctx, slice); err != nil {
		t.Fatal(err)
	}
	if err := provider.reconcile(ctx, nil); err == nil {
		t.Fatal("expected cluster construction failure")
	}
	current, err := registry.Get(ctx, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	if current.GetConfig().Host != "https://replacement.example/services/clusters/tenant" {
		t.Fatal("failed replacement discarded existing workspace")
	}
}
