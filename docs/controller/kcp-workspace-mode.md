# KCP Workspace Mode

KCP workspace mode watches one `APIExportEndpointSlice`. Each engaged KCP workspace becomes one open control plane.

The operator currently contains a small discovery adapter between KCP's `APIExportEndpointSlice` API and `multicluster-runtime`. The released `kcp-dev/multicluster-provider` v0.8.0 uses controller-runtime v0.24, while openmcp-operator uses v0.25. Replace this adapter with the upstream provider after a controller-runtime v0.25 compatible release is available. The adapter only discovers engaged workspaces.

The mode depends only on KCP APIs and these lifecycle signals:

- APIExport engagement starts the workspace runtime.
- The matching `APIBinding` owns workspace-local bootstrap and access objects.
- APIExport disengagement removes the host runtime after the cleanup delay.

The operator creates `ControlPlane/default` in the engaged workspace. The ControlPlane controller creates its normal host namespace, `ClusterRequest`, and `AccessRequest`. The workspace runtime registers the same workspace as the onboarding and MCP API cluster, grants requests owned by the ControlPlane controller or explicitly configured providers, and issues bounded credentials scoped to that workspace. It does not create a nested Kubernetes cluster.

Enable the mode with these flags:

```text
--kcp-endpoint-slice=<APIExportEndpointSlice name>
--kcp-kubeconfig=<provider workspace kubeconfig>
```

The operator reads the export reference from the endpoint slice and finds the matching `APIBinding` in each workspace. Use the optional `--kcp-binding-name` flag only to prefer a stable binding name when one exists.

The optional runtime flags control reconciliation, cleanup, and credential duration:

```text
--kcp-workspace-reconcile-interval=5s
--kcp-workspace-cleanup-delay=1m
--kcp-workspace-token-lifetime=1h
```

Optional per-workspace provider deployments are configured with `--kcp-service-providers=<JSON file>`. The operator creates a dedicated ServiceAccount and RBAC for each deployment and removes them when the workspace runtime is removed. The configuration supplies the provider image, resource GVK, additional arguments, and required RBAC. No particular provider is built into the operator.

Service-controller placement remains the responsibility of each provider. A KCP workspace serves APIs but has no Kubernetes workload APIs such as `Deployment` or `Service`. A provider that installs controllers must choose a Kubernetes cluster independently and use the ControlPlane credential to address the KCP workspace. This keeps the operator's KCP support independent of any product-specific provider or hosting platform.

Example provider configuration (RBAC rules depend on the provider):

```json
[
  {
    "name": "example-provider",
    "providerName": "example",
    "image": "registry.example/provider:v1",
    "resource": {"group": "services.example.io", "version": "v1", "kind": "Example"},
    "args": ["--service-controller-cluster=platform"],
    "roleRules": [],
    "clusterRoleRules": []
  }
]
```

The operator resolves only onboarding and MCP requests to the workspace API.
Workload requests remain the responsibility of the normal cluster scheduler.
Provider configuration is trusted administrator input; its RBAC rules must match
what the configured provider needs on the platform cluster.
