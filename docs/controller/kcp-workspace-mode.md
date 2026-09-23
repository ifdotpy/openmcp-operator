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

Removing a provider from the configuration starts retirement. The operator keeps its existing runtime, access, and shared registrations while any service object remains, including objects with deletion finalizers. It does not delete service objects. After all services are gone, it revokes the ClusterRequests, AccessRequests, credentials, and tenant grants previously owned by the workspace runtime. Runtime resources remain until request finalizers finish. Requests owned by other controllers are preserved.

The operator still completes request deletion initiated by a service provider. Providers delete their requests before removing the service finalizer. After APIExport disengagement, cleanup finalizes requests already marked for deletion while it keeps active requests and the provider runtime.

ServiceProvider resource descriptors remain on the platform cluster so retirement can continue after an operator restart. Keep the service APIs available until retirement completes. Missing descriptors or unreadable service APIs stop cleanup. Restore the provider configuration if its runtime was already removed; descriptors alone cannot recreate deployment configuration.

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

An optional HTTPS admission guard prevents deleting the APIBinding while configured
service objects still exist. Enable it with `--kcp-disconnect-guard-address`,
`--kcp-disconnect-guard-url`, `--kcp-disconnect-guard-cert`, and
`--kcp-disconnect-guard-key`. Use `--kcp-disconnect-guard-ca` for a private CA.
The guard inspects all namespaces and fails closed when inspection fails. Deleting
the entire workspace remains permitted so its normal cleanup can proceed.

### Shared service providers

Set `registrationNamespace` on a provider entry to use an externally deployed,
shared provider instead of one provider Deployment per workspace. Deploy its
ServiceAccount with the configured provider `name` in that namespace. The operator
binds that account to the tenant runtime namespace and publishes a labelled
onboarding kubeconfig Secret in the registration namespace. The credential grants
access to the configured service API group in that workspace.

Each registration Secret is named after the globally unique onboarding namespace.
The shared provider uses the kubeconfig provider from multicluster-runtime and must
reject service objects outside that registered namespace. This preserves existing
platform-side access identities when switching deployment modes. Credential updates
are propagated to the registration Secret. Its owner is the tenant runtime Namespace;
removing that Namespace also removes the registration through garbage collection.
Service deletion must finish before the workspace and its registration are removed.

Shared provider deployments and their registration namespaces are managed by the
installation, not by this operator. Managed service controllers can still run per
tenant; sharing a service provider does not share the managed service instance.
