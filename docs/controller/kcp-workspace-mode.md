# KCP Workspace Mode

KCP workspace mode watches one `APIExportEndpointSlice`. Each engaged KCP workspace becomes one openMCP control plane.

The operator uses no Platform Mesh API. It uses these KCP signals:

- APIExport engagement starts the workspace runtime.
- The configured `APIBinding` owns all workspace resources.
- APIExport disengagement removes the workspace runtime after the cleanup delay.

The operator creates `ControlPlane/default` in the engaged workspace. It registers the same workspace for onboarding and MCP use. It does not create a nested Kubernetes cluster.

Enable the mode with these flags:

```text
--kcp-endpoint-slice=<APIExportEndpointSlice name>
--kcp-kubeconfig=<provider workspace kubeconfig>
--kcp-binding-name=<APIBinding name>
```

Use `--kcp-service-providers` to supply optional service providers. The file contains a JSON array:

```json
[
  {
    "name": "example-provider",
    "image": "registry.example/provider:v1",
    "providerName": "example-config",
    "resource": {
      "group": "services.example.io",
      "version": "v1alpha1",
      "kind": "Example"
    },
    "clusterRoleRules": [
      {
        "apiGroups": ["services.example.io"],
        "resources": ["providerconfigs"],
        "verbs": ["get", "list", "watch"]
      }
    ]
  }
]
```

The operator deploys one instance of each configured provider for each engaged workspace. Each provider receives a short-lived kubeconfig for that workspace only. Provider code does not need KCP support.

The operator does not contain a fixed provider list. A product catalog or another deployment source owns provider images, service GVKs, names, and extra RBAC.
