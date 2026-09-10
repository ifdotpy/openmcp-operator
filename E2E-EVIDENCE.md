# kcp-native ControlPlane e2e — cc-d2, 2026-09-10

Downstream build `ghcr.io/apeirora/openmcp-operator:v1.3.0-kcp.3` (this branch),
deployed on the cc-d2 platform cluster via the `managedcontrolplane` PlatformService
(runCommand: `mcp run --kcp-endpoint-slice=core.open-control-plane.io --kcp-kubeconfig=...`,
init: `mcp init --kcp-mode`). No onboarding cluster involved; old PoC sync-agents scaled to 0.

## Lifecycle (Platform Mesh account `root:orgs:showroom:ig-mcp-test`)

1. `ControlPlane pm-e2e` created in namespace `e2e` of the account workspace (18:44).
2. Controller engaged the workspace through the APIExport virtual workspace,
   created `ClusterRequest` in per-tenant platform namespace `mcp--a4c13601-...`
   (cluster-qualified hash: StableMCPNamespaceCtx).
3. Gardener shoot `s-zb3enhnf` created (openstack, 1 worker), all conditions True.
4. Access token `admin` delivered as secret `token-admin.pm-e2e.kubeconfig`
   INTO the account workspace; verified live: `kubectl get ns/nodes` against the
   new cluster with exactly that kubeconfig succeeded (21:05-21:06).
5. Delete (21:06) → full cascade by 22:07: ControlPlane gone from the workspace,
   secret gone, platform namespace gone, shoot gone from garden-pm-cc-d2.

## Isolation

- `ig-1` (bound, empty): API served, zero objects, zero reconciles.
- `ig-2` (not bound): the ControlPlane API does not exist at all.

## Findings folded into this branch during the run

- Service providers in kcp mode need permission claims beyond their own API:
  `secrets` (kubeconfig delivery into the workspace) and `namespaces`. Added to
  the `core.open-control-plane.io` APIExport + accepted in bindings.
- Deletion gate: an unbound service API (NoMatch) now counts as "no service
  resources" instead of blocking deletion forever (fix commit in this branch).
- Per-cluster REST mapping is cached from engagement time: APIs bound to a
  workspace after engagement need a provider re-engage (operator restart) to
  be visible. Known limitation for the ADR discussion.

## Not covered yet

- service-provider-flux in kcp mode (needs the identity unification + downstream build).
- Marketplace tile visibility in the Portal UI (label + ProviderMetadata +
  bind-RBAC applied to the export; Portal check pending).
