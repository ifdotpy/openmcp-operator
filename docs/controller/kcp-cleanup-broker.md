# KCP cleanup broker

The operator creates one cleanup ServiceAccount for each provider AccessRequest.
The ServiceAccount and its Secret record stay in `openmcp-cleanup` in the permanent provider workspace.
They have no owner in the account workspace or the runtime namespace.
The provider workspace must stay outside the account's deletion tree.

The record contains the account UID, ServiceProvider UID, AccessRequest UID, runtime namespace UID, resource mapping, credential expiration, and cleanup phase.
The broker reads these records on startup.
It renews tokens through the native provider endpoint with its own credential.
It does not require consumer discovery or the previous runtime token.

## Deployment prerequisites

An administrator must create `Namespace/openmcp-cleanup` in the permanent provider workspace before the operator starts.
The credential from `--kcp-kubeconfig` needs these permissions:

- Get the provider `LogicalCluster/cluster` to read its immutable logical cluster name.
- Get `Namespace/openmcp-cleanup`.
- Get, list, create, update, and delete Secrets in that namespace.
- Get, create, update, and delete ServiceAccounts in that namespace.
- Create `serviceaccounts/token` in that namespace.
- Create `authentication.k8s.io/selfsubjectreviews` to read the authenticated broker username.

The existing APIExport claims must permit the operator to create account ClusterRoles and ClusterRoleBindings.
They must also permit the operator to grant the rules below.
The current deployment does not have the native provider permissions above.
The patch requires the administrator to add them before rollout.

Before it grants provider access, the operator creates one permanent broker User binding in each account.
It uses the username returned by SelfSubjectReview.
For a ServiceAccount broker, it converts that local username to the global KCP username.
It uses the authenticated origin cluster from SelfSubjectReview extras, which can differ from the provider workspace.
It rejects a missing or ambiguous ServiceAccount origin.
The binding has no APIBinding owner.
Its ClusterRole permits only:

- `access` on the non-resource URL `/`.
- `get` on `LogicalCluster/cluster`.
- `get` on the account's service APIBinding.
- `get` and `delete` on the named provider cleanup ClusterRoles and ClusterRoleBindings.

The operator extends `resourceNames` when it creates another provider cleanup identity.
The broker binding has no service permissions.
It remains until account deletion so the broker can retry a crash after provider identity revocation.
KCP removes account RBAC after account content deletion.

The operator grants the external cleanup identity access to `/`, service reads, service updates and patches, and status updates and patches.
It uses the immutable provider logical cluster name in the foreign ServiceAccount username.
It grants no wildcard resource or verb.
Namespace permissions and role references keep their existing AccessRequest semantics.
The operator checks authorization before ControlPlane provisioning can create services.
Required-group policies must also permit the foreign cleanup identity.

## Cleanup order

1. Renew the provider token before expiration, including after a restart.
2. Keep the runtime and credential while any provider service remains.
3. Let the provider complete its final status update.
4. Let the provider remove its service finalizer.
5. Confirm service absence through the provider credential.
6. Persist `services-final` before runtime teardown, or persist revocation after the broker observes service absence.
7. Delete the external ServiceAccount and remove the token from the record.
8. Persist `grant-retry`, then delete the provider ClusterRoleBinding and its ClusterRole.
9. Save completion on the UID-matched platform AccessRequest, then delete the record.

Before service completion is confirmed, a denied request stops cleanup and retains the record.
A restart in the revocation phase retries deletion without token renewal or new service grants.
A replaced account or AccessRequest cannot receive the old credential.
An AccessRequest replacement marks the old record as draining before credential publication.
After service completion, the broker revokes the old identity without publishing to the replacement.
Workspace cleanup preserves provider cleanup grants until the broker revokes them.
The ControlPlane controller uses its own credential for its final update.

The unit tests use real HTTP token and service clients with a local authorization fixture.
They cover account deletion, APIBinding deletion, ControlPlane cleanup, token expiration, restart, denial, and revocation retry.
The negative control removes the old account-local issuer and requires its renewal to fail.
A second ControlPlane test calls the real ControlPlane reconciler with fake API objects.
It checks the service and runtime deletion requests before broker revocation.
These tests do not verify KCP's live namespace sweep, authorization caches, required groups, or the Flux and External Secrets controllers.
Repeat the lifecycle test against the deployed KCP version before rollout.

## Account RBAC removal boundary

KCP removes account RBAC after account content deletion.
A native request can then return `Forbidden` even when the LogicalCluster no longer exists.

The runtime persists `services-final` after its service absence checks and before runtime teardown.
The broker also persists the revocation phase after its own service absence check.
These external checkpoints survive account deletion.
After a completion checkpoint, the broker first deletes the external ServiceAccount and token material.
It keeps a token-free `grant-retry` record until it removes the account grants.
If the LogicalCluster GET returns `Forbidden` or `NotFound`, the broker can remove the retry record.
If the GET succeeds but grant deletion is denied, it keeps the retry record and tries again.
A restart in this phase cannot renew the identity.

Before it removes the record, the broker saves completion on the matching platform AccessRequest.
The AccessRequest finalizer uses that result and does not call legacy account grant cleanup.
Without this result, an absent broker record keeps the legacy finalization path.
The runtime skips the generic legacy sweep only when every runtime AccessRequest is terminating and has broker proof.
The proof is a matching durable record or a completion annotation that matches the request UID.
Mixed requests and legacy requests keep the sweep and its denial behavior.

Without a completion checkpoint, denial retains the external ServiceAccount and record.
The code must observe service completion before it can use this rule.
It does not receive an acknowledgement from the provider before the last service finalizer is removed.
Real KCP authentication, authorization, and sweep order still require the dev rollout check.
