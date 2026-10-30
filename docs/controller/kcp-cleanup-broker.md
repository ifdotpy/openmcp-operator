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
6. Persist the revocation phase before the first revoke request.
7. Delete the provider ClusterRoleBinding, then its ClusterRole.
8. Delete the external ServiceAccount, then the durable Secret record.

A denied request stops cleanup and retains the record.
A restart in the revocation phase retries deletion without token renewal or new service grants.
A replaced account or AccessRequest cannot receive the old credential.
The ControlPlane controller uses its own credential for its final update.

The unit tests use real HTTP token and service clients with a local authorization fixture.
They cover account deletion, APIBinding deletion, ControlPlane cleanup, token expiration, restart, denial, and revocation retry.
The negative control removes the old account-local issuer and requires its renewal to fail.
A second ControlPlane test calls the real ControlPlane reconciler with fake API objects.
It checks the service and runtime deletion requests before broker revocation.
These tests do not verify KCP's live namespace sweep, authorization caches, required groups, or the Flux and External Secrets controllers.
Repeat the lifecycle test against the deployed KCP version before rollout.

## Account RBAC removal boundary

KCP can remove the account's broker grant after the last service disappears.
The broker might then receive `Forbidden` before it observes service absence.
It retains the external ServiceAccount and record because denial does not prove account deletion.
This can leave final credential revocation pending after the account is gone.

The provider binaries release service finalizers outside this repository.
The operator cannot record a completion acknowledgement before those writes through the current provider API.
A complete acknowledgement needs a provider runtime change or a broker-owned service finalizer installed before service creation.
This patch does not add that mechanism.
Restore broker access to complete revocation when the account still exists.
Use the dev rollout check to measure this boundary before deployment.
