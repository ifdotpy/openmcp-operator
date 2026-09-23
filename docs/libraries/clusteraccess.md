# The ClusterAccess Library

The ClusterAccess library in `lib/clusteraccess` is all about getting access to k8s clusters represented by the `Cluster` resource. It can be used in a few different ways which are outlined in this document.

## Background: MCP Clusters, Workload Clusters, and Service Providers

This section describes the open control plane architecture used by this library.

Customers have access to a single, shared **onboarding cluster**. They can create `ManagedControlPlane` resources (MCPs) there and request services by creating *service resources* (e.g. for `Flux`, `Landscaper`, etc.) next to the MCPs. Each MCP results in an **MCP cluster** - the customer has access to the ones belonging to his own MCPs and the APIs for the services he requested (k8s CRDs) will be made available on this cluster. As this is a managed service, the customer should not have access to any managed controllers, therefore the service controllers (e.g. the Flux controller, the Landscaper controller, etc.) are usually running in so-called **workload clusters**. Workload clusters cannot be accessed by customers and controllers for multiple different MCPs can be hosted on the same workload cluster.

The available services are determined by which **service providers** are deployed in the landscape. A service provider is responsible for watching the service resources and reacting on them. For example, the Landscaper service provider would watch the Landscaper service resource (which is named `Landscaper`) on the onboarding cluster, and if a new one is created next to an MCP, it would deploy a Landscaper instance (meaning the controllers required to run Landscaper) on any workload cluster and configure them to watch the MCP cluster belonging to the MCP the service resource was created next to. This makes the Landscaper resources (`Installation`, `Target`, etc.) available on the MCP cluster so that the customer can use the Landscaper without having to manage its lifecycle.

As all service providers work similarly, they usually need access to the MCP cluster (to deploy CRDs, for example) and a workload cluster (to deploy the service controllers). As the process for getting access to both clusters is not too intuitive, we decided to build this library to make the development of new service providers easier, more efficient, and less error-prone.

## ClusterAccess Manager

The `Manager` interface mainly specifies the `CreateAndWaitForCluster` and `WaitForClusterAccess` methods. They can be used to create a `ClusterRequest` and `AccessRequest` or just the latter one.

Note that the methods are expected to wait for readiness of the created resources and therefore not suitable to be used within a controller's `Reconcile` method. This is meant to be used in provider's 'init' subcommand, wich is executed as a one-time job, for example.

The existing implementation can be instantiated with `NewClusterAccessManager`.

##### Example

```go
mgr := clusteraccess.NewClusterAccessManager(platformClusterClient, "my-controller", "my-namespace").WithInterval(30 * time.Second)
access, err := mgr.CreateAndWaitForCluster(...)
```

## ClusterAccess Reconciler

The ClusterAccess Reconciler has the same purpose as the Manager - granting access to clusters. However, it is designed to be used within a controller's `Reconcile` method. Instead of waiting for readiness of the created resources, it returns an interval after which the currently reconciled object should be requeued, if the resources are not yet ready.

There are two variants of the ClusterAccess Reconciler: simple and advanced.

The 'simple' ClusterAccess Reconciler lies in `lib/clusteraccess`. It is designed for the original use-case: service providers which need acces to the Managed Control Plane cluster to deliver the service API and a Workload cluster which hosts the kubernets workload of a service instance.

The 'advanced' ClusterAccess Reconciler is in `lib/clusteraccess/advanced`. It can be configured to grant access to arbitrary clusters, either static or depending on the reconciled object. It is possible to either create a new `ClusterRequest` or reference existing `ClusterRequest` or `Cluster` resources. Due to this flexibility, it is significantly more complex to configure than the simple variant, though.

### ClusterAccess Reconciler - Simple

Instantiate the ClusterAccess Reconciler during controller setup and store the instance in the controller's struct.

During reconciliation, call its `Reconcile` method (or `ReconcileDelete`, if the reconciled object is being deleted), which will ensure the required `ClusterRequest` (if any) and `AccessRequest` resources. If the method returns a `reconcile.Result` with a non-zero `RequeueAfter` value, abort the reconciliation and return the given `reconcile.Result`. If not, reconciliation can continue and the `MCPCluster` and `WorkloadCluster` methods can be used to get access to the respective clusters.

During reconciliation, only the `MCPCluster`, `MCPAccessRequest`, `WorkloadCluster`, `WorkloadAccessRequest`, `Reconcile`, and `ReconcileDelete` methods of the reconciler must be used.

##### Example

```go
// controller constructor
func NewMyController(platformClusterClient client.Client, ...) *MyController {
  return &MyController{
    car: clusteraccess.NewClusterAccessReconciler(platformClusterClient, "my-controller").
      WithMCPPermissions(...).
      WithMCPScheme(...).
      WithWorkloadPermissions(...).
      WithWorkloadScheme(...),
  }
}

// reconcile
func (c *MyController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
  ...

  if inDeletion {
    res, err := c.car.ReconcileDelete(ctx, req)
    if err != nil || res.RequeueAfter > 0 {
      return res, err
    }
  } else {
    res, err := c.car.Reconcile(ctx, req)
    if err != nil || res.RequeueAfter > 0 {
      return res, err
    }

    ...

    mcpAccess, err := c.car.MCPCluster(ctx, req)

    ...
  }
}
```

#### Noteworthy Features

The ClusterAccess Reconciler's `SkipWorkloadCluster` method can be used during initialization to disable creation of a `ClusterRequest` for a workload cluster.
If for some reason the `AccessRequest` resources are required, they can be retrieved via `MCPAccessRequest` and `WorkloadAccessRequest`.

The ClusterAccess Reconciler remembers requests that are in deletion and won't create new resources for them. This means that `Reconcile` can safely be called at the beginning of a reconciliation that is going to delete resources and `ReconcileDelete` at the end of it, without the former one recreating resources the latter one has already removed.
A request is considered to be 'in deletion' when `ReconcileDelete` is called for it and it stops being 'in deletion' when `ReconcileDelete` returns with a `RequeueAfter` value of zero and no error.

### ClusterAccess Reconciler - Advanced

Instantiate the ClusterAccess Reconciler during controller setup and store the instance in the controller's struct.
```go
// controller constructor
func NewMyController(platformClusterClient client.Client, ...) *MyController {
  car := advanced.NewClusterAccessReconciler(platformClusterClient, "my-controller")

  // register clusters and configure reconciler here

  return &MyController{
    car: car,
  }
}
```

#### Cluster Registrations

Opposed to the simple ClusterAccess Reconciler, the advanced variant is not limited to MCP and/or workload clusters. Instead, clusters for which access should be provided during the reconciliation need to be registered. The package contains three different constructors for cluster registrations:
- `NewClusterRequest` causes the reconciler to create a new `ClusterRequest` during the first reconciliation. Use this if you need access to a new cluster and not an existing one.
  - Note that a `ClusterRequest` does not necessarily result in a new cluster, it could also return a reference to an existing cluster that is eligible for sharing.
- `ExistingCluster` can be used to get access to an existing `Cluster` when name and namespace of the `Cluster` are known.
- `ExistingClusterRequest` can be used to get access to an existing `Cluster` where name and namespace of the `Cluster` are not known, but an existing `ClusterRequest` for the `Cluster` is.

All of these constructors take similar arguments:
- `id` is an identifier, used to differentiate between multiple clusters that belong to the same reconciled object. It must be unique among all cluster registrations.
  - Example: `mcp` and `workload`.
  - Registering the same `id` multiple times causes the last registration to overwrite any previous one.
- `suffix` is a suffix to use for the resources (`AccessRequest` and `ClusterRequest`, potentially) created for this cluster registration. It must be uniqe among all cluster registrations.
  - If empty, `id` will be used as suffix.
  - In the final resource names, the suffix will be separated from the rest of the name via `--`. The suffix must not contain any prefixing dashes or similar separators.
  - Example: `mcp` for MCP clusters, `wl` for workload clusters.
- A generator function that generates the required information based on the request (name and namespce of the currently reconciled object) and potential further information.
  - `NewClusterRequest` takes a function that returns a `ClusterRequestSpec`. The resulting spec will be used for the created `ClusterRequest`.
    - If the spec is actually static and does not depend on the reconciled object, the `StaticClusterRequestSpecGenerator` helper function can be passed with a static spec definition as argument.
  - `ExistingCluster` and `ExistingClusterRequest` both take a function that results in an `ObjectReference`, pointing to the existing `Cluster` or `ClusterRequest`, respectively.
    - If the returned reference is actually static and does not depend on the reconciled object, the `StaticReferenceGenerator` helper function can be passed with a static reference definition as argument.
    - If the registration should use the same name and namespace as the reconciled object, `IdentityReferenceGenerator` can be used.

These constructors return a `ClusterRegistrationBuilder`. Its `Build` method can be used to turn it into a `ClusterRegistration`, but there are some methods that can be used to configure it before doing that:
- `WithTokenAccessGenerator` takes a function that generates the token configuration to be used in the `AccessRequest` for this cluster registration, depending on the request (= reconciled object). `WithOIDCAccessGenerator` is the equivalent for OIDC-based access.
  - If the configuration does not depend on the request, the static alternatives `WithTokenAccess` and `WithOIDCAccess` can be used.
  - Only one of either OIDC-based access or token-based access can be configured. This means that all four of the aforementioned methods are mutually exclusive - calling any of them will overwrite any call to any of them that has happened before.
  - Note that one of these methods must be used in order to have an `AccessRequest` created for the cluster registration. Otherwise, no access can be provided for the corresponding cluster. There might be use-cases where this is desired, but since the main purpose of this library is to grant access to clusters, they will be rare.
- `WithScheme` can be used to set the scheme for the `client.Client` that can later be retrieved for the cluster registration. Uses the default scheme if not called or called with a `nil` argument.
- `WithNamespaceGenerator` can be used to overwrite the logic for computing the namespace the created resources should be placed in.
  - By default, the logic from the `DefaultNamespaceGenerator` function is used. It computes a UUID-style hash from name and namespace of the request.
  - For resources that are related to MCPs, the `DefaultNamespaceGeneratorForMCP` should be used instead. It uses the library function that is commonly used to determine the MCP namespace on the platform cluster.
  - If a static namespace is desired, the `StaticNamespaceGenerator` helper function can be used.
  - `RequestNamespaceGenerator` is a namespace generator that returns the namespace of the reconciled object.

```go
// controller constructor
func NewMyController(platformClusterClient client.Client, ...) *MyController {
  car := advanced.NewClusterAccessReconciler(platformClusterClient, "my-controller")

  // This registers a to-be-created ClusterRequest.
  // Its name and namespace are generated by the reconciler.
  // Since the spec does not depend on the request, the helper function for static values is used.
  // Token-based access for the cluster is configured.
  car.Register(advanced.NewClusterRequest("foo", "foo", advanced.StaticClusterRequestSpecGenerator(&clustersv1alpha1.ClusterRequestSpec{
    Purpose: "foo",
  })).WithTokenAccess(&clustersv1alpha1.TokenConfig{...}).Build())

  // This registers an existing cluster with the same name and namespace as the reconciled object.
  // Token-based access for the cluster is configured.
  // The AccessRequest will be created in the same namespace as the reconciled object (due to the overwritten namespace generator).
  car.Register(advanced.ExistingCluster("foobar", "fb", func(req reconcile.Request, _ ...any) (*commonapi.ObjectReference, error) {
    return &commonapi.ObjectReference{ // could also use advanced.IdentityReferenceGenerator instead
      Name: req.Name,
      Namespace: req.Namespace,
    }
  }).WithTokenAccess(&clustersv1alpha1.TokenConfig{...}).WithScheme(myScheme).WithNamespaceGenerator(advanced.RequestNamespaceGenerator).Build())

  return &MyController{
    car: car,
  }
}
```

> The `Register` calls could actually be chained in the form of `advanced.NewClusterAccessReconciler(...).Register(...).Register(...)`.

**Important:** Registering or unregistering clusters between calls to `Reconcile`/`ReconcileDelete` can lead to unexpected behavior and is discouraged.

#### Reconciliation

The reconciliation logic works similar to the 'simple' variant: `Reconcile` creates the required resources and must succeed before any getter calls, while `ReconcileDelete` removes the resources again and therefore has to be called when access to the cluster(s) is no longer needed during the deletion process. Both methods return a `reconcile.Result` and an error. The controller's `Reconcile` function is expected to abort if either the error is not `nil`, or the `reconcile.Result` contains a non-zero `RequeueAfter` duration.

There are four getter methods that can be called after the cluster access has been successfully reconciled:
- `Access` returns access to the specified cluster (a `client.Client` can be retrieved from the returned struct).
- `AccessRequest` returns the `AccessRequest` for the specified cluster registration.
- `ClusterRequest` returns the `ClusterRequest` for the specified cluster registration.
- `Cluster` returns the `Cluster` for the specified cluster registration.

Note that not all of these methods will always return something. For example, a registration created via `ExistingCluster(...)` references a `Cluster` directly and can therefore not return a `ClusterRequest`. `Access` and `AccessRequest` will only work if either token-based access or OIDC-based access has been configured during the registration, otherwise there won't be any `AccessRequest`. Any method which cannot return the expected value due to the resource not being configured will simply return `nil` instead, without an error. The error is only returned if something goes wrong during retrieval of the resource.

The ClusterAccess Reconciler remembers requests that are in deletion and won't create new resources for them. This means that `Reconcile` can safely be called at the beginning of a reconciliation that is going to delete resources and `ReconcileDelete` at the end of it, without the former one recreating resources the latter one has already removed.
A request is considered to be 'in deletion' when `ReconcileDelete` is called for it and it stops being 'in deletion' when `ReconcileDelete` returns with a `RequeueAfter` value of zero and no error.

#### Additional Data

While probably not required for most cases, there might be some situations in which the generation of resources requires more information than just the `reconcile.Request`, for example if the controller fetches some kind of configuration that specifies the required access permissions. The ClusterAccess library enables this by allowing arbitrary arguments to be passed into some methods: `Reconcile`, `ReconcileDelete`, as well as the four getter methods `Access`, `AccessRequest`, `ClusterRequest`, and `Cluster` take any amount of optional arguments. Additional arguments that are passed into any of these methods will be passed to the generator functions (which have been passed into `WithTokenAccessGenerator`, `WithOIDCAccessGenerator`, and `WithNamespaceGenerator` during creation of the `ClusterRegistration`), which can use the additional information for generating the namespace or the spec for `AccessRequest` or `ClusterRequest`.

**Important:** To ensure consistent behavior, different calls of `Reconcile`/`ReconcileDelete` for the same request must always be called with the same additional arguments and any call to one of the getter methods for this request must also be given the same additional arguments.

#### Updating AccessRequests

In most scenarios, the permissions that are required for a specific cluster are static and don't need to be updated dynamically. There are some cases where this is required, though. To achieve this, an `Update` method is available at the `ClusterReconciler`. It can be passed in one (or more, but that is rarely useful) updates to apply to an existing registration. This will cause the corresponding `AccessRequest` to be modified during the next `Reconcile` of the reconciler.

```go
err := rec.Update("foobar", advanced.UpdateTokenAccess(myNewTokenConfig))
```

Note that this mechanism can also cause the `AccessRequest` to be deleted, if all access variants (token/oidc) are set to `nil`.

#### Testing Environments

The resources created by the ClusterAccess Reconciler need the open control plane scheduler for `ClusterRequest`s. They also need a ClusterProvider for `Cluster`s and `AccessRequest`s. These controllers might be unavailable in unit tests. The reconciler's extension hooks let tests replace their actions without a separate code path.

On the `ClusterAccessReconciler`, the `WithFakingCallback` method can be used to register callback functions that are executed at specific points during the reconciler's `Reconciler`/`ReconcileDelete` method, depending on the specified `key`.

The available keys and the corresponding points of execution depend on the implementation of the `ClusterAccessReconciler` interface. The implementation provided in the package recognizes the following keys:
- `WaitingForClusterRequestReadiness`: The function is executed during `Reconcile`, when a non-zero `RequeueAfter` value is returned because the logic waits for a `ClusterRequest` to become `Granted`.
- `WaitingForAccessRequestReadiness`: The function is executed during `Reconcile`, when a non-zero `RequeueAfter` value is returned because the logic waits for an `AccessRequest` to become `Granted`.
- `WaitingForClusterRequestDeletion`: The function is executed during `ReconcileDelete`, when a non-zero `RequeueAfter` value is returned because the logic waits for a `ClusterRequest` to get its finalizers removed.
- `WaitingForAccessRequestDeletion`: The function is executed during `ReconcileDelete`, when a non-zero `RequeueAfter` value is returned because the logic waits for an `AccessRequest` to get its finalizers removed.

For all of these keys, the package offers constants that are prefixed with `FakingCallback_`.

While the signature of a callback function is always the same, any argument except for `ctx`, `platformClusterClient`, and `key` may be nil if not known at the point of execution.

In addition to the callbacks, the ClusterAccess Reconciler also takes a `FakeClientGenerator` via its `WithFakeClientGenerator` method. If set to something other than `nil`, the reconciler's `Access` method will pass the raw kubeconfig bytes retrieved from the `AccessRequest`'s secret into this function, instead of creating a regular client from it.

The combination of `WithFakingCallback` and `WithFakeClientGenerator` can enable unit tests for a controller which uses the advanced ClusterAccess library that do not require any test-specific logic in the controller's logic itself.

##### Convenience Implementations

Because most controllers that use the faking callback feature will probably require a very similar logic for the aforementioned callback keys, the package provides a convenience implementation for each key:
- `FakeClusterRequestReadiness` generates a callback function for the `WaitingForClusterRequestReadiness` key. It creates a `Cluster` next to the `ClusterRequest`, sets the reference to it in the request's `status` and sets the request to `Granted`.
  - This mocks cluster scheduler behavior.
- `FakeAccessRequestReadiness` generates a callback function for the `WaitingForAccessRequestReadiness` key. It creates a `Secret` containing a `kubeconfig` key, references the secret in the request's status and sets the `AccessRequest` to `Granted`.
  - This mocks ClusterProvider behavior.
  - If name and namespace of the `Cluster` the `AccessRequest` is for can be identified (which should usually be the case), the fake kubeconfig's content will be `fake:cluster:<cluster-namespace>/<cluster-name>`. Otherwise, it will be `fake:request:<request-namespace>/<request-name>`.
    - This information can be used by a `FakeClientGenerator` to return a fitting fake client implementation.
- `FakeClusterRequestDeletion` generates a callback function for the `WaitingForClusterRequestDeletion` key. Depending on its arguments, the generated function can remove specific or all finalizers on `Cluster` and/or `ClusterRequest`, and potentially also delete the `Cluster` resource.
  - This mocks cluster scheduler behavior.
- `FakeAccessRequestDeletion` generates a callback function for the `WaitingForAccessRequestDeletion` key. It deletes the `Secret`, potentially removing the specified finalizers from it before, and then removes the configured finalizers from the `AccessRequest`.
  - This mocks ClusterProvider behavior.
