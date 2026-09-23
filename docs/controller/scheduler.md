# Cluster Scheduler

The open control plane operator's _Cluster Scheduler_ answers each `ClusterRequest`. It creates a `Cluster` or uses an existing one.

## Configuration

To disable the scheduler, make sure that `scheduler` is not part of the `--controllers` flag when running the binary. It is included by default.

If not disabled, the scheduler requires a config that looks like this:
```yaml
scheduler:
  scope: Cluster  # optional
  strategy: Balanced # optional

  selectors:                  # optional
    clusters:                 # optional
      matchLabels: <...>      # optional
      matchExpressions: <...> # optional
    requests:                 # optional
      matchLabels: <...>      # optional
      matchExpressions: <...> # optional

  purposeMappings:
    mcp:
      selector:                 # optional
        matchLabels: <...>      # optional
        matchExpressions: <...> # optional
      template:
        metadata:
          namespace: mcp-clusters
        spec:
          profile: gcp-workerless
          tenancy: Exclusive
    platform:
      template:
        metadata:
          labels:
            clusters.openmcp.cloud/delete-without-requests: "false"
        spec:
          profile: gcp-large
          tenancy: Shared
    onboarding:
      template:
        metadata:
          labels:
            clusters.openmcp.cloud/delete-without-requests: "false"
        spec:
          profile: gcp-workerless
          tenancy: Shared
    workload:
      tenancyCount: 20
      template:
        metadata:
          namespace: workload-clusters
        spec:
          profile: gcp-small
          tenancy: Shared
```

The following fields can be specified inside the `scheduler` field:
- `scope` _(optional, defaults to `Cluster`)_
  - Valid values: `Namespaced`, `Cluster`
  - Determines whether the scheduler takes `Cluster` resources in all namespaces into accounts or only in a specific one.
    - In `Namespaced` mode, only `Cluster` resources from a single namespace are taken into account when checking for existing clusters to schedule requests to. If the cluster template that corresponds to the purpose specified in the request has a `metadata.namespace` set, this namespace is used to check for `Cluster` resources and also to create new ones. If not, the namespace of the `ClusterRequest` resource is used instead.
    - In `Cluster` mode, the scheduler takes all clusters into account when trying to find existing clusters that can be reused. New clusters are still created in the namespace specified in the cluster template, or in the request's namespace, if the former one is not set.
- `strategy` _(optional, defaults to `Balanced`)_
  - Valid values: `Balanced`, `Random`, `Simple`
  - Determines how the scheduler chooses a cluster if multiple existing ones qualify for a request.
    - With the `Balanced` strategy, the scheduler chooses the cluster with the fewest requests pointing to it. In case of a tie, the first one is chosen.
    - With the `Random` strategy, a cluster is chosen randomly.
    - With the `Simple` strategy, the first cluster in the list (should be in alphabetical order) is chosen.
- `selectors.clusters` _(optional)_
  - A label selector that restricts which `Cluster` resources are evaluated by the scheduler. Clusters that don't match the selector are treated as if they didn't exist.
    - The selector syntax is the default k8s one, as it is used in `Deployment` resources, for example.
    - Validation of the configuration will fail if any of the cluster templates from the `purposeMappings` field does not match the selector - this would otherwise result in the scheduler creating clusters that it would be unable to find again later on.
    - Note that the scheduler might run into naming conflicts with existing `Cluster` resources that don't match the selectors. See below for further information on how new clusters are named.
  - Selectors specified in `selectors.clusters` apply to all `Cluster` resources, while selectors in `purposeMappings[*].selector` are only applied for that specific purpose.
- `selectors.requests` _(optional)_
  - A label selector that restricts which `ClusterRequest` resources are reconciled by the scheduler. Requests that don't match the selector are not reconciled.
    - The selector syntax is the default k8s one, as it is used in `Deployment` resources, for example.
- `purposeMappings`
  - This is a map where each entry maps a purpose to a cluster template and some additional information. When a `ClusterRequest` is reconciled, its `spec.purpose` is looked up in this map and the result determines how existing clusters are evaluated and how new ones are created.
  - The structure for a mapping is explained below.

Each value of a purpose mapping takes the following fields:
- `template`
  - A `Cluster` template, consisting of `metadata` (optional) and `spec`. This is used when new clusters are created, but it is also partly evaluated when checking for existing ones.
    - `metadata.name` and `metadata.generateName` can be used to influence how newly created clusters are named. See below for further explanation on cluster naming.
    - `metadata.namespace` is the namespace in which newly created clusters for this purpose are created. If empty, the request's namespace is used instead. If the scheduler runs in `Namespaced` mode, this is also the only namespace that is evaluated when checking for existing clusters (again falling back to the request's namespace, if not specified). In `Cluster` mode, existing clusters are checked across all namespaces.
    - `metadata.labels` and `metadata.annotations` are simply passed to newly created clusters. If label selectors for clusters are specified, `metadata.labels` has to satisfy the selectors, otherwise the validation will fail.
    - `spec.profile` and `spec.tenancy` have to be set. The latter value determines whether the cluster is for the creating request exlusively (`Exclusive`) or whether other requests are allowed to point to the same cluster (`Shared`).
    - If `spec.purposes` does not contain the purpose this template is mapped to, it will be added during creation of the `Cluster`.
- `tenancyCount` _(optional, defaults to `0`)_
  - This value specifies how many requests may point to a cluster with this purpose.
    - If `template.spec.tenancy` is `Exclusive`, this value has to be `0` and does not have any effect.
    - If `template.spec.tenancy` is `Shared`, this value must be equal to or greater than `0`.
      - If greater than `0`, this is the amount of requests that may point to the same cluster.
        - A value of `1` behaves similar to an exclusive cluster, but the cluster is marked as shared and other requests may refer to it at a later point, if the value is increased or the scheduler logic is changed.
      - If `0`, the cluster is shared with an unlimited amount of requests that may refer to it. This basically means that there will ever be only one cluster for this purpose and all requests with this purpose will refer to this one cluster.
        - Note that 'one cluster' means 'within the boundaries specified by namespace and label selectors'. If the scheduler runs in `Namespaced` mode and the template does not specify a namespace, for example, one cluster per namespace will be created, not one in total.
- `selector` _(optional)_
  - A label selector that is used to filter `Cluster` resources when checking for existing clusters for this purpose. This is merged with any selectors from `selectors.clusters`.

## Names and Namespaces of Clusters created by the Scheduler

If the scheduler needs to create a new `Cluster` resource, because none of the existing ones fits the criteria for a `ClusterRequest`, it has to choose name and namespace for the `Cluster` resource. This is done according to the following logic:

#### Namespace

If the cluster template from the configuration for the requested purpose has `metadata.namespace` set, that is used as namespace. Otherwise, the cluster is created in the same namespace as the `ClusterRequest` that caused its creation.

#### Name

For clusters with `Exclusive` tenancy, or for `Shared` ones with a limited tenancy count, the scheduler uses `metadata.generateName` from the cluster template or defaults it to `<purpose>-`, if not set.

For clusters with unlimited tenancy count, `metadata.generateName` takes precedence, if specified in the template, with `metadata.name` being evaluated second. If neither is specified, `<purpose>` is used as `metadata.name` (as there should be only one instance of this cluster in this namespace due to the unlimited tenancy count, there is no need to add a randomized suffix to the name).

## Deletion of Clusters

By default, the scheduler marks every `Cluster` that it has created itself with a `clusters.openmcp.cloud/delete-without-requests: "true"` label. When a `ClusterRequest` is deleted, the scheduler removes the request's finalizers from all clusters and if it was the last request finalizer on that cluster and the cluster has the aforementioned label, the scheduler will delete the cluster.

To prevent the scheduler from deleting a cluster that was created by it after the last request finalizer has been removed from the `Cluster` resource, add the label with any value except `"true"` to the cluster's template in the scheduler configuration.
