[![REUSE status](https://api.reuse.software/badge/github.com/openmcp-project/openmcp-operator)](https://api.reuse.software/info/github.com/openmcp-project/openmcp-operator)

# openmcp-operator

## About this project

The `openmcp-operator` manages open control planes. Its controllers support these use cases:

* Deployment Controller: The Deployment Controller is responsible to create Kubernetes deployments and manage the lifecycle for `ClusterProviders`, `ServiceProviders` and `PlatformServices` Kubernetes resources on the platform cluster.
* Cluster Scheduler: The cluster scheduler reads `ClusterRequests` and creates either new `Clusters` or reuses existing `Clusters` on the platform cluster. The `Cluster` resources are managed by a `ClusterProvider`, which is not part of the `openmcp-operator`. The scheduling behavior can be configured by a scheduler configuration.
* MCP Controller: The MCP controller is responsible for reconciling `ManagedControlPlanes` on the onboarding cluster and to create a `ClusterRequest` for a `ManagedControlPlane` on the platform cluster. The MCP controller is also handling user authentication and authorization for the `ManagedControlPlane`.

### Deployment Controller

For each type of deployable, `ClusterProvider`, `ServiceProvider` and `PlatformService`, several Kubernetes resources are being created/updated.
The following resources are created/updated:

* `Deployment`: The deployment is created/updated with the image of the deployable and the pull secrets provider. The image is called with the following arguments:
  * `run`: Tells the deployable to start the main operator process.
  * `--environment`: The logical environment the deployable is running in. This is used to filter for resources it is responsible for.
  
* `Job`: The job is created/updated with the image of the deployable and the pull secrets provider. The image is called with the following arguments:
  * `init`: Tells the deployable to start the initialization routine. This can be used to deploy Custom Resource Definitions (CRDs) or webhook configurations.
  * `--environment`: The logical environment the deployable is running in. This is used to filter for resources it is responsible for.

* `ServiceAccount`: The service account is used to access the platform cluster the deployable is running in.
* `ClusterRole`: The cluster role is used to access the resources the deployable is responsible for.
* `ClusterRoleBinding`: The cluster role binding is used to bind the service account to the cluster role.

#### ClusterProvider

To deploy a Cluster Provider, the following API is used:

```yaml
apiVersion: openmcp.cloud/v1alpha1
kind: ClusterProvider
metadata:
  name: my-cluster-provider
spec:
  image: ghcr.io/openmcp-project/images/my-cluster-provider:v0.1.0
  imagePullSecrets:
    - name: my-image-pull-secret
```

#### ServiceProvider

To deploy a Service Provider, the following API is used:

```yaml
apiVersion: openmcp.cloud/v1alpha1
kind: ServiceProvider
metadata:
  name: my-service-provider
spec:
  image: ghcr.io/openmcp-project/images/my-service-provider:v0.1.0
  imagePullSecrets:
    - name: my-image-pull-secret
```

#### PlatformService

To deploy a Platform Service, the following API is used:

```yaml
apiVersion: openmcp.cloud/v1alpha1
kind: PlatformService
metadata:
  name: my-platform-service
spec:
  image: ghcr.io/openmcp-project/images/my-platform-service:v0.1.0
  imagePullSecrets:
    - name: my-image-pull-secret
```

### Cluster Scheduler

A `Cluster` can be created by the following API:

```yaml
apiVersion: clusters.openmcp.cloud
kind: Cluster
metadata:
  name: my-cluster
  namespace: default
spec:
  profile: my-cluster-profile
  clusterConfigRef:
    apiGroup: clusters.openmcp.cloud
    Kind: MyClusterConfig
    name: my-cluster-config
  kubernetes:
    version: v1.32.0
  purposes:
    - testing
    - workload
  tenancy: Shared
```

A `ClusterRequest` can be created by the following API:

```yaml
apiVersion: clusters.openmcp.cloud
kind: ClusterRequest
metadata:
  name: my-cluster-request
  namespace: default
spec:
  purpose: workload
```

The cluster scheduler will create or re-use an already existing `Cluster` resource for the `ClusterRequest` and assign it to the `ClusterRequest`.

An `AccessRequest` can be created by the following API:

```yaml
apiVersion: clusters.openmcp.cloud
kind: AccessRequest
metadata:
  name: my-access-request
  namespace: default
spec:
  clusterRef:
    name: my-cluster
    namespace: default

  token:
    permissions:
      # Role + RoleBinding
      - namespace: default
        rules:
          - apiGroups:
              - ""
            resources:
              - "secrets"
            verbs:
              - "*"
      # ClusterRole + ClusterRoleBinding
      - rules:
          - apiGroups:
              - ""
            resources:
              - "configmaps"
            verbs:
              - "*"
    roleRefs:
      # bind to existing Role
      - kind: Role
        name: my-role
        namespace: my-namespace
      # bind to existing ClusterRole
      - kind: ClusterRole
        name: cluster-admin
```

This will result in a `ServiceAccount` on the referenced `Cluster` with the specified permissions applied.

## Requirements and Setup

### Running in cluster

The `openmcp-operator` is designed to run in a Kubernetes cluster. Run the following command to deploy the operator in a Kubernetes cluster:

```bash
kubectl create deployment openmcp-operator --image ghcr.io/openmcp-project/openmcp-operator:latest
```

## Support, Feedback, Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/openmcp-project/openmcp-operator/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).

## Security / Disclosure
If you find any bug that may be a security problem, please follow our instructions at [in our security policy](https://github.com/openmcp-project/openmcp-operator/security/policy) on how to report it. Please do not create GitHub issues for security-related doubts or problems.

## Code of Conduct

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone. By participating in this project, you agree to abide by its [Code of Conduct](https://github.com/openmcp-project/.github/blob/main/CODE_OF_CONDUCT.md) at all times.

## Licensing

Copyright OpenControlPlane contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/openmcp-project/openmcp-operator).

---

<p align="center">
  <a href="https://apeirora.eu/content/projects/">
    <img alt="BMWK-EU funding logo" src="https://apeirora.eu/assets/img/BMWK-EU.png" width="300"/>
  </a>
</p>

<p align="center">
  OpenControlPlane is part of <a href="https://apeirora.eu/content/projects/">ApeiroRA</a>, an EU Important Project of Common European Interest (IPCEI-CIS).
</p>

<p align="center">
  Copyright Linux Foundation Europe. For web site terms of use, trademark policy and other project policies please see <a href="https://linuxfoundation.eu/en/policies">https://linuxfoundation.eu/en/policies</a>.
</p>
