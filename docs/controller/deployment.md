# Deployment Controllers

An open control plane has three deployment controllers. They deploy other controllers called providers:

- the **ClusterProvider Controller** is responsible for deploying cluster providers. 
- the **ServiceProvider Controller** is responsible for deploying service providers.
- the **PlatformService Controller** is responsible for deploying platform services.

The deployments are specified in kubernetes resources of the kinds `ClusterProvider`, `ServiceProvider`, and `PlatformService` respectively.

## Provider Image

To be deployable, each provider must have an image available in a container registry. The image must have an executable 
as entrypoint. It will be used twice: to initialize the provider and to run it. For the initialization, a Job is started 
with the executable, and the following arguments are supplied:

```shell
init
--environment <environment>
--verbosity <DEBUG|INFO|ERROR>
--provider-name <name of the ClusterProvider|ServiceProvider|PlatformService resource>
```

Once the initialization job has completed, a Deployment is created/updated with the same image and the following arguments:

```shell
run
--environment <environment>
--verbosity <DEBUG|INFO|ERROR>
--provider-name <name of the ClusterProvider|ServiceProvider|PlatformService resource>,
```

### Env Variables

The environment variables below are available in the pods of the init job and the deployment of providers:

- the environment variables specified in the [provider resource](#provider-resource), in field `spec.env`;
- the following predefined environment variables:
  - `POD_NAME`: the name of the pod in which the provider is running,
  - `POD_NAMESPACE`: the namespace of the pod in which the pod is running,
  - `POD_IP`: the IP address of the pod in which the provider is running,
  - `POD_SERVICE_ACCOUNT_NAME`: the name of the service account under which the pod is running.


## Provider Resource

The provider resources specify how to deploy the providers. They are of the kind `ClusterProvider`, `ServiceProvider`, or `PlatformService`. They are cluster-scoped, and have the following common structure:

```yaml
apiVersion: openmcp.cloud/v1alpha1
kind: <ClusterProvider|ServiceProvider|PlatformService>
metadata:
  name: <name>
spec:
  image: <image>
  imagePullSecrets:
    - name: <image-pull-secret-name>
  env:
    - name: <environment-variable-name>
      value: <environment-variable-value>
  verbosity: <DEBUG|INFO|ERROR>
  runReplicas: 3
  topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: topology.kubernetes.io/zone
      whenUnsatisfiable: ScheduleAnyway
    - maxSkew: 1
      topologyKey: kubernetes.io/hostname
      whenUnsatisfiable: ScheduleAnyway
```

- The `image` field specifies the container image to use for the init job and deployment of the provider. 
- The `imagePullSecrets` field specifies a list of secrets that contain the credentials to pull the image from a registry. 
- The `env` field specifies a list of name-value pairs that are passed as environment variables to the init job and deployment of the provider.
- The `verbosity` field specifies the logging level. Supported values are DEBUG, INFO, and ERROR. The default is INFO.
- The `runReplicas` field specifies the number of replicas for the deployment of the provider. The default is `1`.
  If set to greater than `1`, the `--leader-elect=true` argument is automatically added to the provider's command line to enable leader election among the replicas.
- The `topologySpreadConstraints` field specifies a list of topology spread constraints for the deployment of the provider. For more information, see the [Kubernetes documentation](https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/).
  The label selectors for the topology spread constraints are automatically set to match the pods of the deployment.
