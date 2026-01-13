# nidx

![Version: 99999.99999.99999](https://img.shields.io/badge/Version-99999.99999.99999-informational?style=flat-square) ![AppVersion: 99999.99999.99999](https://img.shields.io/badge/AppVersion-99999.99999.99999-informational?style=flat-square)

nidx chart

**Homepage:** <https://nuclia.com>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Nuclia | <nucliadb@nuclia.com> |  |

## Source Code

* <https://github.com/nuclia/nucliadb>

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| imagePullPolicy | string | `"IfNotPresent"` | Image pull policy |
| containerRegistry | string | `"CONTAINER_REGISTRY_TO_REPLACE"` | Container registry (e.g. docker.io/nuclia) |
| image | string | `"IMAGE_TO_REPLACE"` | Image name (without registry eg. nidx:latest) |
| env | object | `{}` |  |
| envFrom | object | `{}` |  |
| podAnnotations | object | `{}` | Global pod annotations to add to all pods |
| podLabels | object | `{"nidxMetrics":"enabled"}` | Global pod labels to add to all pods |
| maintenance | bool | `false` | Enable maintenance mode, which disables writes while keeping searchers active |
| api.replicas | int | `2` | Number of replicas for the API deployment |
| api.nodeSelector | object | `{}` | Node selector for the API pods |
| api.topologySpreadConstraints | list | `[]` | Topology spread constraints for the API pods |
| api.podAnnotations | object | `{}` | Pod annotations to add to the API pods |
| api.podLabels | object | `{}` | Pod labels to add to the API pods |
| api.serviceAccount.create | bool | `false` | Whether to create a new service account |
| api.serviceAccount.name | string | `"default"` | Name of the service account to use |
| api.serviceAccount.labels | object | `{}` | Additional service account labels |
| api.serviceAccount.annotations | object | `{}` | Additional service account annotations |
| api.affinity | object | `{}` | Affinity settings for the API pods |
| api.tolerations | list | `[]` | Tolerations for the API pods |
| api.pdb.enabled | bool | `true` | Enable or disable the PodDisruptionBudget for the API |
| api.pdb.maxUnavailable | int | `1` | Maximum number of unavailable API pods during disruptions |
| api.service.enabled | bool | `true` | Enable or disable the API service |
| api.service.type | string | `"ClusterIP"` | Service type for the API |
| api.service.labels | object | `{}` | Additional labels for the API service |
| api.service.annotations | object | `{}` | Annotations for the API service |
| indexer.podAnnotations | object | `{}` | Pod annotations to add to the indexer pods |
| indexer.podLabels | object | `{}` | Pod labels to add to the indexer pods |
| indexer.serviceAccount.create | bool | `false` | Whether to create a new service account |
| indexer.serviceAccount.name | string | `"default"` | Name of the service account to use |
| indexer.serviceAccount.labels | object | `{}` | Additional service account labels |
| indexer.serviceAccount.annotations | object | `{}` | Additional service account annotations |
| scheduler.podAnnotations | object | `{}` | Pod annotations to add to the scheduler pods |
| scheduler.podLabels | object | `{}` | Pod labels to add to the scheduler pods |
| scheduler.serviceAccount.create | bool | `false` | Whether to create a new service account |
| scheduler.serviceAccount.name | string | `"default"` | Name of the service account to use |
| scheduler.serviceAccount.labels | object | `{}` | Additional service account labels |
| scheduler.serviceAccount.annotations | object | `{}` | Additional service account annotations |
| worker.podAnnotations | object | `{}` | Pod annotations to add to the worker pods |
| worker.podLabels | object | `{}` | Pod labels to add to the worker pods |
| worker.serviceAccount.create | bool | `false` | Whether to create a new service account |
| worker.serviceAccount.name | string | `"default"` | Name of the service account to use |
| worker.serviceAccount.labels | object | `{}` | Additional service account labels |
| worker.serviceAccount.annotations | object | `{}` | Additional service account annotations |
| searcher.replicas | int | `2` | Number of replicas for the searcher deployment |
| searcher.nodeSelector | object | `{}` | Node selector for the searcher pods |
| searcher.podAnnotations | object | `{}` | Pod annotations to add to the searcher pods |
| searcher.podLabels | object | `{}` | Pod labels to add to the searcher pods |
| searcher.topologySpreadConstraints | list | `[]` | Topology spread constraints for the searcher pods |
| searcher.serviceAccount.create | bool | `false` | Whether to create a new service account |
| searcher.serviceAccount.name | string | `"default"` | Name of the service account to use |
| searcher.serviceAccount.labels | object | `{}` | Additional service account labels |
| searcher.serviceAccount.annotations | object | `{}` | Additional service account annotations |
| searcher.affinity | object | `{}` | Affinity settings for the searcher pods |
| searcher.tolerations | list | `[]` | Tolerations for the searcher pods |
| searcher.pdb.enabled | bool | `true` | Enable or disable the PodDisruptionBudget for the searcher |
| searcher.pdb.maxUnavailable | int | `1` | Maximum number of unavailable searcher pods during disruptions |
| searcher.rbac.enabled | bool | `true` | Enable or disable RBAC for the searcher |
| searcher.service.enabled | bool | `true` | Enable or disable the searcher service |
| searcher.service.type | string | `"ClusterIP"` | Service type for the searcher |
| searcher.service.labels | object | `{}` | Additional labels for the searcher service |
| searcher.service.annotations | object | `{}` | Annotations for the searcher service |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
