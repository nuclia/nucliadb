# Settings unification and management

The purpose of this proposal is to provide comprehensive guidance and application
of settings across NucliaDB components.


## Current solution/situation

The current solution is not bad but can be improved. We currently define all
python settings with `pydantic` and our rust settings with a combination of
`clap` and manual solutions for parsing env vars.

The goal here is to have an easy way to document all settings on the system
and have those settings work equally correctly from env vars and charts values.

### Kubernetes Integration:
Kubernetes integration is done through helm chart values settings wiring. All the
environment variables are wired through various chart file values that are mapped
to environment variables. This can make it difficult and confusing to know
exactly how to configure the component as the user needs to know the chart
config as well as the destination desired env var configs they influence.

### Standalone Integration:
Some settings can be used through command line arguments for the standalone server
but not all. Moreover, some of the settings have slightly different implementations
for the allowed values.

We should make the integration with standalone seemless with no divergence
between implementations.

Any non-standard properties between both implementations should be removed.
End users and documentation will need to be updated.

### Naming Conventions:
No explicit naming conventions used across use cases and there are some inconsistencies.

For example, in some places, namespacing is used but in most places, it is not used.


## Proposed Solution

The proposal is to implement naming conventions and utilize consistency across
all uses of settings with NucliaDB.

### Naming conventions


#### Namespacing

When a component needs to be configured, a namespace should be used that describes the usage
and dependency.

Good example: audit settings
Example 1 that needs fixing: chitchat `LISTEN_PORT` -> `CHITCHAT_LISTEN_PORT` and `SEEDS` -> `CHITCHAT_SEEDS`
Example 2 that needs fixing: `RUNNING_PORT`-> `HTTP_PORT` and `RUNNING_HOST` -> `HTTP_PORT`
Example 3 that needs fixing: `NUCLIADB_INGEST` -> `INGEST_HTTP_ADDR`

Settings where it makes sense to not use namespacing are ones where it makes sense
to reuse them between different components. For example, `HTTP_HOST`
for an http service or `GRPC_HOST` for a GRPC service.


### Python integration

- Continue using pydantic
- All Python component settings should be defined in a `settings.py` file at the root of the module so they are easy to discover
  (One example of this being broken is with featureflagging.py)
- All Rust component settings should likewise be defined in a `settings.rs` at the root of the module


### Reuse

Make sure to have consistency between like settings. For example, HTTP and GRPC services
should utilize the same env vars.

### Kubernetes/Helm Integration

- Do not map chart config settings -> env variables in a config map
- Map env variables directly from helm chart configs so they are completely consistent


Example values.yaml:

```yaml
env:
  HTTP_HOST: "0.0.0.0"
  HTTP_PORT: 8080
  METRICS_HTTP_HOST: "0.0.0.0"
  METRICS_HTTP_PORT: 8080
  SENTRY_URL:
  SEARCH_TIMEOUT: 10 # seconds
  JAEGER_ENABLED: true
  CHITCHAT_BINDING_PORT: 31337
  CHITCHAT_BINDING_HOST: "0.0.0.0"
...
```

The rest of the helm config values would be focused around configuring kubernetes deploys/settings/etc,
not the NucliaDB component. Everything in the `env` value would be mapped to env vars in the containers.

> IMPORTANT!
> Shared environment settings will be controlled through a base `values.yaml` file that is merged.

> IMPORTANT!
> No longer use config maps.

> IMPORTANT!
> Dependency shared charts that inject environment variables in a config map would not be used.


### Every setting should be documented

Every setting should be documented in `docs/internal/ENV_VARS.md`

## Rollout plan

In order to implement this iteratively, without downtime or failure, it should be done in phases:

Phases:
 1. Support generic env var settings in helm charts
 2. Implement new env vars that work with backward capatibility with existing variables in components.
    (Pydantic has support for this easily where 2 env vars can point to a single setting)
 3. Update to use new env var names in all deploy configs
 4. Remove old helm chart config support
 5. Remove old env var support
 6. Document new environment variables


## Success Criteria

- All configuration documented
- Seemless migration with no downtime