# Migration Strategy

This is a proposal for how we manage registering and running migrations on the database.

The proposal focuses on providing an automated way to migrate data and index nodes.


## Current solution/situation

We currently do not have any cohesive strategy around migrations on NucliaDB.
It is currently managed through manually running scripts against the cluster.

Additionally, the ingest grpc service has a method `CleanAndUpgradeKnowledgeBoxIndex`
which only deletes a shard and recreates it. Reindexing data there needs to be
done manually.


### Types of data migrations

- Blob storage
- K/V Storage
- IndexNodes

## Proposed Solution

1. Build a framework for registering and running migrations on the platform.
2. Require all changes to to be b/w compatible
3. Migration are not reversible(register a new migration if you need to "undo")


### Types of migrations supported

- IndexNode shard rollovers: create new shards, index all data to new shards, swap active shards over
- custom: run custom snippets of code against every knowledge box

### Migration framework

The migration framework will only be for Python. Short term, the only type of migration strategy
supported for IndexNodes is to simply roll them over to new shards and delete the old ones.

All other migrations will target running python code against a knowledge box.

All migrations will be provided in the `migrations` folder and have a filename
that follows the structure: `[sequence]_[migration name].py`.
Where `sequence` is the order the migration should be run in with zero padding.
Example: `0001_migrate_data.py`.

The migration framework manages the framework around iterating through all the knowledge boxes,
tracking which ones finished succesfully so if there was a failure, it can be retried from
where it was left off.

#### Examples

File: `nucliadb/nucliadb/migrations/0001_migrate_data.py`:

```python

async def migrate(context) -> None:
    ...

async def migrate_kb(context, kbid: str) -> None:
    async for res in interate_resources():
        ...
```

File: `nucliadb/nucliadb/migrations/0002_rollover_shards.py`:

```python
from nucliadb.migrator.utils import rollover_shards

async def migrate(context) -> None:
    ...

async def migrate_kb(context, kbid: str) -> None:
    await rollover_shards(kbid)
```


#### Execution context

The migrations will be run in the context of:
- active database initialized
- blob storage driver initialized
- index node cluster available

#### Observability

- Provide metrics for every migration
- Alerts for failed migrations
- Errors reported to sentry
- Hard failure, migration should retry with backoff and show hard failures in k8s


#### Implementation

Running migrations will be done through a command line utility `nucliadb-run-migrations`.

Command options:
- migrate 1 particular knowledge box
- dry-run: this won't actually run any migration but find all KBs and which migrations will be run against them

This utility will be run automatically with a post release helm/argo hook.

New KBs should not have migrations run against them.

Each KB should have a "data_version" property that is exposed.

## Rollout plan

1. Build framework
2. Wire up post commit hook
3. Run simple dummy migration
4. Run shard rollover migration(we don't want first time to be in an emergency)


## Success Criteria

- Successfully able to run migrations in an automated way
- Observability on speed an errors