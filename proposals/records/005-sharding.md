# Sharding and Scaling

This proposal is attempting to synthesize all the ideas around architectural
design for scaling. Most importantly, this proposal details how to leverage
index unification with sharding and shard management strategy to scale NucliaDB
for larger datasets.

This proposal also focuses on an iterative design change for read replication.

## Current solution/situation

Currently, our IndexNode contains our shards. Each shard contains multiple
types of indexes for vector, bm25(with tantivy) and graph capabilities. The implementation
of each index type involves many managed files, threads, etc and can be
quite complex.

Readers and Writers of a shard must be on the same physical machine for a shard.
Right now, that also means a single Kubernetes Pod(part of stateful set).

While we can "rebalance" nodes to some extent, we don't have good metrics
to really know how we should rebalance. Disk size is not a good metric to
know usage of shard.

Moreover, moving a shard is currently tedious. The only implemented option for us
is to reindex a shard on a new node and cutover the reference of the node the shard is on.
Another option could be to copy all the files; however, this is also slow, error prone,
requires locks and places coupling on the ingest component and our IndexNodes.

Overview of current problems:
- IndexNode is managing multiple types of indexes seperately. High overhead.
- CPU tied to disk and difficult to move shards around
- Can not add replicas for an existing KB's shards
- Coupling with shard management and ingest


## Proposed Solution

The proposed solution is oriented around providing a unified index to simplify
how we store shard data on the IndexNode.

With a unified index implementation, it is feasible to provide more
capabilities around sharding(physical read replicas, rebalancing).


### Unified Index

By "unified index," we are referring to an index that stores all data
in a single disk format that hnsw and bm25 index types utilize.

An important aspect of this unified index design is that it also provides
segmented storage. By segmented storage, we mean that each write should
produce a new segment. That new segment will work along with other existing
segments to provide the shard's full dataset.

[more details on the implementation provided elsewhere]

#### Breaking changes

- Split from tantivy: Tantivy is not compatible with this approach.
- No full entity graph implementation built in
- No advanced query support


### Replication

Each node will have a commit log to track changes through time,
communicate changes and track replication to secondary node replicas.

- Commit log is append only file which tracks all shard segments
  and their files. This is then used for replication to secondary node replicas.
- Primary nodes are responsible for replicating data to secondary read only nodes consistently
    - 2 phase "commit". First write files everywhere. Second, "turn on" new files.
    - Having Primary nodes responsible for replication allows us to avoid needing
      to implement something like raft to coordinate read replica state
- Each node on the system will have a configured set of physical read replicas


### Index coordinator

The Index Coordinator is the component responsible for managing the
IndexNode cluster.

The Index coordinator is the only component that should "know" about
what shards a Knowledge Box has and how to correctly query or write to them.

Responsibilities:
- Know Primary and secondary health
- Balancing shards across cluster
    - Place new shards appropriately
    - Move shards when things are unbalanced
- Record keeping
    - Shard stats
    - Usage stats
- Index queue consuming, operations
  - Allow writing to multiple shards on a node at a time(right now, we only write to 1 at a time)

APIs:
- Create Shard
- Delete Shard
- Shard Operation
    - Search
    - etc


#### Ingest changes

Right now, ingest acts as our "Index Coordinator".

The logic for how to store and query Index Nodes are spread throughout multiple components.

This would be moved to the Index Coordinator.

Changes to ingest/etc:
- No longer responsible to directly connecting to primary node GPRC service to create new shards
- No longer responsible for knowing what the best node to create a new shard on


### Index Node Writer

- sync changes of commit log to configured secondary read only copies
- shard create/delete is also part of commit log

Write path diagram:
![Write Path Diagram](./images/005/write-path-ndb-coor.drawio.png)


#### Disks

Use best performing persistent disks.

Start with standard SSD persistent disks but move to extreme performance disks
if performance required.

### Index Node secondary read only replica

Secondary read only replicas are "dumb" and are mostly only responsible for serving
requests from the replicated state.

Read path diagram:
![Read Path Diagram](./images/005/read-path-ndb-coor.drawio.png)

#### Disks

To improve secondary read only replica performance, utilize local SSDs

## Alternatives


### Decouple primary and read only replica shards

Allow shard Primary and secondary read only replicas to be decoupled from one another.

This would be accomplished through complex shard coordination schemes that
allowed management of increasing particular KB shard replicas.


#### Shard state coordination

In order to manage consistency, we would need to use something like raft to
coordinate secondary read only replication state and have secondaries responsible for
their own replication.

The complexity around this is one of the reasons why this proposal focuses
on having the primary responsible for replicating data.


#### Not now

This approach, while interesting, ends up with a lot of moving parts and becomes
quite complex for little value.

The approach outlined in this document allows us an easier path to iterate toward.


## Rollout plan


1. Unified index + commit log implementation
2. Replicate commit log to configured read replicas
3. Index coordinator to coordinate indexing to primary node and query traffic to secondary read replicas


### Key Metrics

- Index consumer latency/lag
- Replication latency/lag
- CPU/Disk on primary/secondary nodes
- Disk IO performance

## Future

- Dynamically configure node read replicas
- Consider moving to Kafka for a more performant ordered message queue