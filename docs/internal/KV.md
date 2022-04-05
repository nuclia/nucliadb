# nucliadb Key Value Store

The prefix for storing mutable data on nucliadb are:

- `/internal/worker/{worker}` Sequence number for consumer

- `/kbs/{kbid}/`
  - `/title`
  - `/avatar`
  - `/labels` Has PB Labels
  - `/entities/{group}` Has PB Entities
  - `/shards`
    - `{sharduuid}` Has nodeID reference
  - `/r/` Scan all resources
    - `{uuid}` PB Resource
      - `/shard` Shard uuid
      - `/origin` PB Origin
      - `/metadata`
      - `/classifications`
      - `/relations`
      - `/f/l/{field}` PB Field Layout
      - `/f/t/{field}` PB Field Text
      - `/f/u/{field}` PB Field Link
      - `/f/f/{field}` PB Field File
      - `/f/c/{field}/` PB FieldConversation
        - `{page}` PB Conversation
      - `/u/{type}/{field}` PB UserFieldMetadata
  - `/s/` Scan all resources
    - `{slug}` uuid Resource

- `/kbslugs/` Scan all kbs
  - `{slug}` uuid KB

- `/kbtodelete/` Scan all kbs
  - `{kbid}`
