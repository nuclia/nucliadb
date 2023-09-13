# Exports

NucliaDB allows exporting the contents of a KB into a portable binary file.

This can be used to replicate the contents of the KB into another KB of another environment. A typical use-case is when a user moves from the Nuclia's cloud offering to having an on-prem deployment (i.e: self-hosted NucliaDB).

## API

TODO

## Export format

An export of a KB results on a binary file where all exported items are encoded.

The format of the file is simply a sequence of the following parts:

```{item_type}{item_data_size}{item_data}```

The sequence repeats for every item exported. `item_type` is always 3 bytes and represents the type of exported item, which can be:
 - `RES`: a broker message representing a resource
 - `BIN`: a file binary of a broker message's field (e.g: file field, thumbnail, etc)
 - `ENT`: the entities of the KB 
 - `LAB`: the labels of the KB

The `item_data_size` comprises 4 bytes and indicates how much bytes needs to be read from the stream to get the item's data.

Finally, `item_data` is a binary representation of each item type:

- `RES`: a `BrokerMessage` protobuf message. See [schema here](https://github.com/nuclia/nucliadb/blob/main/nucliadb_protos/writer.proto#L37).
- `ENT`: a `EntitiesGroups` protobuf message. See [schema here](https://github.com/nuclia/nucliadb/blob/main/nucliadb_protos/knowledgebox.proto).
- `LAB`: a `Labels` protobuf message. See [schema here](https://github.com/nuclia/nucliadb/blob/main/nucliadb_protos/knowledgebox.proto).
- `BIN`: see subsection below for more details.

### File binaries

How file binaries are represented in the export stream is a bit different from the other items:

1. First, we store the `BIN` item type.
2. Then the next four bytes indicate the size of the `CloudFile` protobuffer representing the metadada of the file binary (the storage bucket name, the download uri, etc.). See [the protobuf message definition](https://github.com/nuclia/nucliadb/blob/main/nucliadb_protos/resources.proto#L9)
3. Then the `CloudFile` bytes with the mentioned metadata.
4. After that, another 4 bytes indicating the size of the actual binary file.
5. The bytes of the file.
