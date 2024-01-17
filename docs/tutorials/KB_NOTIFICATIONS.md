# KnowledgeBox Notifications

The notifications endpoint can be used to get a stream of activity for a Knowledge Box. The endpoint is at `GET /v1/kb/{kbid}/notifications` and will return a streaming response where each streamed line is a base64-encoded notification.

:information_source: At the time of writing, notifications are only supported on the Cloud offering of NucliaDB and the endpoint will not return notifications for On-Prem NucliaDB.

Notifications, once base64-decoded, will have the following structure:

```json
{
    "type": "<notification-type>",
    "data": {
        "<custom>": "<notification data>",
    }
}
```

In the following sections the currently supported notifications are described:

## Resource Notifications

There are three types of resource notifications:
  - `resource_written`: notifies about an HTTP operation being written to the database.
  - `resource_processed`: notifies about a resource being processed by nuclia and the results have been written to the database.
  - `resource_indexed`: indicates that resource metadata has been indexed and is ready to search.

the complete models for resource notifications can be found at [the NucliaDB models package](https://github.com/nuclia/nucliadb/blob/main/nucliadb_models/nucliadb_models/notifications.py).

Any NucliaDB HTTP request on a resource will typically yield 3 notifications:

```json
{"type": "resource_written", "data": {"resource_uuid": "ruuid", "seqid": 224278, "operation": "created", "error": false}}
{"type": "resource_indexed", "data": {"resource_uuid": "ruuid", "seqid": 224278}}
{"type": "resource_indexed", "data": {"resource_uuid": "ruuid", "seqid": 224278}}
```

That is, one for when the data has been written to disk and two for each index operation on different index nodes (we index twice for replication purposes).

The `seqid` is sequence id of the operation. This is an incremental number that allows to preserve order in resource API operations.

The `operation` key corresponds to the CRUD operation done to the resource. It can be `created` for when the resource is first created, `modified` or `deleted`. If the write operation was `created` or `modified`, this means that new data has been pushed to Nuclia for processing. Therefore, eventually three more notifications will pop up:

```json
{"type": "resource_processed", "data": {"resource_uuid": "ruuid", "seqid": 224281, "ingestion_succeeded": "created", "action": "commit", "source": "writer", "processing_errors": null}}
{"type": "resource_indexed", "data": {"resource_uuid": "ruuid", "seqid": 224281}}
{"type": "resource_indexed", "data": {"resource_uuid": "ruuid", "seqid": 224281}}
```

The first one indicates that the resource has been correctly processed by Nuclia and ingested by NucliaDB.

Notice how now the `seqid` has been incremented compared to the previous set of notifications.

After the last two indexed notifications, your unstructured data (extracted text, entities, classifications, etc) is fully available for search!
