# KnowledgeBox Notifications

The notifications endpoint can be used to get a stream of activity for a Knowledge Box. The endpoint is at `GET /v1/kb/{kbid}/notifications` and will return a streaming response where each streamed line is a base64-encoded notification.

:note: At the time of writing, notifications are only supported on the Cloud offering of NucliaDB and the endpoint will not return notifications for On-Prem NucliaDB.

Notifications, once base64-decoded, will have the following structure:

```json
{
    "type": "<notification-type>",
    "data": {
        # custom notification type metadata
        "key": "value",
    }
}
```

In the following sections the currently supported notifications are described:

## Resource Notifications

These are notifications for operations on resources. The notification type value is `resource` and the complete model can be found at [the NucliaDB models package](https://github.com/nuclia/nucliadb/blob/main/nucliadb_models/nucliadb_models/notifications.py).

Here's an example:
```json
{
    "type": "resource",
    "data": {
        "kbid": "be585fbc-3498-4978-a2a8-85cbc45ef519",
        "resource_uuid": "6b4c6c3cec254beba03b13721ab7dd8b",
        "seqid": 542,
        "operation": "created",
        "action": "commit",
        "source": "processor",
        "processing_errors": false
    }
}
```
- `resource_uuid`: the unique ID of the resource.
- `seqid`: sequence id of the operation. This is an incremental number that allows to preserve order in resource API operations.
- `operation`: type of CRUD operation done to the resource. It can be `created` for when the resource is first created, `modified` or `deleted`.
- `action`: indicates which step in the ingestion process the operation is. Typically it will be either `commit` when the metadata has been persisted in the KV storage and `indexed` when the indexed data has been saved in the Index Node. `abort` will show up if there were ingestion errors and operation had to be aborted.
- `source`: indicates who originated the event. It can be either `writer` if the notification is originated from an HTTP request made by the user or `processor` if the notification relates to the ingestion of metadata coming out of Nuclia's processing engine.
- `processing_errors`: will only be set when `source=processor` and indicates whether Nuclia processing engine encountered errors while processing the resource.

## Examples
On a normal HTTP API operation, creating a resource with a POST request will yield the following 3 notifications:

```json
{"type": "resource", "data": {"kbid": "my-kb-id", "resource_uuid": "ruuid", "seqid": 224278, "operation": "created", "action": "commit", "source": "writer", "processing_errors": null}}
{"type": "resource", "data": {"kbid": "my-kb-id", "resource_uuid": "ruuid", "seqid": 224278, "operation": null, "action": "indexed", "source": null, "processing_errors": null}}
{"type": "resource", "data": {"kbid": "my-kb-id", "resource_uuid": "ruuid", "seqid": 224278, "operation": null, "action": "indexed", "source": null, "processing_errors": null}}
```
The first one indicates that is a HTTP operation (`"source": "writer"`) and that the data was committed to the key-value store. Doing a GET operation on the resource before this notification is received would return a 404 status code.

The following two lines notify that the resource has been indexed. There are two notifications because the indexed data is indexed twice for replication purposes. Once both indexed notifications have been sent, it means that the indexed data is ready for search -- at this point only the basic resource metadata will show up: title, summary etc.

Notice how all three notifications share the same sequence id, which allows to correlate them.

Typically, a resource creation has also triggered a process at Nuclia's processing engine. When finished, the results will be sent to NucliaDB and the following 3 notifications will show up:

```json
{"type": "resource", "data": {"kbid": "my-kb-id", "resource_uuid": "ruuid", "seqid": 224281, "operation": "created", "action": "commit", "source": "processor", "processing_errors": false}}
{"type": "resource", "data": {"kbid": "my-kb-id", "resource_uuid": "ruuid", "seqid": 224281, "operation": null, "action": "indexed", "source": null, "processing_errors": null}}
{"type": "resource", "data": {"kbid": "my-kb-id", "resource_uuid": "ruuid", "seqid": 224281, "operation": null, "action": "indexed", "source": null, "processing_errors": null}}
```

Notice how now the `seqid` has been incremented, as it is a different operation, and the `processor` source that NucliaDB has ingested all the extracted metadata resulting of Nuclia's processing.

After the last two indexed notifications, your unstructured data is fully available for search!