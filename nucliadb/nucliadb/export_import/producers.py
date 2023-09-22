import uuid

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.models import (
    ExportMessage,
    ExportMetadata,
    ImportMessage,
    ImportMetadata,
    Status,
)
from nucliadb_utils import const


async def start_export(context: ApplicationContext, kbid: str) -> tuple[str, int]:
    # Create the export metadata
    export_id = uuid.uuid4().hex
    metadata = ExportMetadata(kbid=kbid, id=export_id, status=Status.SCHEDULED)
    dm = ExportImportDataManager(context.kv_driver)
    await dm.set_export_metadata(kbid, export_id, metadata)

    # Enqueue the export
    msg = ExportMessage(kbid=kbid, export_id=export_id)
    partition = 1
    target_subject = const.Streams.EXPORTS.subject.format(partition=partition)
    res = await context.nats_manager.js.publish(
        target_subject, msg.json().encode("utf-8")
    )

    return export_id, res.seq


async def start_import(
    context: ApplicationContext, kbid: str, import_id: str
) -> tuple[str, int]:
    # Create the metadata
    metadata = ImportMetadata(kbid=kbid, id=import_id, status=Status.SCHEDULED)
    dm = ExportImportDataManager(context.kv_driver)
    await dm.set_import_metadata(kbid, import_id, metadata)

    # Enqueue the message
    msg = ImportMessage(kbid=kbid, import_id=import_id)
    partition = 1
    target_subject = const.Streams.IMPORTS.subject.format(partition=partition)
    res = await context.nats_manager.js.publish(
        target_subject, msg.json().encode("utf-8")
    )

    return import_id, res.seq
