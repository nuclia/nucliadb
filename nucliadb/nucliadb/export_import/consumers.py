import asyncio

import nats
from nats.aio.client import Msg
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import import logger
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.exceptions import MetadataNotFound
from nucliadb.export_import.exporter import (
    export_entities,
    export_labels,
    export_resource,
)
from nucliadb.export_import.importer import IteratorExportStream, import_kb
from nucliadb.export_import.models import (
    ExportMessage,
    ExportMetadata,
    ImportMessage,
    ImportMetadata,
    Status,
)
from nucliadb.export_import.utils import get_broker_message, iter_kb_resource_uuids
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.nats import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.storages.storage import StorageField

UNRECOVERABLE_ERRORS = (MetadataNotFound,)

EXPORT_MAX_RETRIES = 5

KB_EXPORTS = "exports/{export_id}"
KB_IMPORTS = "imports/{import_id}"


class BaseConsumer:
    stream: const.Streams

    def __init__(
        self,
        context: ApplicationContext,
        partition: int,
    ):
        self.partition = partition
        self.context = context
        self.initialized = False
        self.data_manager = ExportImportDataManager(context.kv_driver)

    async def initialize(self):
        await self.setup_nats_subscription()

    async def setup_nats_subscription(self):
        subject = self.stream.subject.format(partition=self.partition)
        group = self.stream.group.format(partition=self.partition)
        stream = self.stream.name
        await self.context.nats_manager.subscribe(
            subject=subject,
            queue=group,
            stream=stream,
            flow_control=True,
            cb=self.subscription_worker,
            subscription_lost_cb=self.setup_nats_subscription,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=1,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_ack_pending=nats_consumer_settings.nats_max_ack_pending,
                max_deliver=nats_consumer_settings.nats_max_deliver,
                ack_wait=nats_consumer_settings.nats_ack_wait,
                idle_heartbeat=nats_consumer_settings.nats_idle_heartbeat,
            ),
        )
        logger.info(f"Subscribed to {subject} on stream {stream}")

    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(reply.split(".")[5])
        logger.info(
            f"Message received: subject:{subject}, seqid: {seqid}, reply: {reply}"
        )
        async with MessageProgressUpdater(
            msg, nats_consumer_settings.nats_ack_wait * 0.66
        ):
            try:
                await self.do_work(msg)
            except UNRECOVERABLE_ERRORS as e:
                # Unrecoverable errors are not retried
                errors.capture_exception(e)
                await msg.ack()
            except Exception as e:
                # Unhandled errors are retried after a small delay
                errors.capture_exception(e)
                await asyncio.sleep(2)
                await msg.nak()
                raise e
            else:
                # Successful messages are acked
                await msg.ack()

    async def do_work(self, msg: Msg):
        raise NotImplementedError()


class ExportsConsumer(BaseConsumer):
    stream = const.Streams.EXPORTS

    async def do_work(self, msg: Msg):
        export_msg = ExportMessage.parse_raw(msg.data)
        export_id = export_msg.export_id
        kbid = export_msg.kbid
        metadata = await self.data_manager.get_export_metadata(kbid, export_id)

        if metadata.status in (Status.FINISHED, Status.ERRORED):
            logger.info(f"Export {export_id} for {kbid} is {metadata.status.value}.")
            return

        if metadata.tries >= EXPORT_MAX_RETRIES:
            logger.info(
                f"Export {export_id} for kbid {kbid} failed too many times. Skipping"
            )
            metadata.status = Status.ERRORED
            await self.set_metadata(metadata)
            return

        if metadata.status == Status.FAILED:
            logger.info(f"Export {export_id} for kbid {kbid} failed, retrying")
            metadata.status = Status.RUNNING
        else:  # metadata.status == Status.SCHEDULED:
            logger.info(f"Starting export {export_id} for kbid {kbid}")
            metadata.status = Status.RUNNING
        metadata.tries += 1
        try:
            await self.stream_export_to_blob_storage(metadata)
        except Exception as e:
            # Error during export. Mark as failed. It will be retried
            metadata.status = Status.FAILED
            raise e
        else:
            # Successful export
            metadata.status = Status.FINISHED
        finally:
            await self.set_metadata(metadata)

    async def export(self, metadata: ExportMetadata):
        async for chunk in self.export_resources(metadata):
            yield chunk

        async for chunk in export_entities(self.context, metadata.kbid):
            yield chunk

        async for chunk in export_labels(self.context, metadata.kbid):
            yield chunk

    async def export_resources(self, metadata):
        metadata.exported_resources = []
        if len(metadata.resources_to_export) == 0:
            # Starting an export from scratch
            async for rid in iter_kb_resource_uuids(self.context, metadata.kbid):
                metadata.resources_to_export.append(rid)
            await self.set_metadata(metadata)

        for rid in metadata.resources_to_export:
            bm = await get_broker_message(self.context, metadata.kbid, rid)
            if bm is None:
                logger.warning(f"Skipping resource {rid} as it was deleted")
                continue

            async for chunk in export_resource(self.context, bm):
                yield chunk

            metadata.exported_resources.append(rid)
            await self.set_metadata(metadata)

    async def set_metadata(self, metadata: ExportMetadata):
        await self.data_manager.set_export_metadata(
            metadata.kbid, metadata.id, metadata
        )

    async def stream_export_to_blob_storage(self, metadata: ExportMetadata):
        iterator = self.export(metadata)
        destination: StorageField = self.get_export_field_destination(
            metadata.kbid,
            metadata.id,
        )
        cf = self.get_export_cloud_file(metadata.kbid, metadata.id)
        await self.context.blob_storage.uploaditerator(iterator, destination, cf)

    def get_export_field_destination(self, kbid: str, export_id: str) -> StorageField:
        bucket = self.context.blob_storage.get_bucket_name(kbid)
        key = KB_EXPORTS.format(export_id=export_id)
        return self.context.blob_storage.field_klass(
            storage=self.context.blob_storage,
            bucket=bucket,
            fullkey=key,
        )

    def get_export_cloud_file(self, kbid: str, export_id: str) -> CloudFile:
        cf = CloudFile()
        cf.bucket_name = self.context.blob_storage.get_bucket_name(kbid)
        cf.content_type = "binary/octet-stream"
        cf.source = CloudFile.Source.EXPORT
        cf.filename = f"{kbid}-{export_id}.export"
        return cf


class ImportsConsumer(BaseConsumer):
    stream = const.Streams.IMPORTS

    async def do_work(self, msg: Msg):
        import_msg = ImportMessage.parse_raw(msg.data)
        import_id = import_msg.import_id
        kbid = import_msg.kbid
        metadata = await self.data_manager.get_import_metadata(kbid, import_id)

        if metadata.status in (Status.FINISHED, Status.ERRORED):
            logger.info(f"Import {import_id} for {kbid} is {metadata.status.value}.")
            return

        if metadata.tries >= EXPORT_MAX_RETRIES:
            logger.info(
                f"Import {import_id} for kbid {kbid} failed too many times. Skipping"
            )
            metadata.status = Status.ERRORED
            await self.data_manager.set_import_metadata(kbid, import_id, metadata)
            return

        if metadata.status == Status.FAILED:
            logger.info(f"Import {import_id} for kbid {kbid} failed, retrying")
            metadata.status = Status.RUNNING
        else:  # metadata.status == Status.SCHEDULED:
            logger.info(f"Starting import {import_id} for kbid {kbid}")
            metadata.status = Status.RUNNING
        metadata.tries += 1
        try:
            await self.import_from_blob_storage_stream(metadata)
        except Exception as e:
            # Error during import. Mark as failed. It will be retried
            metadata.status = Status.FAILED
            raise e
        else:
            # Successful import
            metadata.status = Status.FINISHED
        finally:
            await self.data_manager.set_import_metadata(kbid, import_id, metadata)

    async def import_from_blob_storage_stream(self, metadata: ImportMetadata):
        bucket = self.context.blob_storage.get_bucket_name(metadata.kbid)
        key = KB_IMPORTS.format(import_id=metadata.id)
        field = self.context.blob_storage.field_klass(
            storage=self.context.blob_storage,
            bucket=bucket,
            fullkey=key,
        )
        iterator = field.iter_data()
        stream = IteratorExportStream(iterator)
        await import_kb(context=self.context, kbid=metadata.kbid, stream=stream)


async def start_exports_consumer(context: ApplicationContext):
    consumer = ExportsConsumer(context, 1)
    await consumer.initialize()


async def start_imports_consumer(context: ApplicationContext):
    consumer = ImportsConsumer(context, 1)
    await consumer.initialize()
