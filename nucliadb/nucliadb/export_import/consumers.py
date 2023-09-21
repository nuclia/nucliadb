import asyncio

import nats
from nats.aio.client import Msg
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import import logger
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.exceptions import (
    ExportNotResumableError,
    ExportResumableError,
    MetadataNotFound,
)
from nucliadb.export_import.exporter import export_resource
from nucliadb.export_import.models import ExportMessage, ExportMetadata, Status
from nucliadb.export_import.utils import get_broker_message, iter_kb_resource_uuids
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.nats import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.storages.storage import StorageField

UNRECOVERABLE_ERRORS = (
    MetadataNotFound,
    ExportNotResumableError,
)

EXPORT_MAX_RETRIES = 5

"""
TODO:
 - Figure out partitioning strategy
"""


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
        """
        TODO:
        - Figure out nats subscription settings
        """
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
    stream = const.Streams.EXPORTS.name

    async def do_work(self, msg: Msg):
        """
        TODO:
        - Start/resume the export
        - Stream the export to gcs to the specified uri
        - Update export metadata with progress
        - At the end, update export metadata with finished status
        """
        export_msg = ExportMessage.parse_raw(msg.data)
        metadata: ExportMetadata = await self.data_manager.get_export_metadata(
            export_msg.kbid, export_msg.export_id
        )
        export_id = export_msg.export_id
        kbid = export_msg.kbid

        if metadata.status in (Status.FINISHED, Status.ERRORED):
            logger.info(f"Export {export_id} for {kbid} is {metadata.status.value}.")
            return

        if metadata.tries >= EXPORT_MAX_RETRIES:
            logger.info(
                f"Export {export_id} for kbid {kbid} failed too many times. Skipping"
            )
            metadata.status = Status.ERRORED
            await self.data_manager.set_export_metadata(kbid, export_id, metadata)
            return

        if metadata.status == Status.FAILED:
            logger.info(f"Export {export_id} for kbid {kbid} failed, retrying")
            metadata.status = Status.RUNNING
        else:  # metadata.status == Status.SCHEDULED:
            logger.info(f"Starting export {export_id} for kbid {kbid}")
            metadata.status = Status.RUNNING

        metadata.tries += 1
        try:
            await self.export(metadata)
        except Exception as e:
            metadata.status = Status.FAILED
            raise ExportNotResumableError from e
        else:
            metadata.status = Status.FINISHED
        finally:
            await self.data_manager.set_export_metadata(kbid, export_id, metadata)

    async def export(self, metadata: ExportMetadata):
        if len(metadata.resources_to_export) == 0:
            # Starting an export from scratch
            metadata.exported_resources = []
            async for rid in iter_kb_resource_uuids(self.context, metadata.kbid):
                metadata.resources_to_export.append(rid)
            await self.data_manager.set_export_metadata(metadata)

        for rid in metadata.resources_to_export:
            if rid in metadata.exported_resources:
                logger.info(f"Skipping resource {rid} as it was already exported")
                continue

            bm = await get_broker_message(self.context, metadata.kbid, rid)
            if bm is None:
                logger.warning(f"Skipping resource {rid} as it was deleted")
                continue

            iterator = export_resource(self.context, metadata.kbid, bm)
            destination: StorageField = self.get_export_field_destination(
                metadata.kbid,
                metadata.id,
            )
            # TODO: Set
            cf = CloudFile()
            cf.bucket_name = "test"
            cf.uri = "uri"
            cf.content_type = "binary/octet-stream"
            cf.size = 0
            cf.source = CloudFile.Source.EXPORT
            cf.filename = "foo"
            await self.context.blob_storage.uploaditerator(iterator, destination, cf)

            metadata.exported_resources.append(rid)
            await self.data_manager.set_export_metadata(metadata)

    def get_export_field_destination(self, kbid: str, export_id: str) -> StorageField:
        bucket = self.context.blob_storage.get_bucket_name(kbid)
        # TODO use a proper key
        key = f"exports/{export_id}"
        return self.context.blob_storage.field_klass(
            storage=self.context.blob_storage,
            bucket=bucket,
            fullkey=key,
        )


class ImportsConsumer(BaseConsumer):
    stream = const.Streams.IMPORTS.name

    async def do_work(self, msg: Msg):
        """
        TODO:
        - Get kbid and import id from message
        - Get the import metadata
        - Start/resume the import
        - Download the import from gcs and import resources from it
        - Update import metadata with progress
        - At the end, update import metadata with finished status
        """
        pass


async def start_exports_consumer():
    context = ApplicationContext(service_name="exports_consumer")
    await context.initialize()
    for partition in nats_consumer_settings.nats_partitions:
        consumer = ExportsConsumer(context, partition)
        await consumer.initialize()
    return context.finalize


async def start_imports_consumer():
    context = ApplicationContext(service_name="imports_consumer")
    await context.initialize()
    for partition in nats_consumer_settings.nats_partitions:
        consumer = ImportsConsumer(context, partition)
        await consumer.initialize()
    return context.finalize
