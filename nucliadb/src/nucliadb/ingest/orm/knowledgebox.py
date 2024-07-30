# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import AsyncGenerator, Optional, Sequence, cast
from uuid import uuid4

from grpc import StatusCode
from grpc.aio import AioRpcError

from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import ShardNotFound
from nucliadb.common.cluster.manager import get_index_node
from nucliadb.common.cluster.utils import get_shard_manager

# XXX: this keys shouldn't be exposed outside datamanagers
from nucliadb.common.datamanagers.resources import (
    KB_RESOURCE_SLUG,
    KB_RESOURCE_SLUG_BASE,
)
from nucliadb.common.external_index_providers.exceptions import ExternalIndexCreationError
from nucliadb.common.external_index_providers.pinecone import PineconeIndexManager
from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.orm.exceptions import (
    KnowledgeBoxConflict,
    KnowledgeBoxCreationError,
    VectorSetConflict,
)
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.orm.utils import choose_matryoshka_dimension, compute_paragraph_key
from nucliadb.migrator.utils import get_latest_version
from nucliadb_protos import knowledgebox_pb2, nodewriter_pb2, utils_pb2, writer_pb2
from nucliadb_protos.knowledgebox_pb2 import (
    CreateExternalIndexProviderMetadata,
    ExternalIndexProviderType,
    KnowledgeBoxConfig,
    SemanticModelMetadata,
    StoredExternalIndexProviderMetadata,
)
from nucliadb_protos.knowledgebox_pb2 import PineconeServerlessCloud as PBPineconeServerlessCloud
from nucliadb_protos.resources_pb2 import Basic
from nucliadb_protos.utils_pb2 import ReleaseChannel, VectorSimilarity
from nucliadb_utils import const
from nucliadb_utils.aiopynecone.exceptions import PineconeAPIError
from nucliadb_utils.settings import running_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import (
    get_audit,
    get_endecryptor,
    get_pinecone,
    get_storage,
    has_feature,
)

# XXX Eventually all these keys should be moved to datamanagers.kb
KB_RESOURCE = "/kbs/{kbid}/r/{uuid}"

KB_KEYS = "/kbs/{kbid}/"

KB_TO_DELETE_BASE = "/kbtodelete/"
KB_TO_DELETE_STORAGE_BASE = "/storagetodelete/"

KB_TO_DELETE = f"{KB_TO_DELETE_BASE}{{kbid}}"
KB_TO_DELETE_STORAGE = f"{KB_TO_DELETE_STORAGE_BASE}{{kbid}}"

KB_VECTORSET_TO_DELETE_BASE = "/vectorsettodelete"
KB_VECTORSET_TO_DELETE = f"{KB_VECTORSET_TO_DELETE_BASE}/{{kbid}}/{{vectorset}}"


@dataclass
class VectorsetExternalIndex:
    vectorset_id: str
    dimension: int
    similarity: VectorSimilarity.ValueType


class KnowledgeBox:
    def __init__(self, txn: Transaction, storage: Storage, kbid: str):
        self.txn = txn
        self.storage = storage
        self.kbid = kbid
        self._config: Optional[KnowledgeBoxConfig] = None

    @staticmethod
    def new_unique_kbid() -> str:
        return str(uuid4())

    @classmethod
    @processor_observer.wrap({"type": "create_kb"})
    async def create(
        cls,
        driver: Driver,
        *,
        kbid: str,
        slug: str,
        title: str = "",
        description: str = "",
        semantic_model: Optional[SemanticModelMetadata] = None,
        semantic_models: Optional[dict[str, SemanticModelMetadata]] = None,
        release_channel: Optional[ReleaseChannel.ValueType] = ReleaseChannel.STABLE,
        external_index_provider: CreateExternalIndexProviderMetadata = CreateExternalIndexProviderMetadata(),
    ) -> tuple[str, str]:
        """Creates a new knowledge box and return its id and slug."""

        if not kbid:
            raise KnowledgeBoxCreationError("A kbid must be provided to create a new KB")
        if not slug:
            raise KnowledgeBoxCreationError("A slug must be provided to create a new KB")

        if semantic_model is None:
            if semantic_models is None or len(semantic_models) == 0:
                raise KnowledgeBoxCreationError(
                    "KB must only define semantic_model or semantic_models, not both"
                )
        else:
            if semantic_models is not None and len(semantic_models) > 0:
                raise KnowledgeBoxCreationError(
                    "KB must only define semantic_model or semantic_models, not both"
                )

        release_channel = cast(ReleaseChannel.ValueType, release_channel_for_kb(slug, release_channel))

        rollback_ops = []

        try:
            async with driver.transaction() as txn:
                exists = await datamanagers.kb.get_kb_uuid(
                    txn, slug=slug
                ) or await datamanagers.kb.exists_kb(txn, kbid=kbid)
                if exists:
                    raise KnowledgeBoxConflict()

                # Create in maindb
                await datamanagers.kb.set_kbid_for_slug(txn, slug=slug, kbid=kbid)

                # all KBs have the vectorset key initialized, although (for
                # now), not every KB will store vectorsets there
                await datamanagers.vectorsets.initialize(txn, kbid=kbid)

                kb_shards = writer_pb2.Shards()
                kb_shards.kbid = kbid
                # B/c with Shards.actual
                kb_shards.actual = -1
                kb_shards.release_channel = release_channel
                if semantic_model is not None:
                    # bw/c we keep populating the deprecated fields for KBs with
                    # only one vectorset

                    # if this KB uses a matryoshka model, we can choose a different
                    # dimension
                    if len(semantic_model.matryoshka_dimensions) > 0:
                        semantic_model.vector_dimension = choose_matryoshka_dimension(
                            semantic_model.matryoshka_dimensions  # type: ignore
                        )
                    kb_shards.similarity = semantic_model.similarity_function
                    kb_shards.model.CopyFrom(semantic_model)
                    stored_external_index_provider = await cls._maybe_create_external_indexes(
                        kbid,
                        request=external_index_provider,
                        indexes=[
                            VectorsetExternalIndex(
                                vectorset_id="default",
                                dimension=semantic_model.vector_dimension,
                                similarity=semantic_model.similarity_function,
                            )
                        ],
                    )
                    rollback_ops.append(
                        partial(cls._maybe_delete_external_indexes, kbid, stored_external_index_provider)
                    )
                else:
                    vs_external_indexes = []
                    for vectorset_id, semantic_model in semantic_models.items():  # type: ignore
                        # if this KB uses a matryoshka model, we can choose a different
                        # dimension
                        if len(semantic_model.matryoshka_dimensions) > 0:
                            dimension = choose_matryoshka_dimension(semantic_model.matryoshka_dimensions)
                        else:
                            dimension = semantic_model.vector_dimension
                        vs_external_indexes.append(
                            VectorsetExternalIndex(
                                vectorset_id=vectorset_id,
                                dimension=dimension,
                                similarity=semantic_model.similarity_function,
                            )
                        )
                        vectorset_config = knowledgebox_pb2.VectorSetConfig(
                            vectorset_id=vectorset_id,
                            vectorset_index_config=nodewriter_pb2.VectorIndexConfig(
                                similarity=semantic_model.similarity_function,
                                # XXX: hardcoded value
                                vector_type=nodewriter_pb2.VectorType.DENSE_F32,
                                normalize_vectors=len(semantic_model.matryoshka_dimensions) > 0,
                                vector_dimension=dimension,
                            ),
                            matryoshka_dimensions=semantic_model.matryoshka_dimensions,
                        )
                        await datamanagers.vectorsets.set(txn, kbid=kbid, config=vectorset_config)

                    stored_external_index_provider = await cls._maybe_create_external_indexes(
                        kbid, request=external_index_provider, indexes=vs_external_indexes
                    )
                    rollback_ops.append(
                        partial(
                            cls._maybe_delete_external_indexes,
                            kbid,
                            stored_external_index_provider,
                            vs_external_indexes,
                        )
                    )

                config = KnowledgeBoxConfig(
                    title=title,
                    description=description,
                    slug=slug,
                    migration_version=get_latest_version(),
                    external_index_provider=stored_external_index_provider,
                )
                await datamanagers.kb.set_config(txn, kbid=kbid, config=config)
                await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=kb_shards)

                # shard creation will alter this value on maindb, make sure nobody
                # uses this variable anymore
                del kb_shards

                # Create in storage

                storage = await get_storage(service_name=SERVICE_NAME)

                created = await storage.create_kb(kbid)
                rollback_ops.append(partial(storage.delete_kb, kbid))
                if not created:
                    logger.error(f"KB {kbid} could not be created")
                    raise KnowledgeBoxCreationError(
                        f"KB blob storage could not be created (slug={slug})"
                    )

                # Create shards in index nodes

                shard_manager = get_shard_manager()
                # XXX creating a shard is a slow IO operation that requires a write
                # txn to be open!
                await shard_manager.create_shard_by_kbid(txn, kbid)
                # shards don't need a rollback as they will be eventually purged

                await txn.commit()

        except Exception as exc:
            # rollback all changes on the db and raise the exception
            for op in reversed(rollback_ops):
                try:
                    await op()
                except Exception:
                    if isinstance(op, partial):
                        name = op.func.__name__
                    else:
                        getattr(op, "__name__", "unknown?")
                    logger.exception(f"Unexpected error rolling back {name}. Keep rolling back")
            raise exc

        return (kbid, slug)

    @classmethod
    async def update(
        cls,
        txn: Transaction,
        uuid: str,
        slug: Optional[str] = None,
        config: Optional[KnowledgeBoxConfig] = None,
    ) -> str:
        exist = await datamanagers.kb.get_config(txn, kbid=uuid, for_update=True)
        if not exist:
            raise datamanagers.exceptions.KnowledgeBoxNotFound()

        if slug:
            await txn.delete(datamanagers.kb.KB_SLUGS.format(slug=exist.slug))
            await txn.set(
                datamanagers.kb.KB_SLUGS.format(slug=slug),
                uuid.encode(),
            )
            if config:
                config.slug = slug
            else:
                exist.slug = slug

        if config and exist != config:
            exist.MergeFrom(config)

        await datamanagers.kb.set_config(txn, kbid=uuid, config=exist)

        return uuid

    @classmethod
    async def delete(cls, driver: Driver, kbid: str):
        async with driver.transaction() as txn:
            exists = await datamanagers.kb.exists_kb(txn, kbid=kbid)
            if not exists:
                return

            # Delete main anchor
            kb_config = await datamanagers.kb.get_config(txn, kbid=kbid)
            if kb_config is not None:
                slug = kb_config.slug
                await datamanagers.kb.delete_kb_slug(txn, slug=slug)

            await datamanagers.kb.delete_config(txn, kbid=kbid)

            # Mark KB to purge. This will eventually delete all KB keys, storage
            # and index data
            when = datetime.now().isoformat()
            await txn.set(KB_TO_DELETE.format(kbid=kbid), when.encode())

            await txn.commit()

        if kb_config is not None:
            await cls._maybe_delete_external_indexes(kbid, kb_config.external_index_provider)

        audit = get_audit()
        if audit is not None:
            audit.delete_kb(kbid=kbid)

        return kbid

    @classmethod
    async def purge(cls, driver: Driver, kbid: str):
        """
        Deletes all kb parts

        It takes care of signaling the nodes related to this kb that they
        need to delete the kb shards and also deletes the related storage
        buckets.

        As non-empty buckets cannot be deleted, they are scheduled to be
        deleted instead. Actually, this empties the bucket asynchronouysly
        but it doesn't delete it. To do it, we save a marker using the
        KB_TO_DELETE_STORAGE key, so then purge cronjob will keep trying
        to delete once the emptying have been completed.
        """
        storage = await get_storage(service_name=SERVICE_NAME)
        exists = await storage.schedule_delete_kb(kbid)
        if exists is False:
            logger.error(f"{kbid} KB does not exists on Storage")

        async with driver.transaction() as txn:
            storage_to_delete = KB_TO_DELETE_STORAGE.format(kbid=kbid)
            await txn.set(storage_to_delete, b"")

            # Delete KB Shards
            shards_match = datamanagers.cluster.KB_SHARDS.format(kbid=kbid)
            payload = await txn.get(shards_match, for_update=False)

            if payload is None:
                logger.warning(f"Shards not found for kbid={kbid}")
            else:
                shards_obj = writer_pb2.Shards()
                shards_obj.ParseFromString(payload)

                for shard in shards_obj.shards:
                    # Delete the shard on nodes
                    for replica in shard.replicas:
                        node = get_index_node(replica.node)
                        if node is None:
                            logger.error(
                                f"No node {replica.node} found, let's continue. Some shards may stay orphaned",
                                extra={"kbid": kbid},
                            )
                            continue
                        try:
                            await node.delete_shard(replica.shard.id)
                            logger.debug(
                                f"Succeded deleting shard from nodeid={replica.node} at {node.address}",
                                extra={"kbid": kbid, "node_id": replica.node},
                            )
                        except AioRpcError as exc:
                            if exc.code() == StatusCode.NOT_FOUND:
                                continue
                            raise ShardNotFound(f"{exc.details()} @ {node.address}")

            await txn.commit()
        await cls.delete_all_kb_keys(driver, kbid)

    @classmethod
    async def delete_all_kb_keys(cls, driver: Driver, kbid: str, chunk_size: int = 1_000):
        prefix = KB_KEYS.format(kbid=kbid)
        while True:
            async with driver.transaction(read_only=True) as txn:
                all_keys = [key async for key in txn.keys(match=prefix, count=-1)]

            if len(all_keys) == 0:
                break

            # We commit deletions in chunks because otherwise
            # tikv complains if there is too much data to commit
            for chunk_of_keys in chunker(all_keys, chunk_size):
                async with driver.transaction() as txn:
                    for key in chunk_of_keys:
                        await txn.delete(key)
                    await txn.commit()

    async def get_resource_shard(self, shard_id: str) -> Optional[writer_pb2.ShardObject]:
        async with datamanagers.with_ro_transaction() as txn:
            pb = await datamanagers.cluster.get_kb_shards(txn, kbid=self.kbid)
            if pb is None:
                logger.warning("Shards not found for kbid", extra={"kbid": self.kbid})
                return None
        for shard in pb.shards:
            if shard.shard == shard_id:
                return shard
        return None

    async def get(self, uuid: str) -> Optional[Resource]:
        basic = await datamanagers.resources.get_basic(self.txn, kbid=self.kbid, rid=uuid)
        if basic is None:
            return None
        return Resource(
            txn=self.txn,
            storage=self.storage,
            kb=self,
            uuid=uuid,
            basic=basic,
            disable_vectors=False,
        )

    async def delete_resource(self, uuid: str):
        basic = await datamanagers.resources.get_basic(self.txn, kbid=self.kbid, rid=uuid)

        async for key in self.txn.keys(KB_RESOURCE.format(kbid=self.kbid, uuid=uuid), count=-1):
            await self.txn.delete(key)

        if basic and basic.slug:
            slug_key = KB_RESOURCE_SLUG.format(kbid=self.kbid, slug=basic.slug)
            try:
                await self.txn.delete(slug_key)
            except Exception:
                pass

        await self.storage.delete_resource(self.kbid, uuid)

    async def get_resource_uuid_by_slug(self, slug: str) -> Optional[str]:
        return await datamanagers.resources.get_resource_uuid_from_slug(
            self.txn, kbid=self.kbid, slug=slug
        )

    async def get_unique_slug(self, uuid: str, slug: str) -> str:
        key = KB_RESOURCE_SLUG.format(kbid=self.kbid, slug=slug)
        key_ok = False
        while key_ok is False:
            found = await self.txn.get(key, for_update=False)
            if found and found.decode() != uuid:
                slug += ".c"
                key = KB_RESOURCE_SLUG.format(kbid=self.kbid, slug=slug)
            else:
                key_ok = True
        return slug

    async def add_resource(self, uuid: str, slug: str, basic: Optional[Basic] = None) -> Resource:
        if basic is None:
            basic = Basic()
        if slug == "":
            slug = uuid
        slug = await self.get_unique_slug(uuid, slug)
        basic.slug = slug
        fix_paragraph_annotation_keys(uuid, basic)
        await datamanagers.resources.set_basic(self.txn, kbid=self.kbid, rid=uuid, basic=basic)
        return Resource(
            storage=self.storage,
            txn=self.txn,
            kb=self,
            uuid=uuid,
            basic=basic,
            disable_vectors=False,
        )

    async def iterate_resources(self) -> AsyncGenerator[Resource, None]:
        base = KB_RESOURCE_SLUG_BASE.format(kbid=self.kbid)
        async for key in self.txn.keys(match=base, count=-1):
            slug = key.split("/")[-1]
            uuid = await self.get_resource_uuid_by_slug(slug)
            if uuid is not None:
                yield Resource(
                    self.txn,
                    self.storage,
                    self,
                    uuid,
                    disable_vectors=False,
                )

    async def create_vectorset(self, config: knowledgebox_pb2.VectorSetConfig):
        if await datamanagers.vectorsets.exists(
            self.txn, kbid=self.kbid, vectorset_id=config.vectorset_id
        ):
            raise VectorSetConflict(f"Vectorset {config.vectorset_id} already exists")
        await datamanagers.vectorsets.set(self.txn, kbid=self.kbid, config=config)
        shard_manager = get_shard_manager()
        await shard_manager.create_vectorset(self.kbid, config)

    async def delete_vectorset(self, vectorset_id: str):
        await datamanagers.vectorsets.delete(self.txn, kbid=self.kbid, vectorset_id=vectorset_id)
        # mark vectorset for async deletion
        await self.txn.set(KB_VECTORSET_TO_DELETE.format(kbid=self.kbid, vectorset=vectorset_id), b"")
        shard_manager = get_shard_manager()
        await shard_manager.delete_vectorset(self.kbid, vectorset_id)

    @classmethod
    async def _maybe_create_external_indexes(
        cls,
        kbid: str,
        request: CreateExternalIndexProviderMetadata,
        indexes: list[VectorsetExternalIndex],
    ) -> StoredExternalIndexProviderMetadata:
        metadata = StoredExternalIndexProviderMetadata(
            type=request.type,
        )
        if request.type != ExternalIndexProviderType.PINECONE:
            # Only pinecone is supported for now
            return metadata

        api_key = request.pinecone_config.api_key
        endecryptor = get_endecryptor()
        encrypted_api_key = endecryptor.encrypt(api_key)
        metadata.pinecone_config.encrypted_api_key = encrypted_api_key
        metadata.pinecone_config.serverless_cloud = request.pinecone_config.serverless_cloud
        pinecone = get_pinecone().control_plane(api_key=api_key)
        for index in indexes:
            index_name = PineconeIndexManager.get_index_name(kbid, index.vectorset_id)
            logger.info(
                "Creating pincone index",
                extra={"kbid": kbid, "index_name": index_name, "vector_dimension": index.dimension},
            )
            try:
                index_host = await pinecone.create_index(
                    name=index_name,
                    dimension=index.dimension,
                    metric=to_pinecone_index_metric(index.similarity),
                    serverless_cloud=to_pinecone_serverless_cloud_payload(
                        request.pinecone_config.serverless_cloud
                    ),
                )
            except PineconeAPIError as exc:
                raise ExternalIndexCreationError("pinecone", exc.message) from exc
            metadata.pinecone_config.indexes[index_name].index_host = index_host
            metadata.pinecone_config.indexes[index_name].vector_dimension = index.dimension
        return metadata

    @classmethod
    async def _maybe_delete_external_indexes(
        cls,
        kbid: str,
        external_index_provider: StoredExternalIndexProviderMetadata,
        created_external_indexes: Optional[list[VectorsetExternalIndex]] = None,
    ):
        if external_index_provider.type != ExternalIndexProviderType.PINECONE:
            # Only pinecone is supported for now
            return

        encrypted_api_key = external_index_provider.pinecone_config.encrypted_api_key
        endecryptor = get_endecryptor()
        api_key = endecryptor.decrypt(encrypted_api_key)
        pinecone = get_pinecone().control_plane(api_key=api_key)

        index_names_to_delete: set[str] = set()
        index_names_to_delete.update(
            {index_name for index_name in external_index_provider.pinecone_config.indexes.keys()}
        )
        index_names_to_delete.update({ei.vectorset_id for ei in created_external_indexes or []})
        for index_name in index_names_to_delete:
            logger.info("Deleting pincone index", extra={"kbid": kbid, "index_name": index_name})
            try:
                await pinecone.delete_index(name=index_name)
            except Exception:
                logger.exception(
                    "Error deleting pinecone index", extra={"kbid": kbid, "index_name": index_name}
                )


def release_channel_for_kb(
    slug: str, release_channel: Optional[ReleaseChannel.ValueType]
) -> ReleaseChannel.ValueType:
    if running_settings.running_environment == "stage" and has_feature(
        const.Features.EXPERIMENTAL_KB, context={"slug": slug}
    ):
        release_channel = utils_pb2.ReleaseChannel.EXPERIMENTAL

    if release_channel is None:
        return utils_pb2.ReleaseChannel.STABLE

    return release_channel


def chunker(seq: Sequence, size: int):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def fix_paragraph_annotation_keys(uuid: str, basic: Basic) -> None:
    # For the case when you want to create a resource with already labeled paragraphs (on the same request),
    # the user is expected to use N_RID as uuid of the resource in the annotation key (as the uuid of the
    # resource hasn't been computed yet). This code will fix the keys so that they point to the assigned uuid.
    for ufm in basic.fieldmetadata:
        for paragraph_annotation in ufm.paragraphs:
            key = compute_paragraph_key(uuid, paragraph_annotation.key)
            paragraph_annotation.key = key


def to_pinecone_index_metric(similarity: VectorSimilarity.ValueType) -> str:
    return {
        VectorSimilarity.COSINE: "cosine",
        VectorSimilarity.DOT: "dotproduct",
    }[similarity]


def to_pinecone_serverless_cloud_payload(
    serverless: PBPineconeServerlessCloud.ValueType,
) -> dict[str, str]:
    return {
        PBPineconeServerlessCloud.AWS_EU_WEST_1: {
            "cloud": "aws",
            "region": "eu-west-1",
        },
        PBPineconeServerlessCloud.AWS_US_EAST_1: {
            "cloud": "aws",
            "region": "us-east-1",
        },
        PBPineconeServerlessCloud.AWS_US_WEST_2: {
            "cloud": "aws",
            "region": "us-west-2",
        },
        PBPineconeServerlessCloud.AZURE_EASTUS2: {
            "cloud": "azure",
            "region": "eastus2",
        },
        PBPineconeServerlessCloud.GCP_US_CENTRAL1: {
            "cloud": "gcp",
            "region": "us-central1",
        },
    }[serverless]
