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
from datetime import datetime
from typing import AsyncGenerator, AsyncIterator, Optional, Sequence
from uuid import uuid4

from grpc import StatusCode
from grpc.aio import AioRpcError
from nucliadb_protos.knowledgebox_pb2 import (
    KnowledgeBoxConfig,
    Labels,
    LabelSet,
    SemanticModelMetadata,
)
from nucliadb_protos.knowledgebox_pb2 import Synonyms as PBSynonyms
from nucliadb_protos.knowledgebox_pb2 import VectorSet, VectorSets
from nucliadb_protos.resources_pb2 import Basic
from nucliadb_protos.utils_pb2 import ReleaseChannel

from nucliadb.common import datamanagers
from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import ShardNotFound, ShardsNotFound
from nucliadb.common.cluster.manager import get_index_node
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.orm.resource import (
    KB_RESOURCE_SLUG,
    KB_RESOURCE_SLUG_BASE,
    Resource,
)
from nucliadb.ingest.orm.synonyms import Synonyms
from nucliadb.ingest.orm.utils import compute_paragraph_key, get_basic, set_basic
from nucliadb.migrator.utils import get_latest_version
from nucliadb_protos import writer_pb2
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_audit, get_storage

# XXX Eventually all these keys should be moved to datamanagers.kb
KB_RESOURCE = "/kbs/{kbid}/r/{uuid}"

KB_KEYS = "/kbs/{kbid}/"

KB_VECTORSET = "/kbs/{kbid}/vectorsets"

KB_TO_DELETE_BASE = "/kbtodelete/"
KB_TO_DELETE_STORAGE_BASE = "/storagetodelete/"

KB_TO_DELETE = f"{KB_TO_DELETE_BASE}{{kbid}}"
KB_TO_DELETE_STORAGE = f"{KB_TO_DELETE_STORAGE_BASE}{{kbid}}"


class KnowledgeBox:
    def __init__(self, txn: Transaction, storage: Storage, kbid: str):
        self.txn = txn
        self.storage = storage
        self.kbid = kbid
        self._config: Optional[KnowledgeBoxConfig] = None
        self.synonyms = Synonyms(self.txn, self.kbid)

    async def get_config(self) -> Optional[KnowledgeBoxConfig]:
        if self._config is None:
            async with datamanagers.with_transaction() as txn:
                config = await datamanagers.kb.get_config(txn, kbid=self.kbid)
            if config is not None:
                self._config = config
                return config
            else:
                return None
        else:
            return self._config

    @classmethod
    async def delete_kb(cls, txn: Transaction, slug: str = "", kbid: str = ""):
        # Mark storage to be deleted
        # Mark keys to be deleted
        logger.info(f"Deleting KB kbid={kbid} slug={slug}")
        if not kbid and not slug:
            raise AttributeError()

        if slug and not kbid:
            kbid_bytes = await txn.get(datamanagers.kb.KB_SLUGS.format(slug=slug))
            if kbid_bytes is None:
                raise datamanagers.exceptions.KnowledgeBoxNotFound()
            kbid = kbid_bytes.decode()

        if kbid and not slug:
            kbconfig_bytes = await txn.get(datamanagers.kb.KB_UUID.format(kbid=kbid))
            if kbconfig_bytes is None:
                raise datamanagers.exceptions.KnowledgeBoxNotFound()
            pbconfig = KnowledgeBoxConfig()
            pbconfig.ParseFromString(kbconfig_bytes)
            slug = pbconfig.slug

        # Delete main anchor
        async with txn.driver.transaction() as subtxn:
            key_match = datamanagers.kb.KB_SLUGS.format(slug=slug)
            logger.info(f"Deleting KB with slug: {slug}")
            await subtxn.delete(key_match)

            when = datetime.now().isoformat()
            await subtxn.set(KB_TO_DELETE.format(kbid=kbid), when.encode())
            await subtxn.commit()

        audit_util = get_audit()
        if audit_util is not None:
            await audit_util.delete_kb(kbid)
        return kbid

    @classmethod
    async def create(
        cls,
        txn: Transaction,
        slug: str,
        semantic_model: SemanticModelMetadata,
        uuid: Optional[str] = None,
        config: Optional[KnowledgeBoxConfig] = None,
        release_channel: ReleaseChannel.ValueType = ReleaseChannel.STABLE,
    ) -> tuple[str, bool]:
        failed = False
        exist = await datamanagers.kb.get_kb_uuid(txn, slug=slug)
        if exist:
            raise KnowledgeBoxConflict()
        if uuid is None or uuid == "":
            uuid = str(uuid4())

        if slug == "":
            slug = uuid

        await txn.set(
            datamanagers.kb.KB_SLUGS.format(slug=slug),
            uuid.encode(),
        )
        if config is None:
            config = KnowledgeBoxConfig()

        config.migration_version = get_latest_version()
        config.slug = slug
        await txn.set(
            datamanagers.kb.KB_UUID.format(kbid=uuid),
            config.SerializeToString(),
        )
        # Create Storage
        storage = await get_storage(service_name=SERVICE_NAME)

        created = await storage.create_kb(uuid)
        if created is False:
            logger.error(f"{uuid} KB could not be created")
            failed = True

        if failed is False:
            shard_manager = get_shard_manager()
            try:
                await shard_manager.create_shard_by_kbid(
                    txn,
                    uuid,
                    semantic_model=semantic_model,
                    release_channel=release_channel,
                )
            except Exception as e:
                await storage.delete_kb(uuid)
                raise e

        if failed:
            await storage.delete_kb(uuid)

        return uuid, failed

    @classmethod
    async def update(
        cls,
        txn: Transaction,
        uuid: str,
        slug: Optional[str] = None,
        config: Optional[KnowledgeBoxConfig] = None,
    ) -> str:
        exist = await datamanagers.kb.get_config(txn, kbid=uuid)
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

        await txn.set(
            datamanagers.kb.KB_UUID.format(kbid=uuid),
            exist.SerializeToString(),
        )

        return uuid

    async def iterate_kb_nodes(self) -> AsyncIterator[tuple[AbstractIndexNode, str]]:
        async with datamanagers.with_transaction() as txn:
            shards_obj = await datamanagers.cluster.get_kb_shards(txn, kbid=self.kbid)
            if shards_obj is None:
                raise ShardsNotFound(self.kbid)

        for shard in shards_obj.shards:
            for replica in shard.replicas:
                node = get_index_node(replica.node)
                if node is not None:
                    yield node, replica.shard.id

    # Vectorset
    async def get_vectorsets(self, response: writer_pb2.GetVectorSetsResponse):
        vectorset_key = KB_VECTORSET.format(kbid=self.kbid)
        payload = await self.txn.get(vectorset_key)
        if payload is not None:
            response.vectorsets.ParseFromString(payload)

    async def del_vectorset(self, id: str):
        vectorset_key = KB_VECTORSET.format(kbid=self.kbid)
        payload = await self.txn.get(vectorset_key)
        vts = VectorSets()
        if payload is not None:
            vts.ParseFromString(payload)
        del vts.vectorsets[id]
        # For each Node on the KB delete the vectorset
        async for node, shard in self.iterate_kb_nodes():
            await node.del_vectorset(shard, id)
        payload = vts.SerializeToString()
        await self.txn.set(vectorset_key, payload)

    async def set_vectorset(self, id: str, vs: VectorSet):
        vectorset_key = KB_VECTORSET.format(kbid=self.kbid)
        payload = await self.txn.get(vectorset_key)
        vts = VectorSets()
        if payload is not None:
            vts.ParseFromString(payload)
        vts.vectorsets[id].CopyFrom(vs)
        # For each Node on the KB add the vectorset
        async for node, shard in self.iterate_kb_nodes():
            await node.set_vectorset(shard, id, similarity=vs.similarity)
        payload = vts.SerializeToString()
        await self.txn.set(vectorset_key, payload)

    # Labels
    async def set_labelset(self, id: str, labelset: LabelSet):
        await datamanagers.labels.set_labelset(
            self.txn, kbid=self.kbid, labelset_id=id, labelset=labelset
        )

    async def get_labels(self) -> Labels:
        return await datamanagers.labels.get_labels(self.txn, kbid=self.kbid)

    async def get_labelset(
        self, labelset: str, labelset_response: writer_pb2.GetLabelSetResponse
    ):
        ls = await datamanagers.labels.get_labelset(
            self.txn,
            kbid=self.kbid,
            labelset_id=labelset,
        )
        if ls is not None:
            labelset_response.labelset.CopyFrom(ls)

    async def del_labelset(self, id: str):
        await datamanagers.labels.delete_labelset(
            self.txn, kbid=self.kbid, labelset_id=id
        )

    async def get_synonyms(self, synonyms: PBSynonyms):
        pbsyn = await self.synonyms.get()
        if pbsyn is not None:
            synonyms.CopyFrom(pbsyn)

    async def set_synonyms(self, synonyms: PBSynonyms):
        await self.synonyms.set(synonyms)

    async def delete_synonyms(self):
        await self.synonyms.clear()

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
            payload = await txn.get(shards_match)

            if payload is None:
                logger.warning(f"Shards not found for kbid={kbid}")
            else:
                shards_obj = writer_pb2.Shards()
                shards_obj.ParseFromString(payload)  # type: ignore

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
    async def delete_all_kb_keys(
        cls, driver: Driver, kbid: str, chunk_size: int = 1_000
    ):
        prefix = KB_KEYS.format(kbid=kbid)
        while True:
            async with driver.transaction() as txn:
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

    async def get_resource_shard(
        self, shard_id: str
    ) -> Optional[writer_pb2.ShardObject]:
        async with datamanagers.with_transaction() as txn:
            pb = await datamanagers.cluster.get_kb_shards(txn, kbid=self.kbid)
            if pb is None:
                logger.warning("Shards not found for kbid", extra={"kbid": self.kbid})
                return None
        for shard in pb.shards:
            if shard.shard == shard_id:
                return shard
        return None

    async def get(self, uuid: str) -> Optional[Resource]:
        raw_basic = await get_basic(self.txn, self.kbid, uuid)
        if raw_basic:
            return Resource(
                txn=self.txn,
                storage=self.storage,
                kb=self,
                uuid=uuid,
                basic=Resource.parse_basic(raw_basic),
                disable_vectors=False,
            )
        else:
            return None

    async def delete_resource(self, uuid: str):
        raw_basic = await get_basic(self.txn, self.kbid, uuid)
        if raw_basic:
            basic = Resource.parse_basic(raw_basic)
        else:
            basic = None

        async for key in self.txn.keys(
            KB_RESOURCE.format(kbid=self.kbid, uuid=uuid), count=-1
        ):
            await self.txn.delete(key)

        if basic and basic.slug:
            slug_key = KB_RESOURCE_SLUG.format(kbid=self.kbid, slug=basic.slug)
            try:
                await self.txn.delete(slug_key)
            except Exception:
                pass

        await self.storage.delete_resource(self.kbid, uuid)

    async def get_resource_uuid_by_slug(self, slug: str) -> Optional[str]:
        uuid = await self.txn.get(KB_RESOURCE_SLUG.format(kbid=self.kbid, slug=slug))
        if uuid is not None:
            return uuid.decode()
        else:
            return None

    async def get_unique_slug(self, uuid: str, slug: str) -> str:
        key = KB_RESOURCE_SLUG.format(kbid=self.kbid, slug=slug)
        key_ok = False
        while key_ok is False:
            found = await self.txn.get(key)
            if found and found.decode() != uuid:
                slug += ".c"
                key = KB_RESOURCE_SLUG.format(kbid=self.kbid, slug=slug)
            else:
                key_ok = True
        return slug

    @classmethod
    async def resource_slug_exists(
        self, txn: Transaction, kbid: str, slug: str
    ) -> bool:
        key = KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug)
        encoded_slug: Optional[bytes] = await txn.get(key)
        return encoded_slug not in (None, b"")

    async def add_resource(
        self, uuid: str, slug: str, basic: Optional[Basic] = None
    ) -> Resource:
        if basic is None:
            basic = Basic()
        if slug == "":
            slug = uuid
        slug = await self.get_unique_slug(uuid, slug)
        basic.slug = slug
        fix_paragraph_annotation_keys(uuid, basic)
        await set_basic(self.txn, self.kbid, uuid, basic)
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
