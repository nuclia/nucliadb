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
from typing import AsyncGenerator, AsyncIterator, Optional, Sequence, Tuple
from uuid import uuid4

from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.knowledgebox_pb2 import (
    KBConfiguration,
    KnowledgeBoxConfig,
    Labels,
    LabelSet,
    SemanticModelMetadata,
)
from nucliadb_protos.knowledgebox_pb2 import Synonyms as PBSynonyms
from nucliadb_protos.knowledgebox_pb2 import VectorSet, VectorSets
from nucliadb_protos.resources_pb2 import Basic
from nucliadb_protos.utils_pb2 import ReleaseChannel

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import ShardNotFound
from nucliadb.common.cluster.manager import get_index_node
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.datamanagers.kb import KnowledgeBoxDataManager
from nucliadb.common.datamanagers.labels import KB_LABELSET, LabelsDataManager
from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb.common.maindb.utils import get_driver
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
from nucliadb_utils.keys import KB_SHARDS, KB_UUID
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_audit, get_storage

KB_RESOURCE = "/kbs/{kbid}/r/{uuid}"

KB_KEYS = "/kbs/{kbid}/"

KB_VECTORSET = "/kbs/{kbid}/vectorsets"
KB_CONFIGURATION = "/kbs/{kbid}/configuration"
KB_RESOURCE_SHARD = "/kbs/{kbid}/r/{uuid}/shard"
KB_SLUGS_BASE = "/kbslugs/"
KB_SLUGS = KB_SLUGS_BASE + "{slug}"

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
        # b/w compatible, long term this class would change dramatically
        self.data_manager = KnowledgeBoxDataManager(get_driver())

    async def get_config(self) -> Optional[KnowledgeBoxConfig]:
        if self._config is None:
            config = await KnowledgeBoxDataManager._get_config(self.txn, self.kbid)
            if config is not None:
                self._config = config
                return config
            else:
                return None
        else:
            return self._config

    @classmethod
    async def get_kb(cls, txn: Transaction, uuid: str) -> Optional[KnowledgeBoxConfig]:
        payload = await txn.get(KB_UUID.format(kbid=uuid))
        if payload is not None:
            response = KnowledgeBoxConfig()
            response.ParseFromString(payload)
            return response
        else:
            return None

    @classmethod
    async def exist_kb(cls, txn: Transaction, uuid: str) -> bool:
        payload = await txn.get(KB_UUID.format(kbid=uuid))
        if payload is not None:
            return True
        else:
            return False

    @classmethod
    async def delete_kb(cls, txn: Transaction, slug: str = "", kbid: str = ""):
        # Mark storage to be deleted
        # Mark keys to be deleted
        if kbid == "" and slug == "":
            raise AttributeError()

        if kbid == "" and slug != "":
            kbid_bytes = await txn.get(KB_SLUGS.format(slug=slug))
            if kbid_bytes is None:
                raise KnowledgeBoxNotFound()
            kbid = kbid_bytes.decode()

        if slug == "" and kbid != "":
            kbconfig_bytes = await txn.get(KB_UUID.format(kbid=kbid))
            if kbconfig_bytes is None:
                raise KnowledgeBoxNotFound()
            pbconfig = KnowledgeBoxConfig()
            pbconfig.ParseFromString(kbconfig_bytes)
            slug = pbconfig.slug

        # Delete main anchor
        async with txn.driver.transaction() as subtxn:
            key_match = KB_SLUGS.format(slug=slug)
            await subtxn.delete(key_match)

            when = datetime.now().isoformat()
            await subtxn.set(KB_TO_DELETE.format(kbid=kbid), when.encode())
            await subtxn.commit()

        audit_util = get_audit()
        if audit_util is not None:
            await audit_util.delete_kb(kbid)
        return kbid

    @classmethod
    async def get_kb_uuid(cls, txn: Transaction, slug: str) -> Optional[str]:
        uuid = await txn.get(KB_SLUGS.format(slug=slug))
        if uuid is not None:
            return uuid.decode()
        else:
            return None

    @classmethod
    async def get_kbs(
        cls, txn: Transaction, slug: str, count: int = -1
    ) -> AsyncIterator[Tuple[str, str]]:
        async for key in txn.keys(KB_SLUGS.format(slug=slug), count=count):
            slug = key.replace(KB_SLUGS_BASE, "")
            uuid = await cls.get_kb_uuid(txn, slug)
            if uuid is None:
                logger.error(f"KB with slug ({slug}) but without uuid?")
                continue
            yield (uuid, slug)

    @classmethod
    async def create(
        cls,
        txn: Transaction,
        slug: str,
        semantic_model: SemanticModelMetadata,
        uuid: Optional[str] = None,
        config: Optional[KnowledgeBoxConfig] = None,
        release_channel: ReleaseChannel.ValueType = ReleaseChannel.STABLE,
    ) -> Tuple[str, bool]:
        failed = False
        exist = await cls.get_kb_uuid(txn, slug)
        if exist:
            raise KnowledgeBoxConflict()
        if uuid is None or uuid == "":
            uuid = str(uuid4())

        if slug == "":
            slug = uuid

        await txn.set(
            KB_SLUGS.format(
                slug=slug,
            ),
            uuid.encode(),
        )
        if config is None:
            config = KnowledgeBoxConfig()

        config.migration_version = get_latest_version()
        config.slug = slug
        await txn.set(
            KB_UUID.format(
                kbid=uuid,
            ),
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
        exist = await cls.get_kb(txn, uuid)
        if not exist:
            raise KnowledgeBoxNotFound()

        if slug:
            await txn.delete(
                KB_SLUGS.format(
                    slug=exist.slug,
                )
            )
            await txn.set(
                KB_SLUGS.format(
                    slug=slug,
                ),
                uuid.encode(),
            )
            if config:
                config.slug = slug
            else:
                exist.slug = slug

        if config and exist != config:
            exist.MergeFrom(config)

        await txn.set(
            KB_UUID.format(kbid=uuid),
            exist.SerializeToString(),
        )

        return uuid

    async def iterate_kb_nodes(self) -> AsyncIterator[Tuple[AbstractIndexNode, str]]:
        shards_obj = await self.data_manager.get_shards_object(self.kbid)

        for shard in shards_obj.shards:
            for replica in shard.replicas:
                node = get_index_node(replica.node)
                if node is not None:
                    yield node, replica.shard.id

    # Configuration
    async def get_configuration(self, response: KBConfiguration):
        configuration_key = KB_CONFIGURATION.format(kbid=self.kbid)
        payload = await self.txn.get(configuration_key)
        if payload is not None:
            response.ParseFromString(payload)

    async def del_configuration(self):
        configuration_key = KB_CONFIGURATION.format(kbid=self.kbid)
        await self.txn.delete(configuration_key)

    async def set_configuration(self, config: KBConfiguration):
        configuration_key = KB_CONFIGURATION.format(kbid=self.kbid)
        payload = await self.txn.get(configuration_key)
        kb_conf = KBConfiguration()
        if payload is not None:
            kb_conf.ParseFromString(payload)
        kb_conf.MergeFrom(config)
        payload = kb_conf.SerializeToString()
        await self.txn.set(configuration_key, payload)

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
        await LabelsDataManager.set_labelset(self.kbid, id, labelset, self.txn)

    async def get_labels(self) -> Labels:
        return await LabelsDataManager.inner_get_labels(self.kbid, self.txn)

    async def get_labelset(
        self, labelset: str, labelset_response: writer_pb2.GetLabelSetResponse
    ):
        ls = await LabelsDataManager.inner_get_labelset(self.kbid, labelset, self.txn)
        if ls is not None:
            labelset_response.labelset.CopyFrom(ls)

    async def del_labelset(self, id: str):
        labelset_key = KB_LABELSET.format(kbid=self.kbid, id=id)
        await self.txn.delete(labelset_key)

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
        txn = await driver.begin()
        storage_to_delete = KB_TO_DELETE_STORAGE.format(kbid=kbid)
        await txn.set(storage_to_delete, b"")

        # Delete KB Shards
        shards_match = KB_SHARDS.format(kbid=kbid)
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
                            f"No node {replica.node} found lets continue. Some shards may stay orphaned"
                        )
                        continue
                    try:
                        await node.delete_shard(replica.shard.id)
                        logger.debug(
                            f"Succeded deleting shard from nodeid={replica.node} at {node.address}"
                        )
                    except AioRpcError as exc:
                        if exc.code() == StatusCode.NOT_FOUND:
                            continue
                        await txn.abort()
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
        pb = await self.data_manager.get_shards_object(self.kbid)
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

    async def set_resource_shard_id(self, uuid: str, shard: str):
        await self.txn.set(
            KB_RESOURCE_SHARD.format(kbid=self.kbid, uuid=uuid), shard.encode()
        )

    async def get_resource_shard_id(self, uuid: str) -> Optional[str]:
        shard = await self.txn.get(KB_RESOURCE_SHARD.format(kbid=self.kbid, uuid=uuid))
        if shard is not None:
            return shard.decode()
        else:
            return None

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
