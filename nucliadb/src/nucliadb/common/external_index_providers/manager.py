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
import abc
import logging
from typing import Optional

import async_lru

from nucliadb.common import datamanagers
from nucliadb.common.external_index_providers.settings import settings
from nucliadb_protos.knowledgebox_pb2 import (
    ExternalIndexProviderType,
    StoredExternalIndexProviderMetadata,
)
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_utils.aiopynecone.models import Vector as PineconeVector
from nucliadb_utils.utilities import get_endecryptor, get_pinecone

logger = logging.getLogger(__name__)


class ExternalIndexManager(abc.ABC, metaclass=abc.ABCMeta):
    type: str

    def __init__(self, kbid: str):
        self.kbid = kbid

    async def delete_resource(self, resource_uuid: str) -> None:
        logger.info(
            "Deleting resource to external index",
            extra={
                "kbid": self.kbid,
                "resource_uuid": resource_uuid,
                "external_index_provider": self.type,
            },
        )
        await self._delete_resource(resource_uuid)

    async def index_resource(self, resource_uuid: str, resource_data: Resource) -> None:
        logger.info(
            "Indexing resource to external index",
            extra={
                "kbid": self.kbid,
                "resource_uuid": resource_uuid,
                "external_index_provider": self.type,
            },
        )
        await self._index_resource(resource_uuid, resource_data)

    @abc.abstractmethod
    async def _delete_resource(self, resource_uuid: str) -> None: ...

    @abc.abstractmethod
    async def _index_resource(self, resource_uuid: str, resource_data: Resource) -> None: ...


class NoopIndexManager(ExternalIndexManager):
    """
    A no-op external index manager that does nothing.
    This is used for knowledge boxes that do not have an external index.
    """

    type = "noop"

    def __init__(self):
        pass

    async def delete_resource(self, resource_uuid: str) -> None:
        pass

    async def index_resource(self, resource_uuid: str, resource_data: Resource) -> None:
        pass

    async def _delete_resource(self, resource_uuid: str) -> None:
        pass

    async def _index_resource(self, resource_uuid: str, resource_data: Resource) -> None:
        pass


class PineconeIndexManager(ExternalIndexManager):
    type = "pinecone"

    def __init__(
        self,
        kbid: str,
        api_key: str,
        index_host: str,
        upsert_parallelism: int = 3,
        delete_parallelism: int = 2,
        upsert_timeout: int = 10,
        delete_timeout: int = 10,
    ):
        super().__init__(kbid=kbid)
        self.api_key = api_key
        self.index_host = index_host
        pinecone = get_pinecone()
        self.control_plane = pinecone.control_plane(api_key=self.api_key)
        self.data_plane = pinecone.data_plane(api_key=self.api_key, index_host=self.index_host)
        self.upsert_parallelism = upsert_parallelism
        self.delete_parallelism = delete_parallelism
        self.upsert_timeout = upsert_timeout
        self.delete_timeout = delete_timeout

    async def _delete_resource(self, resource_uuid: str) -> None:
        await self.data_plane.delete_by_id_prefix(
            id_prefix=resource_uuid,
            max_parallel_batches=self.delete_parallelism,
            batch_timeout=self.delete_timeout,
        )

    async def _index_resource(self, resource_uuid: str, index_data: Resource) -> None:
        # First off, delete any previously existing vectors with the same field prefixes.
        field_prefixes_to_delete = set()
        for to_delete in list(index_data.sentences_to_delete) + list(index_data.paragraphs_to_delete):
            try:
                resource_uuid, field_type, field_id = to_delete.split("/")[:3]
                field_prefixes_to_delete.add(f"{resource_uuid}/{field_type}/{field_id}")
            except ValueError:
                continue

        # TODO: run tasks while vectors payload is being built
        for prefix in field_prefixes_to_delete:
            await self.data_plane.delete_by_id_prefix(
                id_prefix=prefix,
                max_parallel_batches=self.delete_parallelism,
                batch_timeout=self.delete_timeout,
            )

        access_groups = None
        if index_data.HasField("security"):
            access_groups = list(set(index_data.security.access_groups))

        # Iterate over paragraphs and fetch paragraph data
        resource_labels = set(index_data.labels)
        paragraphs_data = {}
        for field_id, text_info in index_data.texts.items():
            field_labels = set(text_info.labels)
            field_paragraphs = index_data.paragraphs.get(field_id)
            if field_paragraphs is None:
                continue
            for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
                pararaph_pabels = set(paragraph.labels).union(field_labels).union(resource_labels)
                paragraphs_data[paragraph_id] = {
                    "labels": list(pararaph_pabels),
                }
        # Iterate sentences now, and compute the list of vectors to upsert
        vectors: list[PineconeVector] = []
        for _, index_paragraphs in index_data.paragraphs.items():
            for index_paragraph_id, index_paragraph in index_paragraphs.paragraphs.items():
                paragraph_data = paragraphs_data.get(index_paragraph_id) or {}
                for sentence_id, vector_sentence in index_paragraph.sentences.items():
                    sentence_labels = set(index_paragraph.labels).union(
                        paragraph_data.get("labels") or set()
                    )
                    vector_metadata = {
                        "labels": list(sentence_labels),
                    }
                    if access_groups is not None:
                        vector_metadata["access_groups"] = access_groups
                    pc_vector = PineconeVector(
                        id=sentence_id,
                        values=list(vector_sentence.vector),
                        metadata=vector_metadata,
                    )
                    vectors.append(pc_vector)
        if len(vectors) == 0:
            return
        await self.data_plane.upsert_in_batches(
            vectors=vectors,
            max_parallel_batches=self.upsert_parallelism,
            batch_timeout=self.upsert_timeout,
        )


@async_lru.alru_cache(maxsize=None)
async def get_external_index_metadata(kbid: str) -> Optional[StoredExternalIndexProviderMetadata]:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.kb.get_external_index_provider_metadata(txn, kbid=kbid)


async def get_external_index_manager(kbid: str) -> ExternalIndexManager:
    metadata = await get_external_index_metadata(kbid)
    if metadata is None or metadata.type != ExternalIndexProviderType.PINECONE:
        return NoopIndexManager()
    encrypted_api_key = metadata.pinecone_config.encrypted_api_key
    endecryptor = get_endecryptor()
    api_key = endecryptor.decrypt(encrypted_api_key)
    main_index_name = f"{kbid}_default"
    main_index_host = metadata.pinecone_config.index_hosts[main_index_name]
    return PineconeIndexManager(
        kbid=kbid,
        api_key=api_key,
        index_host=main_index_host,
        upsert_parallelism=settings.pinecone_upsert_parallelism,
        delete_parallelism=settings.pinecone_delete_parallelism,
        upsert_timeout=settings.pinecone_upsert_timeout,
        delete_timeout=settings.pinecone_delete_timeout,
    )
