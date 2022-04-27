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
from contextvars import ContextVar
from typing import Dict, List, Optional

from nucliadb_protos.nodereader_pb2 import ParagraphResult

from nucliadb_ingest.maindb.driver import Transaction
from nucliadb_ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb_ingest.orm.resource import KB_REVERSE
from nucliadb_ingest.orm.resource import Resource as ResourceORM
from nucliadb_ingest.utils import get_driver
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import Resource
from nucliadb_models.serialize import (
    ExtractedDataTypeName,
    ResourceProperties,
    serialize,
)
from nucliadb_search import logger
from nucliadb_utils.utilities import get_cache, get_storage

rcache: ContextVar[Optional[Dict[str, ResourceORM]]] = ContextVar(
    "rcache", default=None
)
txn: ContextVar[Optional[Transaction]] = ContextVar("txn", default=None)


def get_resource_cache() -> Dict[str, ResourceORM]:
    value: Optional[Dict[str, ResourceORM]] = rcache.get()
    if value is None:
        value = {}
        rcache.set(value)
    return value


async def get_transaction() -> Transaction:
    transaction: Optional[Transaction] = txn.get()
    if transaction is None:
        driver = await get_driver()
        transaction = await driver.begin()
        txn.set(transaction)
    return transaction


async def abort_transaction():
    transaction: Optional[Transaction] = txn.get()
    if transaction is not None:
        await transaction.abort()


async def fetch_resources(
    resources: List[str],
    kbid: str,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
) -> Dict[str, Resource]:
    result = {}
    for resource in resources:
        serialization = await serialize(
            kbid,
            resource,
            show,
            field_type_filter=field_type_filter,
            extracted=extracted,
        )
        if serialization is not None:
            result[resource] = serialization
    return result


async def get_text_paragraph(result: ParagraphResult, kbid: str) -> str:
    resouce_cache = get_resource_cache()
    if result.uuid not in resouce_cache:
        transaction = await get_transaction()
        storage = await get_storage()
        cache = await get_cache()
        kb = KnowledgeBoxORM(transaction, storage, cache, kbid)
        orm_resource: Optional[ResourceORM] = await kb.get(result.uuid)
        if orm_resource is not None:
            resouce_cache[result.uuid] = orm_resource
    else:
        orm_resource = resouce_cache.get(result.uuid)

    if orm_resource is None:
        logger.error(f"{result.uuid} does not exist on DB")
        return ""

    _, field_type, field = result.field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if result.split not in (None, ""):
        text = extracted_text.split_text[result.split]
        splitted_text = text[result.start : result.end]
    else:
        splitted_text = extracted_text.text[result.start : result.end]
    return splitted_text


async def get_labels_paragraph(result: ParagraphResult, kbid: str) -> List[str]:
    resouce_cache = get_resource_cache()
    if result.uuid not in resouce_cache:
        transaction = await get_transaction()
        storage = await get_storage()
        cache = await get_cache()
        kb = KnowledgeBoxORM(transaction, storage, cache, kbid)
        orm_resource: Optional[ResourceORM] = await kb.get(result.uuid)
        if orm_resource is not None:
            resouce_cache[result.uuid] = orm_resource
    else:
        orm_resource = resouce_cache.get(result.uuid)

    if orm_resource is None:
        logger.error(f"{result.uuid} does not exist on DB")
        return []

    labels: List[str] = []
    basic = await orm_resource.get_basic()
    if basic is not None:
        for classification in basic.usermetadata.classifications:
            labels.append(f"{classification.labelset}/{classification.label}")

    _, field_type, field = result.field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    field_metadata = await field_obj.get_field_metadata()
    if field_metadata:
        paragraph = None
        if result.split not in (None, ""):
            metadata = field_metadata.split_metadata[result.split]
            paragraph = metadata.paragraphs[result.index]
        elif len(field_metadata.metadata.paragraphs) > result.index:
            paragraph = field_metadata.metadata.paragraphs[result.index]

        if paragraph is not None:
            for classification in paragraph.classifications:
                labels.append(f"{classification.labelset}/{classification.label}")

    return labels
