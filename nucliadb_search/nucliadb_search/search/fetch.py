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
import re
from contextvars import ContextVar
from typing import Dict, List, Optional, Tuple

from nucliadb_protos.nodereader_pb2 import DocumentResult, ParagraphResult

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


def get_resource_cache(clear: bool = False) -> Dict[str, ResourceORM]:
    value: Optional[Dict[str, ResourceORM]] = rcache.get()
    if value is None or clear:
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


async def get_text_sentence(
    rid: str,
    field_type: str,
    field: str,
    kbid: str,
    index: int,
    start: int,
    end: int,
    split: Optional[str] = None,
) -> str:
    resouce_cache = get_resource_cache()
    if rid not in resouce_cache:
        transaction = await get_transaction()
        storage = await get_storage()
        cache = await get_cache()
        kb = KnowledgeBoxORM(transaction, storage, cache, kbid)
        orm_resource: Optional[ResourceORM] = await kb.get(rid)
        if orm_resource is not None:
            resouce_cache[rid] = orm_resource
    else:
        orm_resource = resouce_cache.get(rid)

    if orm_resource is None:
        logger.warn(f"{rid} does not exist on DB")
        return ""

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warn(
            f"{rid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return ""
    start = start - 1
    if start < 0:
        start = 0
    if split not in (None, ""):
        text = extracted_text.split_text[split]
        splitted_text = text[start:end]
    else:
        splitted_text = extracted_text.text[start:end]
    return splitted_text


async def get_labels_sentence(
    rid: str,
    field_type: str,
    field: str,
    kbid: str,
    index: int,
    start: int,
    end: int,
    split: Optional[str] = None,
) -> List[str]:
    resouce_cache = get_resource_cache()
    if rid not in resouce_cache:
        transaction = await get_transaction()
        storage = await get_storage()
        cache = await get_cache()
        kb = KnowledgeBoxORM(transaction, storage, cache, kbid)
        orm_resource: Optional[ResourceORM] = await kb.get(rid)
        if orm_resource is not None:
            resouce_cache[rid] = orm_resource
    else:
        orm_resource = resouce_cache.get(rid)

    if orm_resource is None:
        logger.warn(f"{rid} does not exist on DB")
        return []

    labels: List[str] = []
    basic = await orm_resource.get_basic()
    if basic is not None:
        for classification in basic.usermetadata.classifications:
            labels.append(f"{classification.labelset}/{classification.label}")

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    field_metadata = await field_obj.get_field_metadata()
    if field_metadata:
        paragraph = None
        if split not in (None, ""):
            metadata = field_metadata.split_metadata[split]
            paragraph = metadata.paragraphs[index]
        elif len(field_metadata.metadata.paragraphs) > index:
            paragraph = field_metadata.metadata.paragraphs[index]

        if paragraph is not None:
            for classification in paragraph.classifications:
                labels.append(f"{classification.labelset}/{classification.label}")

    return labels


async def get_text_resource(
    result: DocumentResult,
    kbid: str,
    query: Optional[str] = None,
    highlight_split: Optional[bool] = False,
    split: Optional[bool] = False,
) -> Tuple[str, Dict[str, List[Tuple[int, int]]]]:

    if query is None:
        return "", {}

    if split is False:
        return "", {}

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
        return "", {}

    _, field_type, field = result.field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warn(
            f"{result.uuid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return "", {}

    splitted_text, positions = split_text(
        extracted_text.text, query, highlight=highlight_split
    )

    return splitted_text, positions


async def get_text_paragraph(
    result: ParagraphResult,
    kbid: str,
    query: Optional[str] = None,
    highlight_split: bool = False,
    split: bool = False,
) -> Tuple[str, Dict[str, List[Tuple[int, int]]]]:
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
        return "", {}

    _, field_type, field = result.field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warn(
            f"{result.uuid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return "", {}

    positions: Dict[str, List[Tuple[int, int]]] = {}
    if result.split not in (None, ""):
        text = extracted_text.split_text[result.split]
        splitted_text = text[result.start : result.end]
    else:
        splitted_text = extracted_text.text[result.start : result.end]

    if query and split:
        splitted_text, positions = highlight(
            splitted_text, query, highlight=highlight_split
        )

    return splitted_text, positions


def split_text(text: str, query: str, highlight: bool = False, margin: int = 20):
    quoted = re.findall('"([^"]*)"', query)
    cleaned = query
    positions: Dict[str, List[Tuple[int, int]]] = {}
    for quote in quoted:
        cleaned = cleaned.replace(f'"{quote}"', "")
        found = [x.span() for x in re.finditer(quote, text)]
        if len(found):
            positions.setdefault(quote, []).extend(found)

    query_words = "".join([x for x in cleaned if x.isalnum() or x.isspace()]).split()
    for word in query_words:
        if len(word) > 2:
            found = [x.span() for x in re.finditer(word, text)]
            if len(found):
                positions.setdefault(word, []).extend(found)

    new_text = ""
    ordered = [x for xs in positions.values() for x in xs]
    ordered.sort()
    last = 0
    for order in ordered:
        if order[0] < last:
            continue
        if order[0] - margin > last and last > 0:
            new_text += text[last : last + margin]
            new_text += "..."
            last += margin

        if last > order[0] - margin:
            new_text += text[last : order[0]]
        else:
            new_text += " ..."
            new_text += text[order[0] - margin : order[0]]

        if highlight:
            new_text += "<b>"
        new_text += text[order[0] : order[1]]
        if highlight:
            new_text += "</b>"
        last = order[1]
    if len(new_text) > 0:
        new_text += text[last : min(len(text), last + margin)]
        new_text += "..."

    return new_text, positions


def highlight(text: str, query: str, highlight: bool = False):
    quoted = re.findall('"([^"]*)"', query)
    cleaned = query
    positions: Dict[str, List[Tuple[int, int]]] = {}
    for quote in quoted:
        cleaned = cleaned.replace(f'"{quote}"', "")
        found = [x.span() for x in re.finditer(quote, text)]
        if len(found):
            positions.setdefault(quote, []).extend(found)

    query_words = "".join([x for x in cleaned if x.isalnum() or x.isspace()]).split()
    for word in query_words:
        if len(word) > 2:
            found = [x.span() for x in re.finditer(word, text)]
            if len(found):
                positions.setdefault(word, []).extend(found)

    if highlight:
        new_text = ""
        ordered = [x for xs in positions.values() for x in xs]
        ordered.sort()
        last = 0
        for order in ordered:
            if order[0] < last:
                continue
            new_text += text[last : order[0]]
            new_text += "<b>"
            new_text += text[order[0] : order[1]]
            new_text += "</b>"
            last = order[1]
        new_text += text[last:]
        return new_text, positions
    return text, positions


async def get_labels_resource(result: DocumentResult, kbid: str) -> List[str]:
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

    return labels


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
