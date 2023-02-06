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
import string
from contextvars import ContextVar
from typing import Dict, List, Optional, Tuple

from nucliadb_protos.nodereader_pb2 import DocumentResult, ParagraphResult
from nucliadb_protos.resources_pb2 import Paragraph

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.serialize import serialize
from nucliadb.ingest.utils import get_driver
from nucliadb.search import SERVICE_NAME, logger
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import ResourceProperties
from nucliadb_utils.utilities import get_cache, get_storage

rcache: ContextVar[Optional[Dict[str, ResourceORM]]] = ContextVar(
    "rcache", default=None
)
txn: ContextVar[Optional[Transaction]] = ContextVar("txn", default=None)

PRE_WORD = string.punctuation + " "


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


async def abort_transaction() -> None:
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
            service_name=SERVICE_NAME,
        )
        if serialization is not None:
            result[resource] = serialization
    return result


async def get_resource_from_cache(kbid: str, uuid: str) -> Optional[ResourceORM]:
    resouce_cache = get_resource_cache()
    orm_resource: Optional[ResourceORM] = None
    if uuid not in resouce_cache:
        transaction = await get_transaction()
        storage = await get_storage(service_name=SERVICE_NAME)
        cache = await get_cache()
        kb = KnowledgeBoxORM(transaction, storage, cache, kbid)
        orm_resource = await kb.get(uuid)
        if orm_resource is not None:
            resouce_cache[uuid] = orm_resource
    else:
        orm_resource = resouce_cache.get(uuid)
    return orm_resource


async def get_paragraph_from_resource(
    orm_resource: ResourceORM, result: ParagraphResult
) -> Optional[Paragraph]:
    _, field_type, field = result.field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    field_metadata = await field_obj.get_field_metadata()
    paragraph = None
    if field_metadata:
        if result.split not in (None, ""):
            metadata = field_metadata.split_metadata[result.split]
            paragraph = metadata.paragraphs[result.index]
        elif len(field_metadata.metadata.paragraphs) > result.index:
            paragraph = field_metadata.metadata.paragraphs[result.index]
    return paragraph


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
    orm_resource = await get_resource_from_cache(kbid, rid)

    if orm_resource is None:
        logger.warn(f"{rid} does not exist on DB")
        return ""

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.info(
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


async def get_text_paragraph(
    result: ParagraphResult,
    kbid: str,
    highlight: bool = False,
    ematches: Optional[List[str]] = None,
) -> str:
    orm_resource = await get_resource_from_cache(kbid, result.uuid)

    if orm_resource is None:
        logger.error(f"{result.uuid} does not exist on DB")
        return ""

    _, field_type, field = result.field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warn(
            f"{result.uuid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return ""

    if result.split not in (None, ""):
        text = extracted_text.split_text[result.split]
        splitted_text = text[result.start : result.end]
    else:
        splitted_text = extracted_text.text[result.start : result.end]

    if highlight:
        splitted_text = highlight_paragraph(
            splitted_text, words=result.matches, ematches=ematches  # type: ignore
        )

    return splitted_text


def get_regex(some_string: str) -> str:
    return r"\b" + some_string.lower() + r"\b"


def highlight_paragraph(
    text: str, words: Optional[List[str]] = None, ematches: Optional[List[str]] = None
) -> str:
    text_lower = text.lower()

    marks = [0] * (len(text_lower) + 1)
    if ematches is not None:
        for quote in ematches:
            quote_regex = get_regex(quote.lower())
            try:
                for match in re.finditer(quote_regex, text_lower):
                    start, end = match.span()
                    marks[start] = 1
                    marks[end] = 2
            except re.error:
                logger.warning(
                    f"Regex errors while highlighting text. Regex: {quote_regex}"
                )
                continue

    words = words or []
    for word in words:
        word_regex = get_regex(word.lower())
        try:
            for match in re.finditer(word_regex, text_lower):
                start, end = match.span()
                if marks[start] == 0 and marks[end] == 0:
                    marks[start] = 1
                    marks[end] = 2
        except re.error:
            logger.warning(f"Regex errors while highlighting text. Regex: {word_regex}")
            continue

    new_text = ""
    actual = 0
    mod = 0
    skip = False

    length = len(text)

    for index, pos in enumerate(marks):
        if skip:
            skip = False
            continue
        if (index - mod) >= length:
            char_pos = ""
        else:
            begining = True
            if index > 0 and text[index - mod - 1] not in PRE_WORD:
                begining = False
            char_pos = text[index - mod]
            if text[index - mod].lower() != text_lower[index]:
                # May be incorrect positioning due to unicode lower
                mod += 1
                skip = True
        if pos == 1 and actual == 0 and begining:
            new_text += "<mark>"
            new_text += char_pos
            actual = 1
        elif pos == 2 and actual == 1:
            new_text += "</mark>"
            new_text += char_pos
            actual = 0
        elif pos == 1 and actual > 0:
            new_text += char_pos
            actual += 1
        elif pos == 2 and actual > 1:
            new_text += char_pos
            actual -= 1
        else:
            new_text += char_pos

    return new_text


async def get_labels_resource(result: DocumentResult, kbid: str) -> List[str]:
    orm_resource = await get_resource_from_cache(kbid, result.uuid)

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
    orm_resource = await get_resource_from_cache(kbid, result.uuid)

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


async def get_seconds_paragraph(
    result: ParagraphResult, kbid: str
) -> Optional[Tuple[List[int], List[int]]]:
    orm_resource = await get_resource_from_cache(kbid, result.uuid)

    if orm_resource is None:
        logger.error(f"{result.uuid} does not exist on DB")
        return None

    paragraph = await get_paragraph_from_resource(
        orm_resource=orm_resource, result=result
    )

    if (
        paragraph is not None
        and len(paragraph.end_seconds) > 0
        and paragraph.end_seconds[0] > 0
    ):
        return (list(paragraph.start_seconds), list(paragraph.end_seconds))

    return None
