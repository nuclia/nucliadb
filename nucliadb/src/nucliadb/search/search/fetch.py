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
import asyncio
from contextvars import ContextVar
from typing import Optional

from nidx_protos.nodereader_pb2 import DocumentResult, ParagraphResult

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.serialize import managed_serialize
from nucliadb.search import SERVICE_NAME, logger
from nucliadb.search.search import cache
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import ResourceProperties
from nucliadb_protos.resources_pb2 import Paragraph
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

rcache: ContextVar[Optional[dict[str, ResourceORM]]] = ContextVar("rcache", default=None)


async def fetch_resources(
    resources: list[str],
    kbid: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
) -> dict[str, Resource]:
    if ResourceProperties.EXTRACTED in show and has_feature(
        const.Features.IGNORE_EXTRACTED_IN_SEARCH, context={"kbid": kbid}, default=False
    ):
        # Returning extracted metadata in search results is deprecated and this flag
        # will be set to True for all KBs in the future.
        show.remove(ResourceProperties.EXTRACTED)
        extracted = []

    result = {}
    async with get_driver().transaction(read_only=True) as txn:
        tasks = []
        for resource in resources:
            tasks.append(
                asyncio.create_task(
                    managed_serialize(
                        txn,
                        kbid,
                        resource,
                        show,
                        field_type_filter=field_type_filter,
                        extracted=extracted,
                        service_name=SERVICE_NAME,
                    )
                )
            )
        for resource, serialization in zip(resources, await asyncio.gather(*tasks)):
            if serialization is not None:
                result[resource] = serialization
    return result


async def get_paragraph_from_resource(
    orm_resource: ResourceORM, result: ParagraphResult
) -> Optional[Paragraph]:
    _, field_type, field = result.field.split("/")
    field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
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


async def get_labels_resource(result: DocumentResult, kbid: str) -> list[str]:
    orm_resource = await cache.get_resource(kbid, result.uuid)

    if orm_resource is None:
        logger.error(f"{result.uuid} does not exist on DB")
        return []

    labels: list[str] = []
    basic = await orm_resource.get_basic()
    if basic is not None:
        for classification in basic.usermetadata.classifications:
            labels.append(f"{classification.labelset}/{classification.label}")

    return labels


async def get_labels_paragraph(result: ParagraphResult, kbid: str) -> list[str]:
    orm_resource = await cache.get_resource(kbid, result.uuid)

    if orm_resource is None:
        logger.error(f"{result.uuid} does not exist on DB")
        return []

    labels: list[str] = []
    basic = await orm_resource.get_basic()
    if basic is not None:
        for classification in basic.usermetadata.classifications:
            labels.append(f"{classification.labelset}/{classification.label}")

    _, field_type, field = result.field.split("/")
    field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
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
) -> Optional[tuple[list[int], list[int]]]:
    orm_resource = await cache.get_resource(kbid, result.uuid)

    if orm_resource is None:
        logger.error(f"{result.uuid} does not exist on DB")
        return None

    paragraph = await get_paragraph_from_resource(orm_resource=orm_resource, result=result)

    if paragraph is not None and len(paragraph.end_seconds) > 0 and paragraph.end_seconds[0] > 0:
        return (list(paragraph.start_seconds), list(paragraph.end_seconds))

    return None
