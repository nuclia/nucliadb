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

import json
from typing import Any, AsyncGenerator

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.resource import KB_REVERSE, Resource
from nucliadb.train import logger
from nucliadb.train.generators.utils import batchify, get_resource_from_cache_or_db
from nucliadb_protos.dataset_pb2 import (
    ImageClassification,
    ImageClassificationBatch,
    TrainSet,
)
from nucliadb_protos.nodereader_pb2 import StreamRequest
from nucliadb_protos.resources_pb2 import FieldType, PageStructure, VisualSelection

VISUALLY_ANNOTABLE_FIELDS = {FieldType.FILE, FieldType.LINK}

# PAWLS JSON format
PawlsPayload = dict[str, Any]


def image_classification_batch_generator(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
) -> AsyncGenerator[ImageClassificationBatch, None]:
    generator = generate_image_classification_payloads(kbid, trainset, node, shard_replica_id)
    batch_generator = batchify(generator, trainset.batch_size, ImageClassificationBatch)
    return batch_generator


async def generate_image_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
) -> AsyncGenerator[ImageClassification, None]:
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    request.reload = True
    async for item in node.stream_get_fields(request):
        rid = item.uuid
        resource = await get_resource_from_cache_or_db(kbid, rid)
        if resource is None:
            logger.error(f"Resource {rid} does not exist on DB")
            return

        _, field_type_key, field_key = item.field.split("/")
        field_type = KB_REVERSE[field_type_key]

        if field_type not in VISUALLY_ANNOTABLE_FIELDS:
            continue

        field = await resource.get_field(field_key, field_type, load=True)

        page_selections = await get_page_selections(resource, field)
        if len(page_selections) == 0:
            # Generating a payload without annotations makes no sense
            continue

        page_structure = await get_page_structure(field)

        for page, (page_uri, ps) in enumerate(page_structure):
            pawls_payload = {
                "width": ps.page.width,
                "height": ps.page.height,
                "tokens": [
                    {
                        "x": token.x,
                        "y": token.y,
                        "width": token.width,
                        "height": token.height,
                        "text": token.text,
                        "line": token.line,
                    }
                    for token in ps.tokens
                ],
                "annotations": [
                    {
                        "page": page,
                        "label": {
                            "text": selection.label,
                        },
                        "bounds": {
                            "top": selection.top,
                            "left": selection.left,
                            "right": selection.right,
                            "bottom": selection.bottom,
                        },
                        "tokens": [
                            {
                                "pageIndex": page,
                                "tokenIndex": token_id,
                            }
                            for token_id in selection.token_ids
                        ],
                    }
                    for selection in page_selections[page]
                ],
            }

            ic = ImageClassification()
            ic.page_uri = page_uri
            ic.selections = json.dumps(pawls_payload)

            yield ic


async def get_page_selections(resource: Resource, field: Field) -> dict[int, list[VisualSelection]]:
    page_selections: dict[int, list[VisualSelection]] = {}
    basic = await resource.get_basic()
    if basic is None or basic.fieldmetadata is None:
        return page_selections

    # We assume only one fieldmetadata per field as it's implemented in
    # resource ingestion
    for fieldmetadata in basic.fieldmetadata:
        if (
            fieldmetadata.field.field == field.id
            and fieldmetadata.field.field_type == KB_REVERSE[field.type]
        ):
            for selection in fieldmetadata.page_selections:
                page_selections[selection.page] = selection.visual  # type: ignore
            break

    return page_selections


async def get_page_structure(field: Field) -> list[tuple[str, PageStructure]]:
    page_structures: list[tuple[str, PageStructure]] = []
    field_type = KB_REVERSE[field.type]
    if field_type == FieldType.FILE:
        fed = await field.get_file_extracted_data()  # type: ignore
        if fed is None:
            return page_structures

        fp = fed.file_pages_previews
        if len(fp.pages) != len(fp.structures):
            field_path = f"/kb/{field.kbid}/resource/{field.resource.uuid}/file/{field.id}"
            logger.warning(
                f"File extracted data has a different number of pages and structures! ({field_path})"
            )
            return page_structures
        page_structures.extend(
            [
                # we expect this two field to have the same length, if not,
                # something went wrong while processing
                (fp.pages[i].uri, fp.structures[i])
                for i in range(len(fp.pages))
            ]
        )

    elif field_type == FieldType.LINK:
        led = await field.get_link_extracted_data()  # type: ignore
        if led is None:
            return page_structures

        page_structures.append((led.link_image.uri, led.pdf_structure))

    return page_structures
