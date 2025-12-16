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
import base64
import random

from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCES_PREFIX
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.utilities import get_storage
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync


async def cookie_tale_resource(
    kbid: str, nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub
) -> str:
    """Resource with metadata (origin and security), a text field and a file
    field with pages, visual content and tables. Fields have extracted vectors.

    """
    slug = "cookie-tale"
    text_field_id = "cookie-tale"
    file_field_id = "cookie-recipie"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": slug,
            "title": "Replaced title",
            "summary": "Replaced summary",
            "origin": {
                "url": "my://url",
            },
            "security": {
                "access_groups": [
                    "developers",
                    "testers",
                ]
            },
            "texts": {
                text_field_id: {
                    "body": "Replaced text",
                    "format": "PLAIN",
                },
            },
            "files": {
                file_field_id: {
                    "language": "en",
                    "file": {
                        "filename": "cookies.pdf",
                        "content_type": "application/pdf",
                        "payload": base64.b64encode(b"some content we're not going to check").decode(),
                    },
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    storage = await get_storage()

    vectorsets = {}
    async with datamanagers.with_ro_transaction() as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vectorsets[vectorset_id] = vs
    # use a controlled random seed for vector generation
    random.seed(63)

    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        slug=slug,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )

    title_field = bmb.with_title("A tale of cookies")
    bmb.with_summary("Once upon a time, cookies were made...")

    ## Add a text field with paragraphs and paragraph relations

    text_field = bmb.field_builder(text_field_id, FieldType.TEXT)
    extracted_text = [
        "Once upon a time, there was a group of people called Nucliers. ",
        "One of them was an excellent cook and use to bring amazing cookies to their gatherings. ",
        "Chocolate, peanut butter and other delicious kinds of cookies. ",
        "Everyone loved them and those cookies are now part of their story. ",
    ]
    paragraph_ids = []
    paragraph_pbs = []
    for paragraph in extracted_text:
        paragraph_id, paragraph_pb = text_field.add_paragraph(
            paragraph,
            vectors={
                vectorset_id: [random.random()] * config.vectorset_index_config.vector_dimension
                for i, (vectorset_id, config) in enumerate(vectorsets.items())
            },
        )
        paragraph_ids.append(paragraph_id)
        paragraph_pbs.append(paragraph_pb)

    # add paragraph relations

    title_paragraph_id = list(title_field.iter_paragraphs())[0][0]
    paragraph_pbs[1].relations.parents.append(title_paragraph_id.full())

    paragraph_pbs[1].relations.siblings.append(paragraph_ids[0].full())

    paragraph_pbs[1].relations.replacements.extend([paragraph_ids[2].full(), paragraph_ids[3].full()])

    ## Add a file field with some visual content, pages and a table

    file_field = bmb.field_builder(file_field_id, FieldType.FILE)

    (_, paragraph_pb) = file_field.add_paragraph(
        "A yummy image of some cookies",
        kind=resources_pb2.Paragraph.TypeParagraph.INCEPTION,
        vectors={
            vectorset_id: [random.random()] * config.vectorset_index_config.vector_dimension
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )
    paragraph_pb.representation.reference_file = "cookies.png"

    # upload a source "image" for this paragraph
    sf = storage.file_extracted(bmb.bm.kbid, bmb.bm.uuid, "f", file_field_id, "generated/cookies.png")
    await storage.chunked_upload_object(sf.bucket, sf.key, payload=b"delicious cookies image")

    paragraph_pb.page.page = 0
    paragraph_pb.page.page_with_visual = True
    await file_field.add_page_preview(
        page=0,
        content=b"A page with an image of cookies",
    )

    # add a table.

    (_, paragraph_pb) = file_field.add_paragraph(
        "|Ingredient|Quantity|\n|Peanut butter|100g|\n...",
        kind=resources_pb2.Paragraph.TypeParagraph.TABLE,
        vectors={
            vectorset_id: [random.random()] * config.vectorset_index_config.vector_dimension
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )
    paragraph_pb.representation.is_a_table = True
    paragraph_pb.representation.reference_file = "ingredients_table.png"

    # unused right now, but this would be the source image for the table
    sf = storage.file_extracted(
        bmb.bm.kbid, bmb.bm.uuid, "f", file_field_id, "generated/ingredients_table.png"
    )
    await storage.chunked_upload_object(sf.bucket, sf.key, payload=b"ingredients table")

    paragraph_pb.page.page = 1
    paragraph_pb.page.page_with_visual = True
    await file_field.add_page_preview(
        page=1,
        content=b"A page with a table with ingredients and quantities",
    )

    # add a normal paragraph in the same page
    (_, paragraph_pb) = file_field.add_paragraph(
        "Above you can see a table with all the ingredients",
        vectors={
            vectorset_id: [random.random()] * config.vectorset_index_config.vector_dimension
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )

    paragraph_pb.page.page = 1
    paragraph_pb.page.page_with_visual = True

    bm = bmb.build()

    # customize fields we don't want to overwrite from the writer BM
    bm.origin.url = "my://url"

    # ingest the processed BM
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid
