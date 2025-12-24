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
import uuid
from datetime import datetime, timedelta

import jwt
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.ids import ParagraphId
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCES_PREFIX
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import CloudFile, FieldType
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
    random.seed(16)

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
    paragraphs = [
        (
            ParagraphId.from_string(f"{rid}/t/{text_field_id}/0-63"),
            "Once upon a time, there was a group of people called Nucliers. ",
        ),
        (
            ParagraphId.from_string(f"{rid}/t/{text_field_id}/63-151"),
            "One of them was an excellent cook and use to bring amazing cookies to their gatherings. ",
        ),
        (
            ParagraphId.from_string(f"{rid}/t/{text_field_id}/151-214"),
            "Chocolate, peanut butter and other delicious kinds of cookies. ",
        ),
        (
            ParagraphId.from_string(f"{rid}/t/{text_field_id}/214-281"),
            "Everyone loved them and those cookies are now part of their story. ",
        ),
    ]
    paragraph_pbs = []
    for expected_paragraph_id, paragraph in paragraphs:
        paragraph_id, paragraph_pb = text_field.add_paragraph(
            paragraph,
            vectors={
                vectorset_id: [
                    random.random() for _ in range(config.vectorset_index_config.vector_dimension)
                ]
                for i, (vectorset_id, config) in enumerate(vectorsets.items())
            },
        )
        paragraph_pbs.append(paragraph_pb)
        assert paragraph_id == expected_paragraph_id

    # add paragraph relations

    title_paragraph_id = next(iter(title_field.iter_paragraphs()))[0]
    paragraph_pbs[1].relations.parents.append(title_paragraph_id.full())

    paragraph_pbs[1].relations.siblings.append(paragraphs[0][0].full())

    paragraph_pbs[1].relations.replacements.extend([paragraphs[2][0].full(), paragraphs[3][0].full()])

    ## Add a file field with some visual content, pages and a table
    ##
    ## cookies.pdf
    ## +-------------------------------+
    ## |      +--------------+         |
    ## |      |              | <-------|--- cookies image (cookies.png)
    ## |      +--------------+  page 0 |
    ## +-------------------------------+
    ## |      +---+---+---+--+         |
    ## |      +---+---+---+--+ <-------|--- ingredients table (ingredients_table.png)
    ## |      |   |   |   |  |         |
    ## |      +---+---+---+--+         |
    ## |      Above you...      page 1 |
    ## +-----------^-------------------+
    ##             |
    ##             +----------------------- text paragraph
    ##

    file_field = bmb.field_builder(file_field_id, FieldType.FILE)

    (paragraph_id, paragraph_pb) = file_field.add_paragraph(
        "A yummy image of some cookies",
        kind=resources_pb2.Paragraph.TypeParagraph.INCEPTION,
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )
    paragraph_pb.representation.reference_file = "cookies.png"
    assert paragraph_id == ParagraphId.from_string(f"{rid}/f/{file_field_id}/0-29")

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

    (paragraph_id, paragraph_pb) = file_field.add_paragraph(
        "|Ingredient|Quantity|\n|Peanut butter|100g|\n...",
        kind=resources_pb2.Paragraph.TypeParagraph.TABLE,
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )
    paragraph_pb.representation.is_a_table = True
    paragraph_pb.representation.reference_file = "ingredients_table.png"
    assert paragraph_id == ParagraphId.from_string(f"{rid}/f/{file_field_id}/29-75")

    # this is an image representing the table, i.e., the image of the table
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
    (paragraph_id, paragraph_pb) = file_field.add_paragraph(
        "Above you can see a table with all the ingredients",
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )
    paragraph_pb.page.page = 1
    paragraph_pb.page.page_with_visual = True
    assert paragraph_id == ParagraphId.from_string(f"{rid}/f/{file_field_id}/75-125")

    # upload thumbnail and add it to file extracted data (we don't have builder for this)

    now = datetime.now()

    sf = storage.file_extracted(kbid, rid, "f", file_field_id, "file_thumbnail")
    await storage.chunked_upload_object(sf.bucket, sf.key, payload=b"cookie recipie (file) thumbnail")

    file_extracted_data = file_field._file
    file_extracted_data.language = "en"
    file_extracted_data.file_thumbnail.uri = jwt.encode(
        {
            "iss": "urn:processing_slow",
            "sub": "file",
            "aud": "urn:proxy",
            "exp": int((now + timedelta(days=7)).timestamp()),
            "iat": int(now.timestamp()),
            "jti": uuid.uuid4().hex,
            "bucket_name": "ncl-proxy-storage-gcp-stage-1",
            "filename": "thumbnail.jpg",
            "uri": f"kbs/{kbid}/r/{rid}/e/f/{file_field_id}/file_thumbnail",
            "size": 179054,
            "content_type": "image/jpeg",
        },
        "a-string-secret-at-least-256-bits-long",
        "HS256",
    )
    file_extracted_data.file_thumbnail.source = CloudFile.Source.LOCAL

    # build the broker message

    bm = bmb.build()

    # customize fields we don't want to overwrite from the writer BM
    bm.origin.url = "my://url"

    bm.file_extracted_data.append(file_extracted_data)

    # ingest the processed BM
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid
