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
import base64
from dataclasses import dataclass
from typing import AsyncIterable

import pytest
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCES_PREFIX
from nucliadb_models import hydration
from nucliadb_models.common import FieldTypeName
from nucliadb_models.hydration import Hydrated
from nucliadb_protos import resources_pb2
from nucliadb_protos.writer_pb2 import BrokerMessage, FieldType
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.utilities import get_storage
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync


@dataclass
class HydrationKb:
    """Helper dataclass to describe the fixture KB to the tests."""

    kbid: str
    rid: str
    slug: str


@pytest.mark.deploy_modes("standalone")
async def test_hydration_with_default_params(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid
    slug = hydration_kb.slug

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        f"{rid}/t/mytext/63-151",
    ]

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={"data": paragraph_ids, "hydration": hydration.Hydration().model_dump()},
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.resources[rid].id == rid
    assert hydrated.resources[rid].slug == slug
    assert hydrated.resources[rid].title == "A tale of cookies"
    assert hydrated.resources[rid].summary is None
    assert hydrated.resources[rid].origin is None
    assert hydrated.resources[rid].security is None

    assert hydrated.fields[f"{rid}/t/mytext"].id == f"{rid}/t/mytext"
    assert hydrated.fields[f"{rid}/t/mytext"].resource == rid
    assert hydrated.fields[f"{rid}/t/mytext"].field_type == FieldTypeName.TEXT

    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].id == f"{rid}/t/mytext/63-151"
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].field == f"{rid}/t/mytext"
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].resource == rid
    assert (
        hydrated.paragraphs[f"{rid}/t/mytext/63-151"].text
        == "One of them was an excellent cook and use to bring amazing cookies to their gatherings. "
    )


@pytest.mark.deploy_modes("standalone")
async def test_resource_hydration(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid
    slug = hydration_kb.slug

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        f"{rid}/t/mytext/63-151",
    ]

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                resource=hydration.ResourceHydration(
                    title=True, summary=True, origin=True, security=True
                )
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.resources[rid].id == rid
    assert hydrated.resources[rid].slug == slug
    assert hydrated.resources[rid].title == "A tale of cookies"
    assert hydrated.resources[rid].summary == "Once upon a time, cookies were made..."
    assert hydrated.resources[rid].origin is not None
    assert hydrated.resources[rid].origin.url == "my://url"  # type: ignore[union-attr]
    assert hydrated.resources[rid].security is not None
    assert set(hydrated.resources[rid].security.access_groups) == {"developers", "testers"}  # type: ignore[union-attr]

    assert hydrated.fields[f"{rid}/t/mytext"].id == f"{rid}/t/mytext"
    assert hydrated.fields[f"{rid}/t/mytext"].resource == rid
    assert hydrated.fields[f"{rid}/t/mytext"].field_type == FieldTypeName.TEXT

    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].id == f"{rid}/t/mytext/63-151"
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].field == f"{rid}/t/mytext"
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].resource == rid
    assert (
        hydrated.paragraphs[f"{rid}/t/mytext/63-151"].text
        == "One of them was an excellent cook and use to bring amazing cookies to their gatherings. "
    )


@pytest.mark.deploy_modes("standalone")
async def test_hydration_related_paragraphs(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        f"{rid}/t/mytext/63-151",
    ]

    # TEST: hydrate paragraph neighbours

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    related=hydration.RelatedParagraphHydration(
                        # we ask for an extra paragraph before and after than
                        # the ones that actually exist
                        neighbours=hydration.NeighbourParagraphHydration(
                            before=1 + 1,
                            after=2 + 1,
                        )
                    )
                ),
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    # all field paragraphs have been hydrated
    assert {
        f"{rid}/t/mytext/0-63",
        f"{rid}/t/mytext/63-151",
        f"{rid}/t/mytext/151-214",
        f"{rid}/t/mytext/214-281",
    } == hydrated.paragraphs.keys()

    # only the requested paragraph has related paragraphs, the rest are just added
    assert hydrated.paragraphs[f"{rid}/t/mytext/0-63"].related is None
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/mytext/151-214"].related is None
    assert hydrated.paragraphs[f"{rid}/t/mytext/214-281"].related is None

    # the requested paragraph has pointers to the other paragraphs
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.neighbours is not None  # type: ignore[union-attr]
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.neighbours.before == [  # type: ignore[union-attr]
        f"{rid}/t/mytext/0-63"
    ]
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.neighbours.after == [  # type: ignore[union-attr]
        f"{rid}/t/mytext/151-214",
        f"{rid}/t/mytext/214-281",
    ]

    # all paragraphs have id and text by default
    assert hydrated.paragraphs[f"{rid}/t/mytext/151-214"].id == f"{rid}/t/mytext/151-214"
    assert hydrated.paragraphs[f"{rid}/t/mytext/151-214"].field == f"{rid}/t/mytext"
    assert hydrated.paragraphs[f"{rid}/t/mytext/151-214"].resource == rid
    assert (
        hydrated.paragraphs[f"{rid}/t/mytext/151-214"].text
        == "Chocolate, peanut butter and other delicious kinds of cookies. "
    )

    # TEST: do not hydrate text. Nor the paragraph to hydrate nor their related
    # will have text

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    text=False,
                    related=hydration.RelatedParagraphHydration(
                        # we ask for an extra paragraph before and after than
                        # the ones that actually exist
                        neighbours=hydration.NeighbourParagraphHydration(
                            before=0,
                            after=1,
                        )
                    ),
                ),
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert len(hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.neighbours.before) == 0  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.neighbours.after) == 1  # type: ignore[union-attr,arg-type]

    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].text is None
    assert hydrated.paragraphs[f"{rid}/t/mytext/151-214"].text is None

    # TEST: hydrate related parents

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    related=hydration.RelatedParagraphHydration(
                        parents=True,
                    ),
                ),
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.parents is not None  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.parents) == 1  # type: ignore[union-attr,arg-type]
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.parents == [f"{rid}/a/title/0-17"]  # type: ignore[union-attr,arg-type]
    assert f"{rid}/a/title/0-17" in hydrated.paragraphs

    # TEST: hydrate related siblings

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    related=hydration.RelatedParagraphHydration(
                        siblings=True,
                    ),
                ),
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.siblings is not None  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.siblings) == 1  # type: ignore[union-attr,arg-type]
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.siblings == [  # type: ignore[union-attr,arg-type]
        f"{rid}/t/mytext/0-63",
    ]
    assert f"{rid}/t/mytext/0-63" in hydrated.paragraphs

    # TEST: hydrate related replacements

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    related=hydration.RelatedParagraphHydration(
                        replacements=True,
                    ),
                ),
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.replacements is not None  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.replacements) == 2  # type: ignore[union-attr,arg-type]
    assert hydrated.paragraphs[f"{rid}/t/mytext/63-151"].related.replacements == [  # type: ignore[union-attr,arg-type]
        f"{rid}/t/mytext/151-214",
        f"{rid}/t/mytext/214-281",
    ]
    assert f"{rid}/t/mytext/151-214" in hydrated.paragraphs
    assert f"{rid}/t/mytext/214-281" in hydrated.paragraphs


@pytest.mark.deploy_modes("standalone")
async def test_hydration_paragraph_source_image__WIP(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        # inception paragraph
        f"{rid}/f/myfile/0-29",
    ]

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    image=hydration.ImageParagraphHydration(
                        source_image=True,
                    ),
                )
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.paragraphs[f"{rid}/f/myfile/0-29"].image.source_image.content_type == "image/png"  # type: ignore[union-attr]
    assert (
        hydrated.paragraphs[f"{rid}/f/myfile/0-29"].image.source_image.b64encoded  # type: ignore[union-attr]
        == base64.b64encode(b"delicious cookies image").decode()
    )


@pytest.mark.deploy_modes("standalone")
async def test_hydration_paragraph_table_page_preview__WIP(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        # table paragraph
        f"{rid}/f/myfile/29-75",
    ]

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    table=hydration.TableParagraphHydration(
                        table_page_preview=True,
                    ),
                    page=hydration.ParagraphPageHydration(
                        page_with_visual=True,
                    ),
                )
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.paragraphs[f"{rid}/f/myfile/29-75"].image is None
    assert hydrated.paragraphs[f"{rid}/f/myfile/29-75"].table.page_preview_ref == "1"  # type: ignore[union-attr,index]

    assert hydrated.fields[f"{rid}/f/myfile"].previews is not None  # type: ignore[union-attr]
    assert hydrated.fields[f"{rid}/f/myfile"].previews["1"].content_type == "image/png"  # type: ignore[union-attr,index]
    assert (
        hydrated.fields[f"{rid}/f/myfile"].previews["1"].b64encoded  # type: ignore[union-attr,index]
        == base64.b64encode(b"A page with a table with ingredients and quantities").decode()
    )


@pytest.mark.deploy_modes("standalone")
async def test_hydration_paragraph_page_preview__WIP(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        # table paragraph
        f"{rid}/f/myfile/75-125",
    ]

    resp = await nucliadb_reader.post(
        f"/{KB_PREFIX}/{kbid}/hydrate",
        json={
            "data": paragraph_ids,
            "hydration": hydration.Hydration(
                paragraph=hydration.ParagraphHydration(
                    page=hydration.ParagraphPageHydration(
                        page_with_visual=True,
                    )
                )
            ).model_dump(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    hydrated = Hydrated.model_validate(body)

    assert hydrated.paragraphs[f"{rid}/f/myfile/75-125"].page is not None
    assert hydrated.paragraphs[f"{rid}/f/myfile/75-125"].page.page_preview_ref == "1"  # type: ignore[union-attr]

    assert hydrated.fields[f"{rid}/f/myfile"].previews is not None  # type: ignore[union-attr]
    assert hydrated.fields[f"{rid}/f/myfile"].previews["1"].content_type == "image/png"  # type: ignore[union-attr,index]
    assert (
        hydrated.fields[f"{rid}/f/myfile"].previews["1"].b64encoded  # type: ignore[union-attr,index]
        == base64.b64encode(b"A page with a table with ingredients and quantities").decode()
    )


@pytest.fixture
async def hydration_kb(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
) -> AsyncIterable[HydrationKb]:
    """KB with a dataset to properly test hydration."""

    kbid = standalone_knowledgebox
    slug = "myresource"

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
                "mytext": {
                    "body": "Replaced text",
                    "format": "PLAIN",
                },
            },
            "files": {
                "myfile": {
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
    body = resp.json()
    assert "uuid" in body
    rid = body["uuid"]

    # build a BM "from processor"
    bmb = BrokerMessageBuilder(
        kbid=kbid, rid=rid, slug=slug, source=BrokerMessage.MessageSource.PROCESSOR
    )
    bm = await cookies_broker_message(bmb)

    # customize fields we don't want to overwrite from the writer BM
    bm.origin.url = "my://url"

    # ingest the processed BM
    await inject_message(nucliadb_ingest_grpc, bm)
    await asyncio.sleep(0.1)
    await wait_for_sync()

    yield HydrationKb(kbid=kbid, rid=rid, slug=slug)


async def cookies_broker_message(bmb: BrokerMessageBuilder) -> BrokerMessage:
    """Given an empty broker message builder, construct a fairly complete broker
    message.

    """
    storage = await get_storage()

    title_field = bmb.with_title("A tale of cookies")
    bmb.with_summary("Once upon a time, cookies were made...")

    ## Add a text field with paragraphs and paragraph relations

    text_field = bmb.field_builder("mytext", FieldType.TEXT)
    extracted_text = [
        "Once upon a time, there was a group of people called Nucliers. ",
        "One of them was an excellent cook and use to bring amazing cookies to their gatherings. ",
        "Chocolate, peanut butter and other delicious kinds of cookies. ",
        "Everyone loved them and those cookies are now part of their story. ",
    ]
    paragraph_ids = []
    paragraph_pbs = []
    for paragraph in extracted_text:
        paragraph_id, paragraph_pb = text_field.add_paragraph(paragraph)
        paragraph_ids.append(paragraph_id)
        paragraph_pbs.append(paragraph_pb)

    # add paragraph relations

    title_paragraph_id = list(title_field.iter_paragraphs())[0][0]
    paragraph_pbs[1].relations.parents.append(title_paragraph_id.full())

    paragraph_pbs[1].relations.siblings.append(paragraph_ids[0].full())

    paragraph_pbs[1].relations.replacements.extend([paragraph_ids[2].full(), paragraph_ids[3].full()])

    ## Add a file field with some visual content, pages and a table

    file_field = bmb.field_builder("myfile", FieldType.FILE)

    (_, paragraph_pb) = file_field.add_paragraph(
        "A yummy image of some cookies",
        kind=resources_pb2.Paragraph.TypeParagraph.INCEPTION,
    )
    paragraph_pb.representation.reference_file = "cookies.png"

    # upload an source "image" for this paragraph
    sf = storage.file_extracted(bmb.bm.kbid, bmb.bm.uuid, "f", "myfile", "generated/cookies.png")
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
    )
    paragraph_pb.representation.is_a_table = True
    paragraph_pb.representation.reference_file = "ingredients_table.png"

    # unused right now, but this would be the source image for the table
    sf = storage.file_extracted(
        bmb.bm.kbid, bmb.bm.uuid, "f", "myfile", "generated/ingredients_table.png"
    )
    await storage.chunked_upload_object(sf.bucket, sf.key, payload=b"ingredients table")

    paragraph_pb.page.page = 1
    paragraph_pb.page.page_with_visual = True
    await file_field.add_page_preview(
        page=1,
        content=b"A page with a table with ingredients and quantities",
    )

    # add a normal paragraph in the same page
    (_, paragraph_pb) = file_field.add_paragraph("Above you can see a table with all the ingredients")

    paragraph_pb.page.page = 1
    paragraph_pb.page.page_with_visual = True

    # TODO
    # link_field = bmb.field_builder("mylink", FieldType.LINK)

    # TODO
    # conversation_field = bmb.field_builder("myconversation", FieldType.CONVERSATION)

    return bmb.build()
