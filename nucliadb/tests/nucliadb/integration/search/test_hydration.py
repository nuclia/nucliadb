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
from collections.abc import AsyncIterable
from dataclasses import dataclass

import pytest
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import KB_PREFIX
from nucliadb_models import hydration
from nucliadb_models.common import FieldTypeName
from nucliadb_models.hydration import Hydrated
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import cookie_tale_resource


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
        f"{rid}/t/cookie-tale/63-151",
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

    assert hydrated.fields[f"{rid}/t/cookie-tale"].id == f"{rid}/t/cookie-tale"
    assert hydrated.fields[f"{rid}/t/cookie-tale"].resource == rid
    assert hydrated.fields[f"{rid}/t/cookie-tale"].field_type == FieldTypeName.TEXT

    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].id == f"{rid}/t/cookie-tale/63-151"
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].field == f"{rid}/t/cookie-tale"
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].resource == rid
    assert (
        hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].text
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
        f"{rid}/t/cookie-tale/63-151",
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

    assert hydrated.fields[f"{rid}/t/cookie-tale"].id == f"{rid}/t/cookie-tale"
    assert hydrated.fields[f"{rid}/t/cookie-tale"].resource == rid
    assert hydrated.fields[f"{rid}/t/cookie-tale"].field_type == FieldTypeName.TEXT

    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].id == f"{rid}/t/cookie-tale/63-151"
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].field == f"{rid}/t/cookie-tale"
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].resource == rid
    assert (
        hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].text
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
        f"{rid}/t/cookie-tale/63-151",
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
        f"{rid}/t/cookie-tale/0-63",
        f"{rid}/t/cookie-tale/63-151",
        f"{rid}/t/cookie-tale/151-214",
        f"{rid}/t/cookie-tale/214-281",
    } == hydrated.paragraphs.keys()

    # only the requested paragraph has related paragraphs, the rest are just added
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/0-63"].related is None
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/151-214"].related is None
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/214-281"].related is None

    # the requested paragraph has pointers to the other paragraphs
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.neighbours is not None  # type: ignore[union-attr]
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.neighbours.before == [  # type: ignore[union-attr]
        f"{rid}/t/cookie-tale/0-63"
    ]
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.neighbours.after == [  # type: ignore[union-attr]
        f"{rid}/t/cookie-tale/151-214",
        f"{rid}/t/cookie-tale/214-281",
    ]

    # all paragraphs have id and text by default
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/151-214"].id == f"{rid}/t/cookie-tale/151-214"
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/151-214"].field == f"{rid}/t/cookie-tale"
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/151-214"].resource == rid
    assert (
        hydrated.paragraphs[f"{rid}/t/cookie-tale/151-214"].text
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

    assert len(hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.neighbours.before) == 0  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.neighbours.after) == 1  # type: ignore[union-attr,arg-type]

    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].text is None
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/151-214"].text is None

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

    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.parents is not None  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.parents) == 1  # type: ignore[union-attr,arg-type]
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.parents == [f"{rid}/a/title/0-17"]  # type: ignore[union-attr,arg-type]
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

    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.siblings is not None  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.siblings) == 1  # type: ignore[union-attr,arg-type]
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.siblings == [  # type: ignore[union-attr,arg-type]
        f"{rid}/t/cookie-tale/0-63",
    ]
    assert f"{rid}/t/cookie-tale/0-63" in hydrated.paragraphs

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

    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related is not None
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.replacements is not None  # type: ignore[union-attr,arg-type]
    assert len(hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.replacements) == 2  # type: ignore[union-attr,arg-type]
    assert hydrated.paragraphs[f"{rid}/t/cookie-tale/63-151"].related.replacements == [  # type: ignore[union-attr,arg-type]
        f"{rid}/t/cookie-tale/151-214",
        f"{rid}/t/cookie-tale/214-281",
    ]
    assert f"{rid}/t/cookie-tale/151-214" in hydrated.paragraphs
    assert f"{rid}/t/cookie-tale/214-281" in hydrated.paragraphs


@pytest.mark.deploy_modes("standalone")
async def test_hydration_paragraph_source_image(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        # inception paragraph
        f"{rid}/f/cookie-recipie/0-29",
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

    assert (
        hydrated.paragraphs[f"{rid}/f/cookie-recipie/0-29"].image.source_image.content_type  # type: ignore[union-attr]
        == "image/png"
    )
    assert (
        hydrated.paragraphs[f"{rid}/f/cookie-recipie/0-29"].image.source_image.b64encoded  # type: ignore[union-attr]
        == base64.b64encode(b"delicious cookies image").decode()
    )


@pytest.mark.deploy_modes("standalone")
async def test_hydration_paragraph_table_page_preview(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        # table paragraph
        f"{rid}/f/cookie-recipie/29-75",
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

    assert hydrated.paragraphs[f"{rid}/f/cookie-recipie/29-75"].image is None
    assert hydrated.paragraphs[f"{rid}/f/cookie-recipie/29-75"].table.page_preview_ref == "1"  # type: ignore[union-attr,index]

    assert hydrated.fields[f"{rid}/f/cookie-recipie"].previews is not None  # type: ignore[union-attr]
    assert hydrated.fields[f"{rid}/f/cookie-recipie"].previews["1"].content_type == "image/png"  # type: ignore[union-attr,index]
    assert (
        hydrated.fields[f"{rid}/f/cookie-recipie"].previews["1"].b64encoded  # type: ignore[union-attr,index]
        == base64.b64encode(b"A page with a table with ingredients and quantities").decode()
    )


@pytest.mark.deploy_modes("standalone")
async def test_hydration_paragraph_page_preview(
    nucliadb_reader: AsyncClient,
    hydration_kb: HydrationKb,
):
    kbid = hydration_kb.kbid
    rid = hydration_kb.rid

    # a list of hardcoded ids to hydrate. These are taken from the broker
    # message data. Changing the broker message will probably affect these:
    paragraph_ids = [
        # table paragraph
        f"{rid}/f/cookie-recipie/75-125",
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

    assert hydrated.paragraphs[f"{rid}/f/cookie-recipie/75-125"].page is not None
    assert hydrated.paragraphs[f"{rid}/f/cookie-recipie/75-125"].page.page_preview_ref == "1"  # type: ignore[union-attr]

    assert hydrated.fields[f"{rid}/f/cookie-recipie"].previews is not None  # type: ignore[union-attr]
    assert hydrated.fields[f"{rid}/f/cookie-recipie"].previews["1"].content_type == "image/png"  # type: ignore[union-attr,index]
    assert (
        hydrated.fields[f"{rid}/f/cookie-recipie"].previews["1"].b64encoded  # type: ignore[union-attr,index]
        == base64.b64encode(b"A page with a table with ingredients and quantities").decode()
    )


@pytest.fixture
async def hydration_kb(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
) -> AsyncIterable[HydrationKb]:
    """KB with a dataset to properly test hydration."""

    kbid = standalone_knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)
    slug = "cookie-tale"
    yield HydrationKb(kbid=kbid, rid=rid, slug=slug)
