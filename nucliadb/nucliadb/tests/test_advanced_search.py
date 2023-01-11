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
import pytest
from httpx import AsyncClient

from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_protos import resources_pb2 as rpb


@pytest.fixture(scope="function")
async def testdata(nucliadb_grpc, knowledgebox):
    kbid = knowledgebox

    await inject_message(
        nucliadb_grpc,
        broker_resource(
            kbid,
            rid="barack",
            title="barack obama",
            summary="Barack was the president some time ago",
        ),
    )

    await inject_message(
        nucliadb_grpc,
        broker_resource(
            kbid,
            rid="trump",
            title="donald trump",
            summary="Donald Trump has also been a president in the past",
        ),
    )

    await inject_message(
        nucliadb_grpc,
        broker_resource(
            kbid,
            rid="sre",
            title="Benefits of SRE",
            summary="Increased efficiency through automation",
        ),
    )

    await inject_message(
        nucliadb_grpc,
        broker_resource(
            kbid,
            rid="devops",
            title="The Ultimate Guide to DevOps",
            summary="DevOps is a set of practices that combines software development (Dev) and IT operations (Ops).",
        ),
    )

    await inject_message(
        nucliadb_grpc,
        broker_resource(
            kbid,
            rid="terraform",
            title="Terraform for dummies",
            summary="Terraform is used by DevOps and SRE engineers",
        ),
    )

    await inject_message(
        nucliadb_grpc,
        broker_resource(
            kbid,
            rid="terraform-boosted",
            title="The Terraform Bible. By the Terraform team.",
            summary="Terraform, Terraform and only Terraform.",
        ),
    )

    bm = broker_resource(
        kbid,
        rid="terraform-boosted-2",
        title="The Terraform guide. Terraform. Terraform.",
        summary="Terraform, Terraform and only Terraform.",
    )
    c4 = rpb.Classification()
    c4.label = "label_user"
    c4.labelset = "labelset_resources"
    bm.basic.usermetadata.classifications.append(c4)
    await inject_message(nucliadb_grpc, bm)


@pytest.mark.asyncio
async def test_search_advanced_query_basic(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    testdata,
):
    """
    Test description:
    - Searching with an invalid tantivy syntax should return an error
    - Searching with a valid tantivy advanced query should return expected results
    - Searching with advanceed query and a regular query should return the intersection
      of the results (ie: AND operation)
    """
    kbid = knowledgebox

    # Invalid advanced query
    invalid_advanced_query = "IN [unbalanced-parenthesis"
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "advanced_query": invalid_advanced_query,
        },
    )
    assert resp.status_code != 200

    # Valid advanced query
    advanced_query = 'uuid:"barack" OR uuid:"trump"'
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search?advanced_query={advanced_query}"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["resources"]["barack"]
    assert resp_json["resources"]["trump"]

    # Advanced query + regular query should AND the results
    query_all = ""
    advanced_query = 'uuid:"barack"'
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": query_all,
            "advanced_query": advanced_query,
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    assert resp_json["resources"]["barack"]


@pytest.mark.asyncio
async def test_search_advanced_examples(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    testdata,
):
    kbid = knowledgebox

    # Give me resources with titles containing SRE OR DevOps
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json=dict(
            features=["document"],
            fields=["a/title"],
            advanced_query="text:SRE OR text:DevOps",
        ),
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 2
    assert resp_json["resources"]["sre"]
    assert resp_json["resources"]["devops"]

    # Give me resources with summaries containing SRE OR DevOps
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json=dict(
            features=["document"],
            fields=["a/summary"],
            advanced_query="text:SRE OR text:DevOps",
        ),
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 2
    assert resp_json["resources"]["terraform"]
    assert resp_json["resources"]["devops"]

    # Give me resources with titles containing Bible AND Terraform
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json=dict(
            features=["document"],
            fields=["a/title"],
            advanced_query="text:Bible AND text:Terraform",
        ),
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    assert resp_json["resources"]["terraform-boosted"]

    # Give me the most relevant resource that
    # - Is in spanish language
    # - Has 'Terraform' in the title (boosted by 2)
    # - 'dummies' is not in the title
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json=dict(
            features=["document"],
            fields=["a/title"],
            filters=["/s/p/es"],
            advanced_query="text:Terraform^2.0 -dummies",
            page_size=1,
        ),
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    assert resp_json["resources"]["terraform-boosted-2"]

    # Give resources that:
    # - Are labeled with 'label_user' from the `labelset_resources` labelset
    # - Has 'Terraform' in the title
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json=dict(
            features=["document"],
            fields=["a/title"],
            filters=["/l/labelset_resources/label_user"],
            advanced_query="text:Terraform",
        ),
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    assert resp_json["resources"]["terraform-boosted-2"]
