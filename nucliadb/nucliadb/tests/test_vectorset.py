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
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.asyncio
async def test_vectorset(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    """
    Test description:
    - Verify 412 Upload vectors without vectorset
    - Create a vectorset
    - Verify 412 Upload some vectors on the API invalid dimension
    - Upload some vectors on the API valid dimension
    - Count vectors
    - Search by vectorset
    - Modify one vector
    - Search by vectorset
    - Delete one vector
    - Search by vectorset
    - Get Vectorsets
    - Delete Vectorsets
    - Get Vectorsets
    - Search by vectorset
    """
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
            "uservectors": [
                {
                    "vectors": {
                        "vectorset1": {
                            "vector1": {
                                "vector": [],
                                "labels": ["label1"],
                                "positions": (0, 5),
                            }
                        }
                    },
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
        },
    )
    assert resp.status_code == 412
    assert resp.json()["detail"] == "Invalid vector should not be 0"

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/vectorset/vectorset1",
        json={"dimension": "512"},
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/vectorsets")
    assert resp.status_code == 200
    assert "vectorset1" in resp.json()["vectorsets"]

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
            "uservectors": [
                {
                    "vectors": {
                        "vectorset1": {
                            "vector1": {
                                "vector": [1.0, 0.9],
                                "labels": ["label1"],
                                "positions": (0, 5),
                            }
                        }
                    },
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
        },
    )

    assert resp.status_code == 412
    assert resp.json()["detail"] == "Invalid dimension should be 512 was 2"

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
            "uservectors": [
                {
                    "vectors": {
                        "vectorset1": {
                            "vector1": {
                                "vector": [0.0] * 512,
                                "labels": ["label1"],
                                "positions": (0, 5),
                            }
                        }
                    },
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
        },
    )

    assert resp.status_code == 412
    assert resp.json()["detail"] == "Invalid vector should not be 0"

    RES = {}
    for i in range(3):
        slug = f"myresource{i}"
        resp = await nucliadb_writer.post(
            f"/kb/{knowledgebox}/resources",
            json={
                "slug": slug,
                "texts": {"text1": {"body": "My text"}},
                "usermetadata": {
                    "classifications": [{"labelset": "type", "label": "Book"}]
                },
                "uservectors": [
                    {
                        "vectors": {
                            "vectorset1": {
                                "vector1": {
                                    "vector": [(i + 1) * 1.0] * 512,
                                    "positions": (0, 5),
                                }
                            }
                        },
                        "field": {"field": "text1", "field_type": "text"},
                    }
                ],
            },
        )
        RES[slug] = resp.json()["uuid"]
        assert resp.status_code == 201

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/counters?vectorset=vectorset1",
    )
    assert resp.json()["sentences"] == 3

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/counters",
    )
    assert resp.json()["sentences"] == 0

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={"vector": [1.0] * 512, "vectorset": "invalid", "features": ["vector"]},
    )
    assert len(resp.json()["sentences"]["results"]) == 0

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "query": "text",
            "vector": [0.3] * 512,
            "vectorset": "vectorset1",
            "features": ["vector"],
        },
    )
    assert len(resp.json()["sentences"]["results"]) == 3
    assert [result["index"] for result in resp.json()["sentences"]["results"]].count(
        "vector1"
    ) == 3

    # Modify first vector
    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{RES['myresource0']}",
        json={
            "uservectors": [
                {
                    "vectors": {
                        "vectorset1": {
                            "vector1": {
                                "vector": [4.0] * 512,
                                "positions": (0, 7),
                            }
                        }
                    },
                    "field": {"field": "text1", "field_type": "text"},
                }
            ]
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={"vector": [1.0] * 512, "vectorset": "vectorset1", "features": ["vector"]},
    )
    # We should not get myresource1 as first and only 1 res0
    assert [
        result["text"]
        for result in resp.json()["sentences"]["results"]
        if result["rid"] == RES["myresource0"]
    ] == ["My text"]

    # Delete first vector
    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{RES['myresource0']}",
        json={
            "uservectors": [
                {
                    "vectors_to_delete": {"vectorset1": {"vectors": ["vector1"]}},
                    "field": {"field": "text1", "field_type": "text"},
                }
            ]
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={"vector": [4.0] * 512, "vectorset": "vectorset1", "features": ["vector"]},
    )
    # We should not get myresource1
    assert len(resp.json()["sentences"]["results"]) == 2

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/vectorsets")
    assert resp.json()["vectorsets"]["vectorset1"]["dimension"] == 512

    resp = await nucliadb_writer.delete(f"/kb/{knowledgebox}/vectorset/vectorset1")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/vectorsets")
    assert len(resp.json()["vectorsets"]) == 0

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/counters?vectorset=vectorset1",
    )
    assert resp.json()["sentences"] == 0

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={"vector": [4.0] * 512, "vectorset": "vectorset1", "features": ["vector"]},
    )
    assert len(resp.json()["sentences"]["results"]) == 0
