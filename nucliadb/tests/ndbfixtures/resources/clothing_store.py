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


from httpx import AsyncClient

from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync

SCHEMA = {
    "name": "product",
    "description": "Clothing store product schema",
    "fields": [
        {"key": "color", "type": "text", "required": True},
        {"key": "price", "type": "float", "required": True},
        {"key": "featured", "type": "boolean", "required": False},
        {"key": "stock", "type": "integer", "required": False},
    ],
}

PRODUCTS: dict[str, tuple[str, dict]] = {
    "white-t-shirt": (
        "Modern white T-shirt with a surfer riding waves",
        {
            "color": "white",
            "price": 19.99,
            "featured": True,
            "stock": 140,
        },
    ),
    "floral-skirt": (
        "Linen skirt with floral motives",
        {
            "color": "white",
            "price": 24.99,
            "featured": True,
            "stock": 0,
        },
    ),
}


async def clothing_store_resources(
    kbid: str,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
) -> dict[str, str]:
    """Key-value schema and resources for a clothing store."""

    # First we create a key-value schema the products
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=SCHEMA)
    assert resp.status_code == 201

    # Then we add all the products as resources with a text field containing the
    # description and a key-value field with the specifications
    resources = {}
    for id, (description, json) in PRODUCTS.items():
        slug = id
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "slug": slug,
                "title": description,
                "key_values": {
                    "product": {
                        "schema_id": "product",
                        "data": json,
                    },
                },
            },
        )
        assert resp.status_code == 201, resp.text
        rid = resp.json()["uuid"]
        resources[id] = rid

        # generate processor broker messages to index titles
        bmb = BrokerMessageBuilder(
            kbid=kbid,
            rid=rid,
            slug=slug,
            source=BrokerMessage.MessageSource.PROCESSOR,
        )
        bmb.with_title(description)
        bm = bmb.build()
        await inject_message(nucliadb_ingest_grpc, bm)
        await wait_for_sync()

    return resources
