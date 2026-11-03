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
from uuid import uuid4

import pytest
from nidx_protos.nodereader_pb2 import FilterExpression

from nucliadb.common.datamanagers import fields as field_datamanager
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.search.search import backend
from nucliadb.search.search.query_parser.old_filters import convert_keyword_filter_to_expression
from nucliadb_models.search import Filter

TEXT = "[SKU-123:4] Friedrich Nietzsche wrote this treatise."


async def write_field(txn, resource_id: str, text: str) -> None:
    await field_datamanager.create_processed_documents(
        txn,
        field=field_datamanager.FieldProcessedContent(
            resource_id=resource_id,
            field_id="summary",
            field_type="a",
            extracted_text=text,
            facets=["/test/facet"],
        ),
        chunks=[
            field_datamanager.ChunkContent(
                resource_id=resource_id,
                field_id="summary",
                field_type="a",
                chunk_id=f"{resource_id}/a/summary/0-{len(text)}",
                text=text,
                facets=["/test/facet"],
                data={},
            )
        ],
        embeddings=[],
    )


@pytest.mark.parametrize("field_level", [True, False])
async def test_keyword_filters(marklogic_maindb_driver: MarkLogicDriver, field_level: bool):
    kbid = uuid4().hex
    await marklogic_maindb_driver.ensure_kb_database(kbid)
    resource_id = uuid4().hex
    try:
        async with marklogic_maindb_driver.rw_transaction(kbid=kbid) as txn:
            await write_field(txn, resource_id, TEXT)
            # Unrelated field, so a match never selects every field
            await write_field(txn, uuid4().hex, "unrelated text")
            if field_level:
                expected_match = [MarkLogicURIs.field_directory(resource_id, "a", "summary")]
            else:
                expected_match = [
                    MarkLogicURIs.chunk(
                        resource_id, "a", "summary", f"{resource_id}/a/summary/0-{len(TEXT)}"
                    )
                ]

            cases: list[tuple[str | Filter, bool]] = [
                ("Nietzsche", True),
                ("niEtZscHe", True),
                ("Friedrich Nietzsche", True),
                ("SKU-123:4", True),
                ("Nietz", False),
                ("Nietzche", False),
                ("Nietzsche Friedrich", False),
                ("Focault", False),
                ("summary", False),
                (Filter(all=["Nietzsche", "Kant"]), False),
                (Filter(any=["Nietzsche", "Kant"]), True),
                (Filter(none=["Kant"]), True),
                (Filter(none=["Nietzsche"]), False),
            ]
            for keyword, expected in cases:
                expression = FilterExpression()
                operands = expression.bool_and.operands
                operands.add().resource.resource_id = resource_id
                operands.add().facet.facet = "/test/facet"
                operands.add().CopyFrom(convert_keyword_filter_to_expression(keyword))
                if field_level:
                    body = f"{backend._prefilter_javascript(expression, None)} return fieldDirs;"
                else:
                    query = backend._expression_query(expression, field_level=False)
                    body = (
                        "return matchingUris(cts.andQuery([cts.collectionQuery(COLLECTIONS.chunks), "
                        f"{query.js}]), {json.dumps(query.filtered)});"
                    )
                javascript = f"(() => {{ {backend._CONSTANTS_JS} {backend._HELPERS_JS} {body} }})()"
                matches = backend._flatten_eval_array(await documents.evaluate(txn, javascript))
                assert matches == (expected_match if expected else []), keyword
    finally:
        await marklogic_maindb_driver.delete_kb_database(kbid)
