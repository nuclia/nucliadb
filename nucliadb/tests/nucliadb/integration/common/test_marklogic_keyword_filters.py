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
from nidx_protos.nodereader_pb2 import SearchRequest

from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.marklogic import documents
from nucliadb.search.search.backend import _filter_context
from nucliadb.search.search.query_parser.old_filters import convert_keyword_filter_to_expression
from nucliadb_models.search import Filter


@pytest.mark.parametrize("text_property", ["text", "extracted_text"])
async def test_keyword_field_prefilter(marklogic_maindb_driver, text_property):
    resource_id = uuid4().hex
    target = {"resource_id": resource_id, "field_type": "a", "field_id": "summary"}
    async with marklogic_maindb_driver.rw_transaction() as txn:
        await documents.write(
            txn,
            f"/tests/keyword-filter/{resource_id}.json",
            MarkLogicCollections.FIELDS_PROCESSED,
            {
                **target,
                "facets": ["/test/facet"],
                text_property: "[SKU-123:4] Friedrich Nietzsche wrote this treatise.",
            },
        )
        for keyword, expected in [
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
        ]:
            query = SearchRequest()
            operands = query.field_filter.bool_and.operands
            operands.add().resource.resource_id = resource_id
            operands.add().facet.facet = "/test/facet"
            operands.add().CopyFrom(convert_keyword_filter_to_expression(keyword))
            javascript = (
                f"(() => {{ {_filter_context(query)} return fieldMatches({json.dumps(target)}); }})()"
            )
            assert await documents.evaluate(txn, javascript) == [expected], keyword
