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
"""Embedded single-hop graph data and its relation-occurrence TDE view."""

import json
import unicodedata

from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic_schema import SCHEMAS_DATABASE

SCHEMA = "nucliadb"
VIEW = "relations"
TEMPLATE_URI = "/nucliadb/tde/relations-v1.json"


def normalize(value: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(character)
    ).lower()


def relation_content(index_relation, resource_field_id: str, occurrence: int) -> dict:
    from nucliadb.common.marklogic.documents import to_json

    relation = index_relation.relation
    return {
        "resource_field_id": resource_field_id,
        "occurrence": occurrence,
        "source_value": relation.source.value,
        "source_normalized": normalize(relation.source.value),
        "source_type": relation.source.ntype,
        "source_group": relation.source.subtype,
        "destination_value": relation.to.value,
        "destination_normalized": normalize(relation.to.value),
        "destination_type": relation.to.ntype,
        "destination_group": relation.to.subtype,
        "label": relation.relation_label,
        "relation_type": relation.relation,
        "facets_json": json.dumps(list(index_relation.facets)),
        "metadata_json": json.dumps(to_json(relation.metadata)),
    }


def template() -> dict:
    integers = {"source_type", "destination_type", "relation_type", "occurrence"}
    names = [
        "resource_field_id",
        "occurrence",
        "source_value",
        "source_normalized",
        "source_type",
        "source_group",
        "destination_value",
        "destination_normalized",
        "destination_type",
        "destination_group",
        "label",
        "relation_type",
        "facets_json",
        "metadata_json",
    ]
    return {
        "template": {
            "context": "/relations",
            "collections": [MarkLogicCollections.FIELDS_PROCESSED, MarkLogicCollections.RESOURCES],
            "rows": [
                {
                    "schemaName": SCHEMA,
                    "viewName": VIEW,
                    "viewLayout": "sparse",
                    "columns": [
                        {
                            "name": name,
                            "scalarType": "int" if name in integers else "string",
                            "val": name,
                        }
                        for name in names
                    ],
                }
            ],
        }
    }


async def ensure_view(driver, database: str) -> None:
    uri = json.dumps(TEMPLATE_URI)
    existing = await driver.client.eval(
        f"fn.docAvailable({uri}) ? fn.doc({uri}).toObject() : null",
        params={"database": SCHEMAS_DATABASE},
    )
    if existing and existing[0] == template():
        return
    await driver.client.eval(
        "declareUpdate(); const tde = require('/MarkLogic/tde.xqy'); "
        f"tde.templateInsert({uri}, xdmp.toJSON({json.dumps(template())}), "
        "xdmp.defaultPermissions(), ['tde']);",
        params={"database": database},
    )
