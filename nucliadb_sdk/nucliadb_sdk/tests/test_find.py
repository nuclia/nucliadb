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
import nucliadb_sdk
from nucliadb_models.search import KnowledgeboxFindResults, ResourceFindResults


def test_find_on_kb(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    results: KnowledgeboxFindResults = sdk.find(kbid=docs_dataset, query="love")
    assert results.total == 10
    paragraphs = 0
    for res in results.resources.values():
        for field in res.fields.values():
            paragraphs += len(field.paragraphs)
    assert paragraphs == 10


def test_find_on_resource_by_id(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    rid = sdk.list_resources(kbid=docs_dataset).resources[0].id
    results: ResourceFindResults = sdk.find_on_resource_by_id(
        kbid=docs_dataset, rid=rid, query="love"
    )
    paragraphs = 0
    for field in results.resource.fields.values():
        paragraphs += len(field.paragraphs)
    assert paragraphs > 0


def test_find_on_resource_by_slug(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    rslug = sdk.list_resources(kbid=docs_dataset).resources[0].slug
    results: ResourceFindResults = sdk.find_on_resource_by_slug(
        kbid=docs_dataset, rslug=rslug, query="love"
    )
    paragraphs = 0
    for field in results.resource.fields.values():
        paragraphs += len(field.paragraphs)
    assert paragraphs > 0
