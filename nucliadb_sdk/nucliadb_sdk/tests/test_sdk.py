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

import nucliadb_sdk


def test_constructor():
    # Using the region enum
    ndb = nucliadb_sdk.NucliaDB(region=nucliadb_sdk.Region.EUROPE1)
    assert ndb.base_url == "https://europe-1.nuclia.cloud/api"

    # Strings should be accepted too
    ndb = nucliadb_sdk.NucliaDB(region="europe-1")
    assert ndb.base_url == "https://europe-1.nuclia.cloud/api"

    # Unknown region should not fail
    ndb = nucliadb_sdk.NucliaDB(region="foo")
    assert ndb.base_url == "https://foo.nuclia.cloud/api"


def test_kb_management(sdk: nucliadb_sdk.NucliaDB):
    sdk.create_knowledge_box(slug="foo")
    kb = sdk.get_knowledge_box_by_slug(slug="foo")
    assert sdk.get_knowledge_box(kbid=kb.uuid)
    kbs = sdk.list_knowledge_boxes()
    assert len(kbs.kbs) > 0
    sdk.delete_knowledge_box(kbid=kb.uuid)


def test_kb_services(sdk: nucliadb_sdk.NucliaDB, kb):
    # Labels
    sdk.set_labelset(kbid=kb.uuid, labelset="foo", title="Bar")
    sdk.get_labelset(kbid=kb.uuid, labelset="foo")
    sdk.get_labelsets(kbid=kb.uuid)
    sdk.delete_labelset(kbid=kb.uuid, labelset="foo")

    # Entities
    sdk.create_entitygroup(kbid=kb.uuid, group="foo")
    sdk.update_entitygroup(kbid=kb.uuid, group="foo", title="bar")
    sdk.get_entitygroups(kbid=kb.uuid)
    sdk.get_entitygroup(kbid=kb.uuid, group="foo")
    sdk.delete_entitygroup(kbid=kb.uuid, group="foo")

    # Vectorsets
    sdk.create_vectorset(kbid=kb.uuid, vectorset="foo", dimension=10)
    sdk.list_vectorsets(kbid=kb.uuid)
    sdk.delete_vectorset(kbid=kb.uuid, vectorset="foo")


def test_resource_endpoints(sdk: nucliadb_sdk.NucliaDB, kb):
    # Create, Get, List, Update
    sdk.create_resource(kbid=kb.uuid, title="Resource", slug="resource")
    resource = sdk.get_resource_by_slug(kbid=kb.uuid, slug="resource")
    sdk.get_resource_by_id(kbid=kb.uuid, rid=resource.id)
    resources = sdk.list_resources(kbid=kb.uuid)
    assert len(resources.resources) == 1
    sdk.update_resource(kbid=kb.uuid, rid=resource.id, title="Resource2")
    sdk.update_resource_by_slug(kbid=kb.uuid, rslug="resource", title="Resource3")

    # Reindex / Reprocess
    sdk.reindex_resource(kbid=kb.uuid, rid=resource.id)
    sdk.reindex_resource_by_slug(kbid=kb.uuid, slug="resource")
    sdk.reprocess_resource(kbid=kb.uuid, rid=resource.id)
    sdk.reprocess_resource_by_slug(kbid=kb.uuid, slug="resource")

    # Delete
    sdk.delete_resource_by_slug(kbid=kb.uuid, rslug="resource")
    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        sdk.delete_resource(kbid=kb.uuid, rid=resource.id)


def test_search_endpoints(sdk: nucliadb_sdk.NucliaDB, kb):
    sdk.find(kbid=kb.uuid, query="foo")
    sdk.search(kbid=kb.uuid, query="foo")
    sdk.chat(kbid=kb.uuid, query="foo")

    resource = sdk.create_resource(kbid=kb.uuid, title="Resource", slug="resource")
    sdk.chat_on_resource(kbid=kb.uuid, rid=resource.uuid, query="foo")
    sdk.feedback(kbid=kb.uuid, ident="bar", good=True, feedback="baz", task="CHAT")
    sdk.summarize(kbid=kb.uuid, resources=["foobar"])


def test_learning_config_endpoints(sdk: nucliadb_sdk.NucliaDB, kb):
    sdk.set_configuration(kbid=kb.uuid, content={"foo": "bar"})
    sdk.get_configuration(kbid=kb.uuid)
    sdk.download_model(kbid=kb.uuid, model_id="foo", filename="bar")
    sdk.get_models(kbid=kb.uuid)
    sdk.get_model(kbid=kb.uuid, model_id="foo")
    sdk.get_configuration_schema(kbid=kb.uuid)
