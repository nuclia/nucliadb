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
from nucliadb_models.search import FeedbackTasks


async def test_kb_management(sdk_async: nucliadb_sdk.NucliaDBAsync):
    await sdk_async.create_knowledge_box(slug="foo")
    kb = await sdk_async.get_knowledge_box_by_slug(slug="foo")
    assert await sdk_async.get_knowledge_box(kbid=kb.uuid)
    kbs = await sdk_async.list_knowledge_boxes()
    assert len(kbs.kbs) > 0
    await sdk_async.delete_knowledge_box(kbid=kb.uuid)


async def test_kb_services(sdk_async: nucliadb_sdk.NucliaDBAsync, kb):
    # Labels
    await sdk_async.set_labelset(kbid=kb.uuid, labelset="foo", title="Bar")
    await sdk_async.get_labelset(kbid=kb.uuid, labelset="foo")
    await sdk_async.get_labelsets(kbid=kb.uuid)
    await sdk_async.delete_labelset(kbid=kb.uuid, labelset="foo")

    # Entities
    await sdk_async.create_entitygroup(kbid=kb.uuid, group="foo")
    await sdk_async.update_entitygroup(kbid=kb.uuid, group="foo", title="bar")
    await sdk_async.get_entitygroups(kbid=kb.uuid)
    await sdk_async.get_entitygroup(kbid=kb.uuid, group="foo")
    await sdk_async.delete_entitygroup(kbid=kb.uuid, group="foo")

    # Vectorsets
    await sdk_async.create_vectorset(kbid=kb.uuid, vectorset="foo", dimension=10)
    await sdk_async.list_vectorsets(kbid=kb.uuid)
    await sdk_async.delete_vectorset(kbid=kb.uuid, vectorset="foo")


async def test_resource_endpoints(sdk_async: nucliadb_sdk.NucliaDBAsync, kb):
    # Create, Get, List, Update
    await sdk_async.create_resource(kbid=kb.uuid, title="Resource", slug="resource")
    resource = await sdk_async.get_resource_by_slug(kbid=kb.uuid, slug="resource")
    await sdk_async.get_resource_by_id(kbid=kb.uuid, rid=resource.id)
    resources = await sdk_async.list_resources(kbid=kb.uuid)
    assert len(resources.resources) == 1
    await sdk_async.update_resource(kbid=kb.uuid, rid=resource.id, title="Resource2")
    await sdk_async.update_resource_by_slug(
        kbid=kb.uuid, rslug="resource", title="Resource3"
    )

    # Reindex / Reprocess
    await sdk_async.reindex_resource(kbid=kb.uuid, rid=resource.id)
    await sdk_async.reindex_resource_by_slug(kbid=kb.uuid, slug="resource")
    await sdk_async.reprocess_resource(kbid=kb.uuid, rid=resource.id)
    await sdk_async.reprocess_resource_by_slug(kbid=kb.uuid, slug="resource")

    # Delete
    await sdk_async.delete_resource_by_slug(kbid=kb.uuid, rslug="resource")

    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        await sdk_async.delete_resource(kbid=kb.uuid, rid=resource.id)


async def test_search_endpoints(sdk_async: nucliadb_sdk.NucliaDBAsync, kb):
    await sdk_async.find(kbid=kb.uuid, query="foo")
    await sdk_async.search(kbid=kb.uuid, query="foo")
    await sdk_async.chat(kbid=kb.uuid, query="foo")

    resource = await sdk_async.create_resource(
        kbid=kb.uuid, title="Resource", slug="resource"
    )
    await sdk_async.chat_on_resource(kbid=kb.uuid, rid=resource.uuid, query="foo")
    await sdk_async.feedback(
        kbid=kb.uuid, ident="bar", good=True, feedback="baz", task=FeedbackTasks.CHAT
    )
    await sdk_async.summarize(kbid=kb.uuid, resources=["foobar"])


async def test_learning_config_endpoints(sdk_async: nucliadb_sdk.NucliaDB, kb):
    await sdk_async.set_configuration(kbid=kb.uuid, content={"foo": "bar"})
    await sdk_async.get_configuration(kbid=kb.uuid)
    iterator = sdk_async.download_model(kbid=kb.uuid, model_id="foo", filename="bar")
    async for _ in iterator():
        pass
    await sdk_async.get_models(kbid=kb.uuid)
    await sdk_async.get_model(kbid=kb.uuid, model_id="foo")
    await sdk_async.get_configuration_schema(kbid=kb.uuid)
