# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import pytest

import nucliadb_sdk
from nucliadb_models.search import FeedbackTasks
from nucliadb_models.synonyms import KnowledgeBoxSynonyms


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
    await sdk_async.get_entitygroups(kbid=kb.uuid)
    # await sdk_async.get_entitygroup(kbid=kb.uuid, group="foo")

    # Synonyms
    synonyms = KnowledgeBoxSynonyms(synonyms={"foo": ["bar"]})
    await sdk_async.set_custom_synonyms(kbid=kb.uuid, content=synonyms)
    assert (await sdk_async.get_custom_synonyms(kbid=kb.uuid)) == synonyms


async def test_resource_endpoints(sdk_async: nucliadb_sdk.NucliaDBAsync, kb):
    # Create, Get, List, Update
    await sdk_async.create_resource(kbid=kb.uuid, title="Resource", slug="resource")
    resource = await sdk_async.get_resource_by_slug(kbid=kb.uuid, slug="resource")
    await sdk_async.get_resource_by_id(kbid=kb.uuid, rid=resource.id)
    resources = await sdk_async.list_resources(kbid=kb.uuid)
    assert len(resources.resources) == 1
    await sdk_async.catalog(kbid=kb.uuid, query="foo")
    await sdk_async.update_resource(kbid=kb.uuid, rid=resource.id, title="Resource2")
    await sdk_async.update_resource_by_slug(kbid=kb.uuid, rslug="resource", title="Resource3")
    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        await sdk_async.head_resource(kbid=kb.uuid, rid="nonexistent")
    await sdk_async.head_resource(kbid=kb.uuid, rid=resource.id)
    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        await sdk_async.head_resource_by_slug(kbid=kb.uuid, slug="nonexistent")
    await sdk_async.head_resource_by_slug(kbid=kb.uuid, slug="resource")

    # Reindex / Reprocess
    await sdk_async.reindex_resource(kbid=kb.uuid, rid=resource.id)
    await sdk_async.reindex_resource_by_slug(kbid=kb.uuid, slug="resource")
    await sdk_async.reprocess_resource(kbid=kb.uuid, rid=resource.id)
    await sdk_async.reprocess_resource_by_slug(kbid=kb.uuid, slug="resource")

    # Delete
    await sdk_async.delete_resource_by_slug(kbid=kb.uuid, rslug="resource")
    try:
        await sdk_async.delete_resource(kbid=kb.uuid, rid=resource.id)
    except nucliadb_sdk.v2.exceptions.NotFoundError:
        pass


async def test_search_endpoints(sdk_async: nucliadb_sdk.NucliaDBAsync, kb):
    await sdk_async.find(kbid=kb.uuid, query="foo")
    await sdk_async.search(kbid=kb.uuid, query="foo")
    await sdk_async.ask(kbid=kb.uuid, query="foo", headers={"any-header": "any-value"})

    resource = await sdk_async.create_resource(kbid=kb.uuid, title="Resource", slug="resource")
    await sdk_async.ask_on_resource(kbid=kb.uuid, rid=resource.uuid, query="foo")
    await sdk_async.ask_on_resource_by_slug(kbid=kb.uuid, slug="resource", query="foo")
    await sdk_async.feedback(
        kbid=kb.uuid, ident="bar", good=True, feedback="baz", task=FeedbackTasks.CHAT
    )
    with pytest.raises(nucliadb_sdk.v2.exceptions.UnknownError) as err:
        await sdk_async.summarize(kbid=kb.uuid, resources=["foobar"])
    assert "Could not summarize" in str(err.value)


async def test_learning_config_endpoints(sdk_async: nucliadb_sdk.NucliaDB, kb):
    await sdk_async.set_configuration(kbid=kb.uuid, content={"foo": "bar"})
    await sdk_async.get_configuration(kbid=kb.uuid)
    await sdk_async.update_configuration(kbid=kb.uuid, content={"foo": "baz"})
    iterator = sdk_async.download_model(kbid=kb.uuid, model_id="foo", filename="bar")
    async for _ in iterator():
        pass
    await sdk_async.get_models(kbid=kb.uuid)
    await sdk_async.get_model(kbid=kb.uuid, model_id="foo")
    await sdk_async.get_configuration_schema(kbid=kb.uuid)
    await sdk_async.get_generative_providers(kbid=kb.uuid)
