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
import httpx
import pytest

import nucliadb_sdk
from nucliadb_models.conversation import InputMessage, InputMessageContent
from nucliadb_models.synonyms import KnowledgeBoxSynonyms


def test_constructor():
    # Using the region enum still works, although deprecated
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
    sdk.get_entitygroups(kbid=kb.uuid)
    # sdk.get_entitygroup(kbid=kb.uuid, group="foo")

    # Synonyms
    synonyms = KnowledgeBoxSynonyms(synonyms={"foo": ["bar"]})
    sdk.set_custom_synonyms(kbid=kb.uuid, content=synonyms)
    assert sdk.get_custom_synonyms(kbid=kb.uuid) == synonyms


def test_resource_endpoints(sdk: nucliadb_sdk.NucliaDB, kb):
    # Create, Get, List, Update
    sdk.create_resource(kbid=kb.uuid, title="Resource", slug="resource")
    resource = sdk.get_resource_by_slug(kbid=kb.uuid, slug="resource")
    sdk.get_resource_by_id(kbid=kb.uuid, rid=resource.id)
    resources = sdk.list_resources(kbid=kb.uuid)
    assert len(resources.resources) == 1
    sdk.catalog(kbid=kb.uuid, query="foo")
    sdk.update_resource(kbid=kb.uuid, rid=resource.id, title="Resource2")
    sdk.update_resource_by_slug(kbid=kb.uuid, rslug="resource", title="Resource3")
    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        sdk.head_resource(kbid=kb.uuid, rid="nonexistent")
    sdk.head_resource(kbid=kb.uuid, rid=resource.id)
    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        sdk.head_resource_by_slug(kbid=kb.uuid, slug="nonexistent")
    sdk.head_resource_by_slug(kbid=kb.uuid, slug="resource")

    # Reindex / Reprocess
    sdk.reindex_resource(kbid=kb.uuid, rid=resource.id)
    sdk.reindex_resource_by_slug(kbid=kb.uuid, slug="resource")
    sdk.reprocess_resource(kbid=kb.uuid, rid=resource.id)
    sdk.reprocess_resource_by_slug(kbid=kb.uuid, slug="resource")

    # Delete
    sdk.delete_resource_by_slug(kbid=kb.uuid, rslug="resource")
    try:
        sdk.delete_resource(kbid=kb.uuid, rid=resource.id)
    except nucliadb_sdk.exceptions.NotFoundError:
        pass


@pytest.mark.parametrize("method", ["add_conversation_message", "add_conversation_message_by_slug"])
def test_conversation(sdk: nucliadb_sdk.NucliaDB, kb, method: str):
    kbid = kb.uuid
    resource = sdk.create_resource(kbid=kbid, title="Resource", slug="myslug")
    rid = resource.uuid
    messages = [
        InputMessage(  # type: ignore
            ident="1",
            content=InputMessageContent(
                text="Hello",
            ),
        )
    ]
    fid = "conv"

    if method == "add_conversation_message":
        sdk.add_conversation_message(kbid=kbid, rid=rid, field_id=fid, content=messages)
    elif method == "add_conversation_message_by_slug":
        sdk.add_conversation_message_by_slug(kbid=kbid, slug="myslug", field_id=fid, content=messages)

    field = sdk.get_resource_field(
        kbid=kbid, rid=rid, field_type="conversation", field_id=fid, query_params={"page": 1}
    )
    assert field.field_id == fid
    assert field.field_type == "conversation"
    assert field.value.messages[0].ident == "1"
    assert field.value.messages[0].content.text == "Hello"

    field = sdk.get_resource_field_by_slug(
        kbid=kbid, slug="myslug", field_type="conversation", field_id=fid, query_params={"page": 1}
    )
    assert field.field_id == fid
    assert field.field_type == "conversation"
    assert field.value.messages[0].ident == "1"
    assert field.value.messages[0].content.text == "Hello"


def test_search_endpoints(sdk: nucliadb_sdk.NucliaDB, kb):
    sdk.find(kbid=kb.uuid, query="foo")
    sdk.search(kbid=kb.uuid, query="foo")
    sdk.ask(kbid=kb.uuid, query="foo")

    resource = sdk.create_resource(kbid=kb.uuid, title="Resource", slug="resource")
    sdk.ask_on_resource(kbid=kb.uuid, rid=resource.uuid, query="foo")
    sdk.ask_on_resource_by_slug(kbid=kb.uuid, slug="resource", query="foo")
    sdk.feedback(kbid=kb.uuid, ident="bar", good=True, feedback="baz", task="CHAT")
    with pytest.raises(nucliadb_sdk.v2.exceptions.UnknownError) as err:
        sdk.summarize(kbid=kb.uuid, resources=["foobar"])
    assert "Could not summarize" in str(err.value)


def test_learning_config_endpoints(sdk: nucliadb_sdk.NucliaDB, kb):
    sdk.set_configuration(kbid=kb.uuid, content={"foo": "bar"})
    sdk.get_configuration(kbid=kb.uuid)
    sdk.update_configuration(kbid=kb.uuid, content={"foo": "baz"})
    sdk.download_model(kbid=kb.uuid, model_id="foo", filename="bar")
    sdk.get_models(kbid=kb.uuid)
    sdk.get_model(kbid=kb.uuid, model_id="foo")
    sdk.get_configuration_schema(kbid=kb.uuid)
    sdk.get_generative_providers(kbid=kb.uuid)


def test_check_response():
    sdk = nucliadb_sdk.NucliaDB(region="europe-1")

    response = httpx.Response(200)
    assert sdk._check_response(response) is response

    response = httpx.Response(299)
    assert sdk._check_response(response) is response

    with pytest.raises(nucliadb_sdk.exceptions.UnknownError) as err:
        sdk._check_response(httpx.Response(300, text="foo"))
        assert str(err.value) == "Unknown error connecting to API: 300: foo"

    for status_code in (401, 403):
        with pytest.raises(nucliadb_sdk.exceptions.AuthError) as err:
            sdk._check_response(httpx.Response(status_code, text="foo"))
            assert str(err.value) == f"Auth error {status_code}: foo"

    with pytest.raises(nucliadb_sdk.exceptions.AccountLimitError) as err:
        sdk._check_response(httpx.Response(402, text="foo"))
        assert str(err.value) == f"Account limits exceeded error {status_code}: foo"

    with pytest.raises(nucliadb_sdk.exceptions.RateLimitError) as err:
        sdk._check_response(httpx.Response(429, json={"detail": {"try_after": 1}}, text="Rate limit!"))
        assert str(err.value) == f"Rate limit!"
        assert err.value.try_after == 1

    for status_code in (409, 419):
        with pytest.raises(nucliadb_sdk.exceptions.ConflictError) as err:
            sdk._check_response(httpx.Response(status_code, text="foo"))
            assert str(err.value) == "foo"

    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError) as err:
        sdk._check_response(
            httpx.Response(
                404, text="foo", request=httpx.Request(method="GET", url=httpx.URL("http://url"))
            ),
        )
        assert str(err.value) == "Resource not found at http://url: foo"
