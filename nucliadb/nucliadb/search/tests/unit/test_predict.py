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
import asyncio
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, Mock

import aiohttp
import pytest
from nucliadb_protos.knowledgebox_pb2 import KBConfiguration
from yarl import URL

from nucliadb.search.predict import (
    DummyPredictEngine,
    PredictEngine,
    PredictVectorMissing,
    ProxiedPredictAPIError,
    RephraseError,
    RephraseMissingContextError,
    SendToPredictError,
    _parse_rephrase_response,
    get_answer_generator,
)
from nucliadb.tests.utils.aiohttp_session import get_mocked_session
from nucliadb_models.search import (
    AskDocumentModel,
    ChatModel,
    FeedbackRequest,
    FeedbackTasks,
    RephraseModel,
    SummarizedResource,
    SummarizedResponse,
    SummarizeModel,
    SummarizeResourceModel,
)
from nucliadb_utils.exceptions import LimitsExceededError


@pytest.mark.asyncio
async def test_dummy_predict_engine():
    pe = DummyPredictEngine()
    await pe.initialize()
    await pe.finalize()
    await pe.send_feedback("kbid", Mock(), "", "", "")
    assert await pe.rephrase_query("kbid", Mock())
    assert await pe.chat_query("kbid", Mock())
    assert await pe.convert_sentence_to_vector("kbid", "some sentence")
    assert await pe.detect_entities("kbid", "some sentence")
    assert await pe.ask_document("kbid", "query", [["footext"]], "userid")
    assert await pe.summarize("kbid", Mock(resources={}))


@pytest.fixture(scope="function", autouse=True)
def txn():
    txn = mock.MagicMock()
    txn.get = AsyncMock(return_value=None)
    with mock.patch("nucliadb.search.predict.get_transaction", return_value=txn):
        yield txn


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "onprem,expected_url,expected_header,expected_header_value",
    [
        (
            True,
            "{public_url}/api/v1/predict/sentence",
            "X-STF-NUAKEY",
            "Bearer {service_account}",
        ),
        (False, "{cluster}/api/internal/predict/sentence", "X-STF-KBID", "{kbid}"),
    ],
)
async def test_convert_sentence_ok(
    onprem, expected_url, expected_header, expected_header_value
):
    service_account = "service-account"

    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        service_account,
        zone="zone1",
        onprem=onprem,
    )

    pe.session = get_mocked_session(
        "GET", 200, json={"data": [0.0, 0.1]}, context_manager=False
    )

    kbid = "kbid"
    sentence = "some sentence"

    assert await pe.convert_sentence_to_vector(kbid, sentence) == [0.0, 0.1]

    path = expected_url.format(public_url=pe.public_url, cluster=pe.cluster_url)

    headers = {
        expected_header: expected_header_value.format(
            kbid=kbid, service_account=service_account
        )
    }
    pe.session.get.assert_awaited_once_with(
        url=path,
        params={"text": sentence},
        headers=headers,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("onprem", [True, False])
async def test_convert_sentence_error(onprem):
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        "service-account",
        onprem=onprem,
    )
    pe.session = get_mocked_session("GET", 400, json="uops!", context_manager=False)
    with pytest.raises(ProxiedPredictAPIError):
        await pe.convert_sentence_to_vector("kbid", "some sentence")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "onprem,expected_url,expected_header,expected_header_value",
    [
        (
            True,
            "{public_url}/api/v1/predict/tokens",
            "X-STF-NUAKEY",
            "Bearer {service_account}",
        ),
        (False, "{cluster}/api/internal/predict/tokens", "X-STF-KBID", "{kbid}"),
    ],
)
async def test_detect_entities_ok(
    onprem, expected_url, expected_header, expected_header_value
):
    cluster_url = "cluster"
    public_url = "public-{zone}"
    service_account = "service-account"
    zone = "zone1"

    pe = PredictEngine(
        cluster_url,
        public_url,
        service_account,
        zone=zone,
        onprem=onprem,
    )
    pe.session = get_mocked_session(
        "GET",
        200,
        json={"tokens": [{"text": "foo", "ner": "bar"}]},
        context_manager=False,
    )

    kbid = "kbid"
    sentence = "some sentence"

    assert len(await pe.detect_entities(kbid, sentence)) > 0

    path = expected_url.format(public_url=pe.public_url, cluster=pe.cluster_url)

    headers = {
        expected_header: expected_header_value.format(
            kbid=kbid, service_account=service_account
        )
    }
    pe.session.get.assert_awaited_once_with(
        url=path,
        params={"text": sentence},
        headers=headers,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("onprem", [True, False])
async def test_detect_entities_error(onprem):
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        "service-account",
        onprem=onprem,
    )
    pe.session = get_mocked_session("GET", 500, json="error", context_manager=False)
    with pytest.raises(ProxiedPredictAPIError):
        await pe.detect_entities("kbid", "some sentence")


@pytest.fixture(scope="function")
def session_limits_exceeded():
    session = AsyncMock()
    resp = Mock(status=402)
    resp.json = AsyncMock(return_value={"detail": "limits exceeded"})
    resp.read = AsyncMock(return_value="something went wrong")
    session.post.return_value = resp
    session.get.return_value = resp
    return session


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,args",
    [
        ("convert_sentence_to_vector", ["kbid", "sentence"]),
        ("detect_entities", ["kbid", "sentence"]),
        ("chat_query", ["kbid", ChatModel(question="foo", user_id="bar")]),
        (
            "send_feedback",
            [
                "kbid",
                FeedbackRequest(ident="foo", good=True, task=FeedbackTasks.CHAT),
                "",
                "",
                "",
            ],
        ),
        ("rephrase_query", ["kbid", RephraseModel(question="foo", user_id="bar")]),
        ("ask_document", ["kbid", "query", [["footext"]], "userid"]),
    ],
)
async def test_predict_engine_handles_limits_exceeded_error(
    session_limits_exceeded, method, args
):
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        "service-account",
        onprem=True,
    )
    pe.session = session_limits_exceeded
    with pytest.raises(LimitsExceededError):
        await getattr(pe, method)(*args)


@pytest.mark.parametrize(
    "method,args,exception,output",
    [
        ("chat_query", ["kbid", Mock()], True, None),
        ("rephrase_query", ["kbid", Mock()], True, None),
        ("send_feedback", ["kbid", MagicMock(), "", "", ""], False, None),
        ("convert_sentence_to_vector", ["kbid", "sentence"], False, []),
        ("detect_entities", ["kbid", "sentence"], False, []),
        ("ask_document", ["kbid", "query", [["footext"]], "userid"], True, None),
        ("summarize", ["kbid", Mock(resources={})], True, None),
    ],
)
async def test_onprem_nuclia_service_account_not_configured(
    method, args, exception, output
):
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        nuclia_service_account=None,
        onprem=True,
    )
    if exception:
        with pytest.raises(SendToPredictError):
            await getattr(pe, method)(*args)
    else:
        assert await getattr(pe, method)(*args) == output


async def test_convert_sentence_to_vector_empty_vectors():
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        nuclia_service_account="foo",
        onprem=True,
    )
    pe.session = get_mocked_session(
        "GET", 200, json={"data": []}, context_manager=False
    )
    with pytest.raises(PredictVectorMissing):
        await pe.convert_sentence_to_vector("kbid", "sentence")


async def test_ask_document_onprem():
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        nuclia_service_account="foo",
        zone="europe1",
        onprem=True,
    )
    pe.session = get_mocked_session(
        "POST", 200, text="The answer", context_manager=False
    )

    assert (
        await pe.ask_document("kbid", "query", [["footext"]], "userid") == "The answer"
    )

    pe.session.post.assert_awaited_once_with(
        url="public-europe1/api/v1/predict/ask_document",
        json=AskDocumentModel(
            question="query", blocks=[["footext"]], user_id="userid"
        ).dict(),
        headers={"X-STF-NUAKEY": "Bearer foo"},
        timeout=None,
    )


async def test_ask_document_cloud():
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        zone="europe1",
        onprem=False,
    )
    pe.session = get_mocked_session(
        "POST", 200, text="The answer", context_manager=False
    )

    assert (
        await pe.ask_document("kbid", "query", [["footext"]], "userid") == "The answer"
    )

    pe.session.post.assert_awaited_once_with(
        url="cluster/api/internal/predict/ask_document",
        json=AskDocumentModel(
            question="query", blocks=[["footext"]], user_id="userid"
        ).dict(),
        headers={"X-STF-KBID": "kbid"},
        timeout=None,
    )


async def test_rephrase():
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        zone="europe1",
        onprem=False,
    )
    pe.session = get_mocked_session(
        "POST", 200, json="rephrased", context_manager=False
    )

    item = RephraseModel(
        question="question", chat_history=[], user_id="foo", user_context=["foo"]
    )
    rephrased_query = await pe.rephrase_query("kbid", item)
    # The rephrase query should not be wrapped in quotes, otherwise it will trigger an exact match query to the index
    assert rephrased_query.strip('"') == rephrased_query
    assert rephrased_query == "rephrased"

    pe.session.post.assert_awaited_once_with(
        url="cluster/api/internal/predict/rephrase",
        json=item.dict(),
        headers={"X-STF-KBID": "kbid"},
    )


@pytest.mark.parametrize(
    "content,exception",
    [
        ("foobar", None),
        ("foobar0", None),
        ("foobar-1", RephraseError),
        ("foobar-2", RephraseMissingContextError),
    ],
)
async def test_parse_rephrase_response(content, exception):
    resp = Mock()
    resp.json = AsyncMock(return_value=content)
    if exception:
        with pytest.raises(exception):
            await _parse_rephrase_response(resp)
    else:
        assert await _parse_rephrase_response(resp) == content.rstrip("0")


async def test_check_response_error():
    response = aiohttp.ClientResponse(
        "GET",
        URL("http://predict:8080/api/v1/chat"),
        writer=None,
        continue100=Mock(),
        timer=Mock(),
        request_info=Mock(),
        traces=[],
        loop=Mock(),
        session=Mock(),
    )
    response.status = 503
    response._body = b"some error"
    response._headers = {"Content-Type": "text/plain; charset=utf-8"}

    with pytest.raises(ProxiedPredictAPIError) as ex:
        await PredictEngine().check_response(response, expected_status=200)
    assert ex.value.status == 503
    assert ex.value.detail == "some error"


async def test_summarize():
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        zone="europe1",
        onprem=False,
    )

    summarized = SummarizedResponse(
        resources={"r1": SummarizedResource(summary="resource summary", tokens=10)}
    )
    pe.session = get_mocked_session(
        "POST", 200, json=summarized.dict(), context_manager=False
    )

    item = SummarizeModel(
        resources={"r1": SummarizeResourceModel(fields={"f1": "field extracted text"})}
    )
    summarize_response = await pe.summarize("kbid", item)

    assert summarize_response == summarized

    pe.session.post.assert_awaited_once_with(
        url="cluster/api/internal/predict/summarize",
        json=item.dict(),
        headers={"X-STF-KBID": "kbid"},
        timeout=None,
    )


@pytest.mark.parametrize("onprem", [True, False])
async def test_get_predict_headers(onprem, txn):
    kb_config = KBConfiguration(
        semantic_model="foo",
        generative_model="bar",
        ner_model="baz",
        anonymization_model="qux",
        visual_labeling="quux",
    )
    txn.get.return_value = kb_config.SerializeToString()

    nua_service_account = "nua-service-account"
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        zone="europe1",
        onprem=onprem,
        nuclia_service_account=nua_service_account,
    )
    predict_headers = await pe.get_predict_headers("kbid")
    if onprem:
        assert predict_headers["X-STF-NUAKEY"] == f"Bearer {pe.nuclia_service_account}"
        assert predict_headers["X-STF-SEMANTIC-MODEL"] == kb_config.semantic_model
        assert predict_headers["X-STF-GENERATIVE-MODEL"] == kb_config.generative_model
        assert predict_headers["X-STF-NER-MODEL"] == kb_config.ner_model
        assert (
            predict_headers["X-STF-ANONYMIZATION-MODEL"]
            == kb_config.anonymization_model
        )
        assert predict_headers["X-STF-VISUAL-LABELING"] == kb_config.visual_labeling
    else:
        assert predict_headers == {"X-STF-KBID": "kbid"}


async def test_get_answer_generator():
    async def _iter_chunks():
        await asyncio.sleep(0.1)
        # Chunk, end_of_chunk
        yield b"foo", False
        yield b"bar", True
        yield b"baz", True

    resp = Mock()
    resp.content.iter_chunks = Mock(return_value=_iter_chunks())
    get_answer_generator(resp)

    answer_chunks = [chunk async for chunk in get_answer_generator(resp)]
    assert answer_chunks == [b"foobar", b"baz"]
