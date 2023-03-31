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
from unittest.mock import AsyncMock, Mock

import pytest

from nucliadb.search.predict import PredictEngine, SendToPredictError
from nucliadb_models.search import ChatModel, FeedbackRequest, FeedbackTasks
from nucliadb_utils.exceptions import LimitsExceededError


@pytest.mark.asyncio
async def test_dummy_predict_engine():
    pe = PredictEngine(
        "cluster", "public-{zone}", "service-account", zone="zone1", dummy=True
    )
    assert pe.public_url == "public-zone1"
    await pe.initialize()
    await pe.finalize()
    assert await pe.convert_sentence_to_vector("kbid", "some sentence")
    assert await pe.detect_entities("kbid", "some sentence")


@pytest.fixture(scope="function")
def ok_session():
    vector = [0.0, 0.1]
    tokens = [{"text": "foo", "ner": "bar"}]
    ok_response = Mock(
        status=200, json=AsyncMock(return_value={"data": vector, "tokens": tokens})
    )
    session = AsyncMock()
    session.get.return_value = ok_response
    return session


@pytest.fixture(scope="function")
def error_session():
    error_response = Mock(status=400, read=AsyncMock(return_value="uops!"))
    session = AsyncMock()
    session.get.return_value = error_response
    return session


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
    ok_session, onprem, expected_url, expected_header, expected_header_value
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
        dummy=False,
    )
    pe.session = ok_session

    kbid = "kbid"
    sentence = "some sentence"

    assert await pe.convert_sentence_to_vector(kbid, sentence) == [0.0, 0.1]

    path = expected_url.format(public_url=pe.public_url, cluster=pe.cluster_url)
    url = f"{path}?text={sentence}"

    headers = {
        expected_header: expected_header_value.format(
            kbid=kbid, service_account=service_account
        )
    }
    pe.session.get.assert_awaited_once_with(
        url=url,
        headers=headers,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("onprem", [True, False])
async def test_convert_sentence_error(error_session, onprem):
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        "service-account",
        onprem=onprem,
        dummy=False,
    )
    pe.session = error_session
    with pytest.raises(SendToPredictError):
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
    ok_session, onprem, expected_url, expected_header, expected_header_value
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
        dummy=False,
    )
    pe.session = ok_session

    kbid = "kbid"
    sentence = "some sentence"

    assert len(await pe.detect_entities(kbid, sentence)) > 0

    path = expected_url.format(public_url=pe.public_url, cluster=pe.cluster_url)
    url = f"{path}?text={sentence}"

    headers = {
        expected_header: expected_header_value.format(
            kbid=kbid, service_account=service_account
        )
    }
    pe.session.get.assert_awaited_once_with(
        url=url,
        headers=headers,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("onprem", [True, False])
async def test_detect_entities_error(error_session, onprem):
    pe = PredictEngine(
        "cluster",
        "public-{zone}",
        "service-account",
        onprem=onprem,
        dummy=False,
    )
    pe.session = error_session
    with pytest.raises(SendToPredictError):
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
        dummy=False,
    )
    pe.session = session_limits_exceeded
    with pytest.raises(LimitsExceededError):
        await pe.__getattribute__(method)(*args)
