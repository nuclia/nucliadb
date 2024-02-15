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
from unittest.mock import Mock

import pytest
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb.ingest.processing import (
    DummyProcessingEngine,
    ProcessingEngine,
    PushPayload,
)
from nucliadb.tests.utils.aiohttp_session import get_mocked_session
from nucliadb_models import File, FileField
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError

TEST_FILE = FileField(
    password="mypassword", file=File(filename="myfile.pdf", payload="")
)

TEST_CLOUD_FILE = CloudFile(
    uri="file.png",
    source=CloudFile.Source.LOCAL,
    bucket_name="/integration/ingest/assets",
    size=4,
    content_type="image/png",
    filename="file.png",
)

TEST_ITEM = PushPayload(uuid="foo", kbid="bar", userid="baz", partition=1)


@pytest.mark.asyncio
async def test_dummy_processing_engine():
    engine = DummyProcessingEngine()
    await engine.initialize()
    await engine.finalize()
    await engine.convert_filefield_to_str(None)
    engine.convert_external_filefield_to_str(None)
    await engine.convert_internal_filefield_to_str(None, None)
    await engine.convert_internal_cf_to_str(None, None)
    await engine.send_to_process(Mock(kbid="foo"), 1)


@pytest.fixture(scope="function")
def engine():
    pe = ProcessingEngine(
        onprem=True,
        nuclia_processing_cluster_url="cluster_url",
        nuclia_public_url="public_url",
    )
    yield pe


async def test_convert_filefield_to_str_200(engine):
    engine.session = get_mocked_session("POST", 200, text="jwt")

    assert await engine.convert_filefield_to_str(TEST_FILE) == "jwt"


async def test_convert_filefield_to_str_402(engine):
    engine.session = get_mocked_session("POST", 402, json={"detail": "limits exceeded"})

    with pytest.raises(LimitsExceededError) as exc:
        await engine.convert_filefield_to_str(TEST_FILE)
    assert exc.value.status_code == 402


async def test_convert_filefield_to_str_429(engine):
    engine.session = get_mocked_session("POST", 429, json={"detail": "limits exceeded"})

    with pytest.raises(LimitsExceededError) as exc:
        await engine.convert_filefield_to_str(TEST_FILE)
    assert exc.value.status_code == 429


async def test_convert_filefield_to_str_500(engine):
    engine.session = get_mocked_session("POST", 500, text="error")

    with pytest.raises(Exception) as exc:
        await engine.convert_filefield_to_str(TEST_FILE)
    assert str(exc.value) == "STATUS: 500 - error"


async def test_convert_internal_cf_to_str_200(engine):
    engine.session = get_mocked_session("POST", 200, text="jwt")

    assert await engine.convert_internal_cf_to_str(TEST_CLOUD_FILE, Mock()) == "jwt"


async def test_convert_internal_cf_to_str_402(engine):
    engine.session = get_mocked_session("POST", 402, json={"detail": "limits exceeded"})

    with pytest.raises(LimitsExceededError) as exc:
        await engine.convert_internal_cf_to_str(TEST_CLOUD_FILE, Mock())
    assert exc.value.status_code == 402


async def test_convert_internal_cf_to_str_429(engine):
    engine.session = get_mocked_session("POST", 429, json={"detail": "limits exceeded"})

    with pytest.raises(LimitsExceededError) as exc:
        await engine.convert_internal_cf_to_str(TEST_CLOUD_FILE, Mock())
    assert exc.value.status_code == 429


async def test_convert_internal_cf_to_str_500(engine):
    engine.session = get_mocked_session("POST", 500, text="error")

    with pytest.raises(Exception) as exc:
        await engine.convert_internal_cf_to_str(TEST_CLOUD_FILE, Mock())
    assert str(exc.value) == "STATUS: 500 - error"


async def test_send_to_process_200(engine):
    json_data = {"seqid": 11, "account_seq": 22, "queue": "private"}
    engine.session = get_mocked_session(
        "POST", 200, json=json_data, context_manager=False
    )

    processing_info = await engine.send_to_process(TEST_ITEM, 1)
    assert processing_info.seqid == 11
    assert processing_info.account_seq == 22
    assert processing_info.queue == "private"


@pytest.mark.parametrize("status", [402, 413])
async def test_send_to_process_limits_exceeded(status, engine):
    engine.session = get_mocked_session(
        "POST", status, json={"detail": "limits exceeded"}, context_manager=False
    )

    with pytest.raises(LimitsExceededError) as exc:
        await engine.send_to_process(TEST_ITEM, 1)
    assert exc.value.status_code == status


async def test_send_to_process_limits_exceeded_429(engine):
    engine.session = get_mocked_session(
        "POST", 429, json={"detail": "limits exceeded"}, context_manager=False
    )

    with pytest.raises(LimitsExceededError) as exc:
        await engine.send_to_process(TEST_ITEM, 1)
    assert exc.value.status_code == 429


async def test_send_to_process_500(engine):
    engine.session = get_mocked_session(
        "POST", 500, text="error", context_manager=False
    )

    with pytest.raises(SendToProcessError):
        await engine.send_to_process(TEST_ITEM, 1)
