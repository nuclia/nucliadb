import asyncio
import math
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import nats
import pytest
from httpx import AsyncClient
from nats.aio.client import Client
from nats.js import JetStreamContext
from nucliadb_protos.audit_pb2 import AuditRequest, ClientType
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.ingest.tests.vectors import V1
from nucliadb.search.predict import PredictVectorMissing, SendToPredictError
from nucliadb.search.search.query import pre_process_query
from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.utilities import Utility, clean_utility, get_audit, set_utility


@pytest.mark.asyncio
async def test_find_with_label_changes(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    await asyncio.sleep(1)

    # should get 1 result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    # assert we get no results with label filter
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "filters": ["/l/labels/label1"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0

    # add new label
    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{rid}",
        json={
            # "title": "My new title",
            "usermetadata": {
                "classifications": [
                    {
                        "labelset": "labels",
                        "label": "label1",
                        "cancelled_by_user": False,
                    }
                ],
                "relations": [],
            }
        },
        headers={"X-SYNCHRONOUS": "True"},
        timeout=None,
    )
    assert resp.status_code == 200
    await asyncio.sleep(1)

    # we should get 1 result now with updated label
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "filters": ["/l/labels/label1"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1
