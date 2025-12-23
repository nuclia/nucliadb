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

import asyncio
import json

from httpx import AsyncClient

from nucliadb_telemetry import grpc_metrics
from nucliadb_telemetry.jetstream import msg_consume_time_histo
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.tests.telemetry import Greeter


def fmt_span(span):
    tags_by_key = {tag["key"]: tag["value"] for tag in span["tags"]}
    return {
        "time": span["startTime"],
        "id": span["spanID"],
        "parent": span["references"][0]["spanID"],
        "process": span["processID"],
        "scope": tags_by_key["otel.scope.name"],
        "operation": span["operationName"],
    }


def order_spans(spans):
    return sorted([fmt_span(span) for span in spans], key=lambda x: x["time"])


def debug_spans(spans):
    print(json.dumps(spans, indent=4))


async def test_telemetry_dict(http_service: AsyncClient, greeter: Greeter):
    resp = await http_service.get(
        "http://test/",
        headers={
            "x-b3-traceid": "f13dc5318bf3bef64a0a5ea607db93a1",
            "x-b3-spanid": "bfc2225c60b39d97",
            "x-b3-sampled": "1",
        },
    )
    assert resp.status_code == 200

    # Check that trace ids are returned in response headers
    assert resp.headers["X-NUCLIA-TRACE-ID"]
    assert resp.headers["X-NUCLIA-TRACE-ID"] != "0"
    assert "X-NUCLIA-TRACE-ID" in resp.headers["Access-Control-Expose-Headers"]

    for i in range(10):
        if len(greeter.messages) == 0:
            await asyncio.sleep(1)
    assert greeter.messages[0].headers["x-b3-traceid"] == "f13dc5318bf3bef64a0a5ea607db93a1"
    assert len(greeter.messages) == 4

    expected_spans = 17

    await asyncio.sleep(2)
    client = AsyncClient()
    for _ in range(10):
        resp = await client.get(
            f"http://localhost:{telemetry_settings.jaeger_query_port}/api/traces/f13dc5318bf3bef64a0a5ea607db93a1",
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200 or len(resp.json()["data"][0]["spans"]) < expected_spans:
            await asyncio.sleep(2)
        else:
            break

    assert resp.json()["data"][0]["traceID"] == "f13dc5318bf3bef64a0a5ea607db93a1"

    ordered_spans = order_spans(resp.json()["data"][0]["spans"])
    # Enable this block for debugging purposes, to see sunmmarized and sorted details of all spans
    # debug_spans(ordered_spans)

    # This order may change if the test is changed a lot, so make sure to enable debug_spans and
    # set the asserts correctly
    api_span = ordered_spans[1]
    grpc_client_span = ordered_spans[2]
    grpc_server_span = ordered_spans[3]

    assert grpc_server_span["parent"] != grpc_client_span["parent"]
    assert grpc_server_span["parent"] == grpc_client_span["id"]
    assert grpc_client_span["parent"] == api_span["id"]

    assert len(resp.json()["data"][0]["spans"]) == expected_spans
    assert len(resp.json()["data"][0]["processes"]) == 3

    assert grpc_metrics.grpc_client_observer.histogram.collect()[0].samples  # type: ignore
    assert grpc_metrics.grpc_server_observer.histogram.collect()[0].samples  # type: ignore

    assert msg_consume_time_histo.histo.collect()[0].samples  # type: ignore

    sample = next(
        sam.labels
        for sam in msg_consume_time_histo.histo.collect()[0].samples  # type: ignore
        if sam.labels.get("le") == "0.005"
    )
    sample.pop("consumer")
    assert sample == {"stream": "testing", "acked": "no", "le": "0.005"}
