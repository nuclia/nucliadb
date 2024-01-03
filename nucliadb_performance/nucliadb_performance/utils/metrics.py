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

import json
import os
import statistics as stats
from typing import Optional

import aiohttp

PROCESS_TIME_HEADER = "X-PROCESS-TIME"

METRICS: dict[str, list[float]] = {}


def record_sample(metric_name, sample):
    METRICS.setdefault(metric_name, []).append(sample)


def get_samples(metric_name):
    return METRICS.get(metric_name, [])


def record_request_process_time(
    resp: aiohttp.ClientResponse,
    *,
    client_time: float,
    metric_name: Optional[str] = None,
):
    if metric_name is None:
        # Get endpoint name from url
        endpoint = resp.url.path.split("/")[-1]
        metric_name = endpoint
    metric_name = f"{metric_name}_latency_s"
    server_request_process_time = resp.headers.get(PROCESS_TIME_HEADER, None)
    if server_request_process_time is None:
        record_sample(metric_name, client_time)
    else:
        record_sample(metric_name, float(server_request_process_time))


def get_percentile(metric_name, *, p):
    samples = get_samples(metric_name)
    # Discard the first few samples
    if len(samples) > 100:
        samples = samples[10:]
    quantiles = 1000
    percentile_pos = int(quantiles * p) - 1
    return stats.quantiles(samples, n=quantiles)[percentile_pos]


def print_metrics():
    print("Metrics summary:")
    for metric_name in sorted(METRICS.keys()):
        p50 = get_percentile(metric_name, p=0.5)
        p95 = get_percentile(metric_name, p=0.95)
        print(
            f"- {metric_name} -> p50: {prettify_latency(p50)} p95: {prettify_latency(p95)}"
        )
    print("=" * 50)


def prettify_latency(latency):
    if latency > 1:
        return f"{latency:.2f}s"
    else:
        return f"{(latency * 1000):.2f}ms"


def save_benchmark_json_results(file_path: str):
    json_results = []
    for metric_name in METRICS:
        json_results.append(
            {
                "name": f"{metric_name}_p50",
                "unit": f"s",
                "value": get_percentile(metric_name, p=0.5),
            }
        )
        json_results.append(
            {
                "name": f"{metric_name}_p95",
                "unit": f"s",
                "value": get_percentile(metric_name, p=0.95),
            }
        )

    real_path = os.path.realpath(file_path)
    print(f"Saving benchmark results to {real_path}")
    with open(real_path, mode="w") as f:
        f.write(json.dumps(json_results, indent=4, sort_keys=True))
