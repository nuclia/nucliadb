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
