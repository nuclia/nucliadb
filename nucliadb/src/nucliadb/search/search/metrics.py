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
import contextlib
import time
from typing import Any, Optional, Union

from nucliadb_telemetry import metrics

merge_observer = metrics.Observer("merge_results", labels={"type": ""})
node_features = metrics.Counter("nucliadb_node_features", labels={"type": ""})
query_parse_dependency_observer = metrics.Observer("query_parse_dependency", labels={"type": ""})
query_parser_observer = metrics.Observer("nucliadb_query_parser", labels={"type": ""})

buckets = [
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    30.0,
    60.0,
    metrics.INF,
]

generative_first_chunk_histogram = metrics.Histogram(
    name="generative_first_chunk",
    buckets=buckets,
)
rag_histogram = metrics.Histogram(
    name="rag",
    labels={"step": ""},
    buckets=buckets,
)

MetricsData = dict[str, Union[int, float]]


class Metrics:
    def __init__(self: "Metrics", id: str):
        self.id = id
        self.child_spans: list[Metrics] = []
        self._metrics: MetricsData = {}

    @contextlib.contextmanager
    def time(self, step: str):
        start_time = time.monotonic()
        try:
            yield
        finally:
            elapsed = time.monotonic() - start_time
            self._metrics[step] = elapsed
            rag_histogram.observe(elapsed, labels={"step": step})

    def child_span(self, id: str) -> "Metrics":
        child_span = Metrics(id)
        self.child_spans.append(child_span)
        return child_span

    def set(self, key: str, value: Union[int, float]):
        self._metrics[key] = value

    def get(self, key: str) -> Optional[Union[int, float]]:
        return self._metrics.get(key)

    def to_dict(self) -> MetricsData:
        return self._metrics

    def dump(self) -> dict[str, Any]:
        result = {}
        for child in self.child_spans:
            result.update(child.dump())
        result[self.id] = self.to_dict()
        return result

    def __getitem__(self, key: str) -> Union[int, float]:
        return self._metrics[key]


class AskMetrics(Metrics):
    def __init__(self: "AskMetrics"):
        super().__init__(id="ask")
        self.global_start = time.monotonic()
        self.first_chunk_yielded_at: Optional[float] = None

    def record_first_chunk_yielded(self):
        self.first_chunk_yielded_at = time.monotonic()
        generative_first_chunk_histogram.observe(self.first_chunk_yielded_at - self.global_start)

    def get_first_chunk_time(self) -> Optional[float]:
        if self.first_chunk_yielded_at is None:
            return None
        return self.first_chunk_yielded_at - self.global_start
