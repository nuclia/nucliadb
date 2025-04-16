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
from typing import Optional

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


class RAGMetrics:
    def __init__(self: "RAGMetrics"):
        self.global_start = time.monotonic()
        self._start_times: dict[str, float] = {}
        self._end_times: dict[str, float] = {}
        self.first_chunk_yielded_at: Optional[float] = None

    @contextlib.contextmanager
    def time(self, step: str):
        self._start(step)
        try:
            yield
        finally:
            self._end(step)

    def steps(self) -> dict[str, float]:
        return {step: self.elapsed(step) for step in self._end_times.keys()}

    def elapsed(self, step: str) -> float:
        return self._end_times[step] - self._start_times[step]

    def record_first_chunk_yielded(self):
        self.first_chunk_yielded_at = time.monotonic()
        generative_first_chunk_histogram.observe(self.first_chunk_yielded_at - self.global_start)

    def get_first_chunk_time(self) -> Optional[float]:
        if self.first_chunk_yielded_at is None:
            return None
        return self.first_chunk_yielded_at - self.global_start

    def _start(self, step: str):
        self._start_times[step] = time.monotonic()

    def _end(self, step: str):
        self._end_times[step] = time.monotonic()
        elapsed = self.elapsed(step)
        rag_histogram.observe(elapsed, labels={"step": step})
