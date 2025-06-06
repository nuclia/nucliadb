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

from nucliadb.search.search.metrics import AskMetrics, Metrics


def test_metrics():
    metrics = Metrics("foo")

    with metrics.time("key_1"):
        pass

    metrics.set("key_2", 1)

    child_metrics = metrics.child_span("child_1")
    with child_metrics.time("key_3"):
        pass

    assert child_metrics["key_3"] > 0
    assert len(child_metrics.to_dict()) == 1
    assert child_metrics.to_dict()["key_3"] > 0
    assert child_metrics.dump() == {
        "child_1": child_metrics.to_dict(),
    }

    assert metrics["key_1"] > 0
    assert metrics["key_2"] == 1
    assert len(metrics.to_dict()) == 2
    assert metrics.to_dict()["key_1"] > 0
    assert metrics.to_dict()["key_2"] == 1

    metrics_dump = metrics.dump()
    assert len(metrics_dump) == 2
    assert metrics_dump == {
        "foo": metrics.to_dict(),
        "child_1": child_metrics.to_dict(),
    }


def test_ask_metrics():
    metrics = AskMetrics()
    assert metrics.id == "ask"
    assert metrics.get_first_chunk_time() is None
    metrics.record_first_chunk_yielded()
    assert metrics.get_first_chunk_time() > 0
