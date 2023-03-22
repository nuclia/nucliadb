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
from unittest.mock import MagicMock, patch

import pytest

from nucliadb_telemetry import metrics

pytestmark = pytest.mark.asyncio


class TestObserver:
    @pytest.fixture(autouse=True)
    def histogram(self):
        mock = MagicMock()
        with patch(
            "nucliadb_telemetry.metrics.prometheus_client.Histogram", return_value=mock
        ):
            yield mock

    @pytest.fixture(autouse=True)
    def counter(self):
        mock = MagicMock()
        with patch(
            "nucliadb_telemetry.metrics.prometheus_client.Counter", return_value=mock
        ):
            yield mock

    def test_observer(self, histogram, counter):
        observer = metrics.Observer(
            "my_metric", buckets=[1, 2, 3], labels={"foo": "bar"}
        )
        with observer(labels={"foo": "baz"}):
            pass

        histogram.labels.assert_called_once_with(foo="baz")
        histogram.labels().observe.assert_called_once()
        counter.labels.assert_called_once_with(status=metrics.OK, foo="baz")
        counter.labels().inc.assert_called_once()

    def test_observer_error_labels(self, histogram, counter):
        class MyError(Exception):
            pass

        observer = metrics.Observer("my_metric", error_mappings={"my_error": MyError})
        with pytest.raises(MyError), observer():
            raise MyError("my_error")

        histogram.observe.assert_called_once()
        counter.labels.assert_called_once_with(status="my_error")
        counter.labels().inc.assert_called_once()

    def test_sync_decorator(self, histogram, counter):
        observer = metrics.Observer("my_metric")

        @observer.wrap()
        def my_func():
            pass

        my_func()

        histogram.observe.assert_called_once()
        counter.labels().inc.assert_called_once()

    async def test_async_decorator(self, histogram, counter):
        observer = metrics.Observer("my_metric")

        @observer.wrap()
        async def my_func():
            pass

        await my_func()

        histogram.observe.assert_called_once()
        counter.labels().inc.assert_called_once()


class TestGauge:
    def test_guage(self):
        gauge = metrics.Gauge("my_guage")
        gauge.set(5)

        assert gauge.gauge._value.get() == 5.0

    def test_guage_with_labels(self):
        gauge = metrics.Gauge("my_guage2", labelnames=["foo", "bar"])
        gauge.set(5, labels={"foo": "baz", "bar": "qux"})

        assert gauge.gauge.labels(**{"foo": "baz", "bar": "qux"})._value.get() == 5.0

        gauge.remove({"foo": "baz", "bar": "qux"})

        assert gauge.gauge.labels(**{"foo": "baz", "bar": "qux"})._value.get() == 0.0


class TestCounter:
    def test_counter(self):
        counter = metrics.Counter("my_counter")
        counter.inc()

        assert counter.counter._value.get() == 1.0

    def test_counter_with_labels(self):
        counter = metrics.Counter("my_counter2", labelnames=["foo", "bar"])
        counter.inc(labels={"foo": "baz", "bar": "qux"})

        assert (
            counter.counter.labels(**{"foo": "baz", "bar": "qux"})._value.get() == 1.0
        )
