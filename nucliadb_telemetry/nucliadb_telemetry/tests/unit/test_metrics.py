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
import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from nucliadb_telemetry import metrics


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

    @pytest.mark.asyncio
    async def test_async_decorator(self, histogram, counter):
        observer = metrics.Observer("my_metric")

        @observer.wrap()
        async def my_func():
            pass

        await my_func()

        histogram.observe.assert_called_once()
        counter.labels().inc.assert_called_once()

    def test_gen_decorator(self, histogram, counter):
        observer = metrics.Observer("my_metric")

        @observer.wrap()
        def my_func():
            for i in range(1):
                time.sleep(0.2)
                yield i

        for _ in my_func():
            pass

        histogram.observe.assert_called_once()
        assert histogram.observe.call_args[0][0] >= 0.2
        counter.labels().inc.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_gen_decorator(self, histogram, counter):
        observer = metrics.Observer("my_metric")

        @observer.wrap()
        async def my_func():
            for i in range(1):
                await asyncio.sleep(0.2)
                yield i

        async for _ in my_func():
            pass

        histogram.observe.assert_called_once()
        assert histogram.observe.call_args[0][0] >= 0.2
        counter.labels().inc.assert_called_once()

    def test_observer_with_env(self, histogram, counter, monkeypatch):
        monkeypatch.setenv("VERSION", "1.0.0")
        observer = metrics.Observer(
            "my_metric", buckets=[1, 2, 3], labels={"foo": "bar"}
        )
        with observer(labels={"foo": "baz"}):
            pass

        histogram.labels.assert_called_once_with(foo="baz", version="1.0.0")
        histogram.labels().observe.assert_called_once()
        counter.labels.assert_called_once_with(
            status=metrics.OK, foo="baz", version="1.0.0"
        )
        counter.labels().inc.assert_called_once()


class TestGauge:
    def test_guage(self):
        gauge = metrics.Gauge("my_guage")
        gauge.set(5)

        assert gauge.gauge._value.get() == 5.0

    def test_guage_with_labels(self):
        gauge = metrics.Gauge("my_guage2", labels={"foo": "", "bar": ""})
        gauge.set(5, labels={"foo": "baz", "bar": "qux"})

        assert gauge.gauge.labels(**{"foo": "baz", "bar": "qux"})._value.get() == 5.0

        gauge.remove({"foo": "baz", "bar": "qux"})

        assert gauge.gauge.labels(**{"foo": "baz", "bar": "qux"})._value.get() == 0.0

    def test_guage_with_env_label(self, monkeypatch):
        monkeypatch.setenv("VERSION", "1.0.0")
        gauge = metrics.Gauge("my_guage3")
        gauge.set(5)

        assert gauge.gauge.labels(**{"version": "1.0.0"})._value.get() == 5.0


class TestCounter:
    def test_counter(self):
        counter = metrics.Counter("my_counter")
        counter.inc()

        assert counter.counter._value.get() == 1.0

    def test_counter_with_labels(self):
        counter = metrics.Counter("my_counter2", labels={"foo": "", "bar": ""})
        counter.inc(labels={"foo": "baz", "bar": "qux"})

        assert (
            counter.counter.labels(**{"foo": "baz", "bar": "qux"})._value.get() == 1.0
        )

    def test_counter_with_env_label(self, monkeypatch):
        monkeypatch.setenv("VERSION", "1.0.0")
        counter = metrics.Counter("my_counter3")
        counter.inc(labels={"version": "1.0.0"})

        assert counter.counter.labels(**{"version": "1.0.0"})._value.get() == 1.0


class TestHistogram:
    def test_histo(self):
        histo = metrics.Histogram("my_histo")
        histo.observe(5)

        assert [
            s for s in histo.histo.collect()[0].samples if s.labels.get("le") == "5.0"
        ][0].value == 1.0

    def test_histo_with_labels(self):
        histo = metrics.Histogram(
            "my_histo2", labels={"foo": "", "bar": ""}, buckets=[1, 2, 3]
        )
        histo.observe(1, labels={"foo": "baz", "bar": "qux"})

        assert [
            s for s in histo.histo.collect()[0].samples if s.labels.get("le") == "1.0"
        ][0].value == 1.0

    def test_histo_with_env_label(self, monkeypatch):
        monkeypatch.setenv("VERSION", "1.0.0")
        histo = metrics.Histogram("my_histo3", buckets=[1, 2, 3])
        histo.observe(1)

        assert [
            s for s in histo.histo.collect()[0].samples if s.labels.get("le") == "1.0"
        ][0].value == 1.0
