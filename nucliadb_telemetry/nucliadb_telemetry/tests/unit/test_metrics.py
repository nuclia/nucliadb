from unittest.mock import MagicMock, patch

import pytest

from nucliadb_telemetry import metrics

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def histogram():
    mock = MagicMock()
    with patch(
        "nucliadb_telemetry.metrics.prometheus_client.Histogram", return_value=mock
    ):
        yield mock


@pytest.fixture(autouse=True)
def counter():
    mock = MagicMock()
    with patch(
        "nucliadb_telemetry.metrics.prometheus_client.Counter", return_value=mock
    ):
        yield mock


class TestObserver:
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
