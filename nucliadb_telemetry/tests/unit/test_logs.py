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
import logging
from unittest.mock import MagicMock, patch

import orjson
import pydantic
from opentelemetry.trace import format_span_id, format_trace_id

from nucliadb_telemetry import context, logs


def test_setup_logging(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("LOGGER_LEVELS", '{"foo": "WARNING"}')
    with patch("nucliadb_telemetry.logs.logging") as logging:
        logs.setup_logging()

        logging.getLogger.assert_any_call("foo")
        assert len(logging.getLogger().addHandler.mock_calls) == 6

        logger = logging.getLogger()
        handler = logger.addHandler.mock_calls[0].args[0]
        assert isinstance(handler.setFormatter.mock_calls[0].args[0], logs.JSONFormatter)


def test_setup_logging_plain(monkeypatch):
    with patch("nucliadb_telemetry.logs.logging") as logging:
        logs.setup_logging(
            settings=logs.LogSettings(
                log_format_type=logs.LogFormatType.PLAIN,
                logger_levels={"foo": "WARNING"},
            )
        )

        logging.getLogger.assert_any_call("foo")
        assert len(logging.getLogger().addHandler.mock_calls) == 5

        logger = logging.getLogger()
        handler = logger.addHandler.mock_calls[0].args[0]
        assert isinstance(handler.setFormatter.mock_calls[0].args[0], logs.ExtraFormatter)


class _TestLogMessage(pydantic.BaseModel):
    message: str
    foo: str
    bar: int


def test_logger_with_formatter(caplog):
    logger = logging.getLogger("test.logger")
    formatter = logs.JSONFormatter()

    outputted_records = []

    class Handler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            data = orjson.loads(msg)
            outputted_records.append(data)
            assert data["foo"] == "bar", msg
            assert data["bar"] == 42, msg
            assert data["message"] == "foobar", msg

    handler = Handler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(logging.ERROR)
    logger.propagate = False

    logger.error("foobar", extra={"foo": "bar", "bar": 42})
    logger.error({"message": "foobar", "foo": "bar", "bar": 42})
    logger.error(_TestLogMessage(message="foobar", foo="bar", bar=42))

    assert len(outputted_records) == 3


def test_logger_with_access_formatter(caplog):
    logger = logging.getLogger("test.logger2")
    formatter = logs.UvicornAccessFormatter()

    outputted_records = []

    class Handler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            data = orjson.loads(msg)
            outputted_records.append(data)

    handler = Handler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(logging.ERROR)
    logger.propagate = False

    logger.error(
        '%s - "%s %s HTTP/%s" %d',
        "client_addr",
        "method",
        "full_path",
        "http_version",
        200,
    )

    assert len(outputted_records) == 1

    assert outputted_records[0]["httpRequest"] == {
        "requestMethod": "method",
        "requestUrl": "full_path",
        "status": 200,
        "remoteIp": "client_addr",
        "protocol": "http_version",
    }


def test_logger_with_extra_formatter(caplog):
    logger = logging.getLogger("test.logger.extra")
    formatter = logs.ExtraFormatter("%(message)s%(extra_formatted)s")

    outputted_records = []

    class Handler(logging.Handler):
        def emit(self, record):
            outputted_records.append(self.format(record))

    handler = Handler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(logging.ERROR)
    logger.propagate = False

    logger.error("Something wrong", extra={"foo": "bar"})

    assert len(outputted_records) == 1

    assert "Something wrong" in outputted_records[0]
    assert "foo=bar" in outputted_records[0]


def test_logger_with_formatter_and_active_span(caplog):
    logger = logging.getLogger("test.logger3")
    formatter = logs.JSONFormatter()

    outputted_records = []

    class Handler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            data = orjson.loads(msg)
            outputted_records.append(data)

    handler = Handler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(logging.ERROR)
    logger.propagate = False

    span = MagicMock()
    span.get_span_context.return_value = MagicMock(
        # make sure to give potential very large numbers to make sure they are
        # serializable
        trace_id=9999999999999999999999,
        span_id=9999999999999999999999,
    )
    with patch("nucliadb_telemetry.logs.trace.get_current_span", return_value=span):
        logger.error("foobar")

    assert len(outputted_records) == 1
    assert outputted_records[0]["trace_id"] == format_trace_id(9999999999999999999999)
    assert outputted_records[0]["span_id"] == format_span_id(9999999999999999999999)


async def test_logger_with_context(caplog):
    logger = logging.getLogger("test.logger4")
    formatter = logs.JSONFormatter()

    outputted_records = []

    class Handler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            data = orjson.loads(msg)
            outputted_records.append(data)

    handler = Handler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(logging.ERROR)
    logger.propagate = False

    async def task2():
        context.add_context({"task2": "value", "foo": "baz"})
        logger.error("baz")

    async def task1():
        context.add_context({"task1": "value", "foo": "bar"})
        logger.error("bar")
        await asyncio.create_task(task2())

    await asyncio.create_task(task1())
    assert len(outputted_records) == 2

    assert outputted_records[0]["context"] == {
        "task1": "value",
        "foo": "bar",
    }
    assert outputted_records[1]["context"] == {
        "task1": "value",
        "task2": "value",
        "foo": "baz",
    }
