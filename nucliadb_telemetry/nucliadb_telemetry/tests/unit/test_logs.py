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
import logging
from unittest.mock import MagicMock, patch

import orjson
import pydantic
import pytest
from opentelemetry.trace import format_span_id, format_trace_id

from nucliadb_telemetry import context, logs


def test_setup_logging(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("LOGGER_LEVELS", '{"foo": "WARNING"}')
    with patch("nucliadb_telemetry.logs.logging") as logging:
        logs.setup_logging()

        logging.getLogger.assert_any_call("foo")
        assert len(logging.getLogger().addHandler.mock_calls) == 5

        logger = logging.getLogger()
        handler = logger.addHandler.mock_calls[0].args[0]
        assert isinstance(
            handler.setFormatter.mock_calls[0].args[0], logs.JSONFormatter
        )


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
        assert isinstance(
            handler.setFormatter.mock_calls[0].args[0], logs.ExtraFormatter
        )


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

    assert outputted_records[0] == "Something wrong -- foo=bar"


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


@pytest.mark.asyncio
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
