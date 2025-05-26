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

from unittest.mock import patch

from nucliadb_telemetry import errors


def test_capture_exception() -> None:
    with (
        patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk,
        patch.object(errors, "SENTRY", True),
    ):
        ex = Exception("test")
        errors.capture_exception(ex)
        mock_sentry_sdk.capture_exception.assert_called_once_with(ex)


def test_capture_exception_no_sentry() -> None:
    with (
        patch.object(errors, "SENTRY", False),
        patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk,
    ):
        errors.capture_exception(Exception())
        mock_sentry_sdk.capture_exception.assert_not_called()


def test_capture_message() -> None:
    with (
        patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk,
        patch.object(errors, "SENTRY", True),
    ):
        errors.capture_message("error_msg", "info", "scope")
        mock_sentry_sdk.capture_message.assert_called_once_with("error_msg", "info", "scope")


def test_capture_message_no_sentry() -> None:
    with (
        patch.object(errors, "SENTRY", False),
        patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk,
    ):
        errors.capture_message("error_msg", "info", "scope")
        mock_sentry_sdk.capture_message.assert_not_called()


def test_setup_error_handling(monkeypatch):
    monkeypatch.setenv("sentry_url", "sentry_url")
    monkeypatch.setenv("environment", "environment")
    with (
        patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk,
        patch.object(errors, "SENTRY", True),
    ):
        errors.setup_error_handling("1.0.0")
        mock_sentry_sdk.init.assert_called_once_with(
            release="1.0.0",
            environment="environment",
            dsn="sentry_url",
            integrations=[],
            default_integrations=False,
        )


def test_setup_error_handling_no_sentry(monkeypatch):
    with patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk:
        errors.setup_error_handling("1.0.0")
        mock_sentry_sdk.init.assert_not_called()


def test_push_scope() -> None:
    with (
        patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk,
        patch.object(errors, "SENTRY", True),
    ):
        with errors.push_scope() as scope:
            scope.set_extra("key", "value")
        mock_sentry_sdk.push_scope.assert_called_once_with()


def test_push_scope_no_sentry() -> None:
    with (
        patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk,
        patch.object(errors, "SENTRY", False),
    ):
        with errors.push_scope() as scope:
            scope.set_extra("key", "value")
        mock_sentry_sdk.push_scope.assert_not_called()
