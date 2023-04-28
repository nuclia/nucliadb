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

from unittest.mock import patch

from nucliadb_telemetry import errors


def test_capture_exception() -> None:
    with patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk, patch.object(
        errors, "SENTRY", True
    ):
        ex = Exception("test")
        errors.capture_exception(ex)
        mock_sentry_sdk.capture_exception.assert_called_once_with(ex)


def test_capture_exception_no_sentry() -> None:
    with patch.object(errors, "SENTRY", False), patch(
        "nucliadb_telemetry.errors.sentry_sdk"
    ) as mock_sentry_sdk:
        errors.capture_exception(Exception())
        mock_sentry_sdk.capture_exception.assert_not_called()


def test_capture_message() -> None:
    with patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk, patch.object(
        errors, "SENTRY", True
    ):
        errors.capture_message("error_msg", "level", "scope")
        mock_sentry_sdk.capture_message.assert_called_once_with(
            "error_msg", "level", "scope"
        )


def test_capture_message_no_sentry() -> None:
    with patch.object(errors, "SENTRY", False), patch(
        "nucliadb_telemetry.errors.sentry_sdk"
    ) as mock_sentry_sdk:
        errors.capture_message("error_msg", "level", "scope")
        mock_sentry_sdk.capture_message.assert_not_called()


def test_setup_error_handling(monkeypatch):
    monkeypatch.setenv("sentry_url", "sentry_url")
    monkeypatch.setenv("environment", "environment")
    with patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk, patch.object(
        errors, "SENTRY", True
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
    with patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk, patch.object(
        errors, "SENTRY", True
    ):
        with errors.push_scope() as scope:
            scope.set_extra("key", "value")
        mock_sentry_sdk.push_scope.assert_called_once_with()


def test_push_scope_no_sentry() -> None:
    with patch("nucliadb_telemetry.errors.sentry_sdk") as mock_sentry_sdk, patch.object(
        errors, "SENTRY", False
    ):
        with errors.push_scope() as scope:
            scope.set_extra("key", "value")
        mock_sentry_sdk.push_scope.assert_not_called()
