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

# abstract advanced error handling into its own module to prevent
# code from handling sentry integration everywhere
import logging
from typing import Any, ContextManager, Optional

import pydantic

try:
    import sentry_sdk
    from sentry_sdk import Scope
    from sentry_sdk.integrations.logging import LoggingIntegration

    SENTRY = True
except ImportError:  # pragma: no cover
    Scope = LoggingIntegration = sentry_sdk = None  # type: ignore
    SENTRY = False


def capture_exception(error: BaseException) -> Optional[str]:
    if SENTRY:
        return sentry_sdk.capture_exception(error)
    return None


def capture_message(
    error_msg: str,
    level: Optional[str] = None,
    scope: Optional[Any] = None,
    **scope_args: Any
) -> Optional[str]:
    if SENTRY:
        return sentry_sdk.capture_message(error_msg, level, scope, **scope_args)
    return None


class NoopScope:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        ...

    def set_extra(self, key: str, value: Any) -> None:
        ...


def push_scope(**kwargs: Any) -> ContextManager[Scope]:
    if SENTRY:
        return sentry_sdk.push_scope(**kwargs)
    else:
        return NoopScope()  # type: ignore


class ErrorHandlingSettings(pydantic.BaseSettings):
    sentry_url: Optional[str] = None
    environment: str = pydantic.Field(
        "local", env=["environment", "running_environment"]
    )
    logging_integration: bool = False


def setup_error_handling(version: str) -> None:
    settings = ErrorHandlingSettings()

    if settings.sentry_url:
        enabled_integrations: list[Any] = []

        if settings.logging_integration:
            sentry_logging = LoggingIntegration(
                level=logging.CRITICAL, event_level=logging.CRITICAL
            )
            enabled_integrations.append(sentry_logging)

        sentry_sdk.init(
            release=version,
            environment=settings.environment,
            dsn=settings.sentry_url,
            integrations=enabled_integrations,
            default_integrations=False,
        )
