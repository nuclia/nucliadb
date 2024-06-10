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

import logging
import os

# abstract advanced error handling into its own module to prevent
# code from handling sentry integration everywhere
from typing import Any, ContextManager, List, Literal, Optional

import pydantic
from pydantic_settings import BaseSettings

try:
    import sentry_sdk
    from sentry_sdk import Scope
    from sentry_sdk.integrations.logging import (
        BreadcrumbHandler,
        EventHandler,
        LoggingIntegration,
    )

    SENTRY = os.environ.get("SENTRY_URL") is not None
except ImportError:  # pragma: no cover
    Scope = sentry_sdk = None  # type: ignore

    class LoggingIntegration:  # type: ignore
        pass

    EventHandler = BreadcrumbHandler = LoggingIntegration  # type: ignore
    SENTRY = False


def capture_exception(error: BaseException) -> Optional[str]:
    if SENTRY:
        return sentry_sdk.capture_exception(error)
    return None


def capture_message(
    error_msg: str,
    level: Optional[Literal["fatal", "critical", "error", "warning", "info", "debug"]] = None,
    scope: Optional[Any] = None,
    **scope_args: Any,
) -> Optional[str]:
    if SENTRY:
        return sentry_sdk.capture_message(error_msg, level, scope, **scope_args)
    return None


class NoopScope:
    def __enter__(self):
        return self

    def __exit__(self, *args): ...

    def set_extra(self, key: str, value: Any) -> None: ...


def push_scope(**kwargs: Any) -> ContextManager[Scope]:
    if SENTRY:
        return sentry_sdk.push_scope(**kwargs)
    else:
        return NoopScope()


class ErrorHandlingSettings(BaseSettings):
    zone: str = pydantic.Field(
        default="local", validation_alias=pydantic.AliasChoices("NUCLIA_ZONE", "ZONE")
    )
    sentry_url: Optional[str] = None
    environment: str = pydantic.Field(
        default="local",
        validation_alias=pydantic.AliasChoices("environment", "running_environment"),
    )


def setup_error_handling(version: str) -> None:
    settings = ErrorHandlingSettings()

    if settings.sentry_url:
        # Disabled everywhere for now. Let's have less knobs to tweak.
        # Either we use with with sentry or we don't.
        # enabled_integrations: list[Any] = [
        #     LoggingIntegration(level=logging.CRITICAL, event_level=logging.CRITICAL)
        # ]

        sentry_sdk.init(
            release=version,
            environment=settings.environment,
            dsn=settings.sentry_url,
            integrations=[],
            default_integrations=False,
        )
        sentry_sdk.set_tag("zone", settings.zone)


class SentryHandler(EventHandler):
    def __init__(self, allowed_loggers: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._allowed_loggers = allowed_loggers

    def emit(self, record):
        if record.name in self._allowed_loggers or record.name.split(".")[0] in self._allowed_loggers:
            super().emit(record)


class SentryLoggingIntegration(LoggingIntegration):
    def __init__(self, allowed_loggers: List[str], level=logging.INFO, event_level=logging.ERROR):
        self._breadcrumb_handler = BreadcrumbHandler(level=level)
        self._handler = SentryHandler(allowed_loggers, level=event_level)


# Initialize Sentry with the custom logging handler


def setup_sentry_logging_integration(for_loggers: List[str]) -> None:
    settings = ErrorHandlingSettings()
    if settings.sentry_url:
        sentry_sdk.init(
            dsn=settings.sentry_url,
            environment=settings.environment,
            integrations=[SentryLoggingIntegration(for_loggers)],
        )
        sentry_sdk.set_tag("zone", settings.zone)
