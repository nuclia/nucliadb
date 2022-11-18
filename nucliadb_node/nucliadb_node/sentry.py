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
import logging
from typing import Any, List, Optional

try:
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    SENTRY = True
except ImportError:
    SENTRY = False


def set_sentry(
    sentry_url: Optional[str] = None,
    environment: str = "local",
    logging_integration: bool = False,
):
    if sentry_url:
        enabled_integrations: List[Any] = []

        # sentry_exception = ExcepthookIntegration(always_run=True)
        # enabled_integrations.append(sentry_exception)

        if logging_integration:
            sentry_logging = LoggingIntegration(
                level=logging.CRITICAL, event_level=logging.CRITICAL
            )
            enabled_integrations.append(sentry_logging)

        import pkg_resources  # type: ignore

        version = pkg_resources.get_distribution("nucliadb_node").version
        sentry_sdk.init(
            release=version,
            environment=environment,
            dsn=sentry_url,
            integrations=enabled_integrations,
            default_integrations=False,
        )
