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

import pkg_resources
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration


def set_sentry(
    sentry_url: str,
    environment: str = "local",
    logging_integration: bool = False,
) -> None:
    if sentry_url:
        enabled_integrations = []

        if logging_integration:
            sentry_logging = LoggingIntegration(
                level=logging.CRITICAL, event_level=logging.CRITICAL
            )
            enabled_integrations.append(sentry_logging)

        version = pkg_resources.get_distribution("nucliadb_search").version
        sentry_sdk.init(
            release=version,
            environment=environment,
            dsn=sentry_url,
            integrations=enabled_integrations,
        )
