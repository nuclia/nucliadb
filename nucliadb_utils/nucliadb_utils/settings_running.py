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

from typing import Optional

from pydantic import BaseSettings


class RunningSettings(BaseSettings):
    debug: bool = True
    sentry_url: Optional[str] = None
    running_environment: str = "local"
    logging_integration: bool = False
    log_level: str = "DEBUG"
    activity_log_level: str = "INFO"
    chitchat_level: str = "INFO"
    metrics_port: int = 3030
    metrics_host: str = "0.0.0.0"
    serving_port: int = 8080
    serving_host: str = "0.0.0.0"


running_settings = RunningSettings()
