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

import json
from typing import Any, Optional

import mrflagly
import pydantic_settings

from nucliadb_utils import const
from nucliadb_utils.settings import nuclia_settings, running_settings


class Settings(pydantic_settings.BaseSettings):
    flag_settings_url: Optional[str] = None


DEFAULT_FLAG_DATA: dict[str, Any] = {
    # These are just defaults to use for local dev and tests
    const.Features.SKIP_EXTERNAL_INDEX: {
        "rollout": 0,
        "variants": {"environment": ["none"]},
    },
    const.Features.LOG_REQUEST_PAYLOADS: {
        "rollout": 0,
        "variants": {"environment": ["none"]},
    },
    const.Features.IGNORE_EXTRACTED_IN_SEARCH: {
        "rollout": 0,
        "variants": {"environment": ["local"]},
    },
}


class FlagService:
    def __init__(self):
        settings = Settings()
        if settings.flag_settings_url is None:
            self.flag_service = mrflagly.FlagService(data=json.dumps(DEFAULT_FLAG_DATA))
        else:
            self.flag_service = mrflagly.FlagService(url=settings.flag_settings_url)

    def enabled(self, flag_key: str, default: bool = False, context: Optional[dict] = None) -> bool:
        if context is None:
            context = {}
        context["environment"] = running_settings.running_environment
        context["zone"] = nuclia_settings.nuclia_zone
        return self.flag_service.enabled(flag_key, default=default, context=context)
