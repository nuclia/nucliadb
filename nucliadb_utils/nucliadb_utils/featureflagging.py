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
import pydantic

from nucliadb_utils import const


class Settings(pydantic.BaseSettings):
    environment: str = pydantic.Field(
        "local", env=["environment", "running_environment"]
    )
    flag_settings_url: Optional[str]


DEFAULT_FLAG_DATA: dict[str, Any] = {
    # These are just defaults to use for local dev and tests
    const.Features.WAIT_FOR_INDEX: {
        "rollout": 0,
        "variants": {"environment": ["none"]},
    },
    const.Features.DEFAULT_MIN_SCORE: {
        "rollout": 0,
        "variants": {"environment": ["none"]},
    },
    const.Features.ASK_YOUR_DOCUMENTS: {
        "rollout": 0,
        "variants": {"environment": ["stage", "local"]},
    },
    const.Features.EXPERIMENTAL_KB: {
        "rollout": 0,
        "variants": {"environment": ["local"]},
    },
    const.Features.READ_REPLICA_SEARCHES: {
        "rollout": 0,
        "variants": {"environment": ["local"]},
    },
}


class FlagService:
    def __init__(self):
        self.settings = Settings()
        if self.settings.flag_settings_url is None:
            self.flag_service = mrflagly.FlagService(data=json.dumps(DEFAULT_FLAG_DATA))
        else:
            self.flag_service = mrflagly.FlagService(
                url=self.settings.flag_settings_url
            )

    def enabled(
        self, flag_key: str, default: bool = False, context: Optional[dict] = None
    ) -> bool:
        if context is None:
            context = {}
        context["environment"] = self.settings.environment
        return self.flag_service.enabled(flag_key, default=default, context=context)
