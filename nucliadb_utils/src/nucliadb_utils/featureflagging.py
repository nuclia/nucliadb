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
import uuid
from typing import Any

import mrflagly
import pydantic_settings
from flipt_client import FliptClient  # type: ignore[import-untyped]
from flipt_client.errors import EvaluationError  # type: ignore[import-untyped]
from flipt_client.models import (  # type: ignore[import-untyped]
    ClientOptions,
    ClientTokenAuthentication,
    FetchMode,
)
from pydantic import Field

from nucliadb_utils import const, logger
from nucliadb_utils.settings import nuclia_settings, running_settings


class Settings(pydantic_settings.BaseSettings):
    flag_settings_url: str | None = None

    flipt_server_url: str | None = Field(default=None, description="Flipt feature flag server URL")
    flipt_token: str | None = Field(default=None, description="Flipt feature flag server auth token")


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
    const.Features.SEMANTIC_GRAPH: {
        "rollout": 0,
        "variants": {"environment": ["local"]},
    },
    const.Features.AUDIT_RETRIEVE_AND_AUGMENT: {
        "rollout": 0,
        "variants": {"environment": ["local"]},
    },
}


class FlagService:
    def __init__(self) -> None:
        settings = Settings()

        if settings.flag_settings_url is None:
            self.flag_service = mrflagly.FlagService(data=json.dumps(DEFAULT_FLAG_DATA))  # type: ignore[attr-defined,ty:unresolved-attribute]
        else:
            self.flag_service = mrflagly.FlagService(url=settings.flag_settings_url)  # type: ignore[attr-defined,ty:unresolved-attribute]

        # We are transitioning from mr. flaggly to Flipt. Meanwhile, we'll have
        # both clients and check both places
        self.flipt_enabled = (settings.flipt_server_url is not None) and (
            settings.flipt_token is not None
        )
        logger.info(f"Flipt enabled? {self.flipt_enabled}")
        if self.flipt_enabled and settings.flipt_token:
            self.client: FliptClient = FliptClient(
                opts=ClientOptions(
                    url=settings.flipt_server_url,
                    authentication=ClientTokenAuthentication(client_token=settings.flipt_token),
                    environment=running_settings.running_environment,
                    namespace="nucliadb",
                    fetch_mode=FetchMode.STREAMING,
                )
            )
            self.entity_id = str(uuid.uuid4())

    def enabled(self, flag_key: str, default: bool = False, context: dict | None = None) -> bool:
        if context is None:
            context = {}
        context["environment"] = running_settings.running_environment
        context["zone"] = nuclia_settings.nuclia_zone

        if self.flipt_enabled and flag_key in const._FliptFeatures:
            try:
                evaluation = self.client.evaluate_boolean(
                    flag_key=flag_key,
                    entity_id=self.entity_id,
                    context=context,
                )
            except EvaluationError as exc:
                logger.warning("Flipt FF evaluation failed", exc_info=exc)
                return False
            else:
                logger.debug(f"Flipt evaluation of {flag_key} for {context} was {evaluation}")
                return evaluation.enabled
        else:
            return self.flag_service.enabled(flag_key, default=default, context=context)
