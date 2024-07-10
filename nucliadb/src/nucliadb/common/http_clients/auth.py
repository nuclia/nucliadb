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

import aiohttp
import pydantic

from nucliadb_utils.settings import nuclia_settings

from .utils import check_status

logger = logging.getLogger(__name__)


class AuthInfoUser(pydantic.BaseModel):
    user_id: str
    account_id: str
    account_type: str


class AuthInfoResponse(pydantic.BaseModel):
    auth: str
    user: AuthInfoUser


class NucliaAuthHTTPClient:
    def __init__(self):
        self.session = aiohttp.ClientSession()
        self.base_url = (
            nuclia_settings.nuclia_public_url.format(zone=nuclia_settings.nuclia_zone) + "/api"
        )
        self.headers = {}
        if nuclia_settings.nuclia_service_account is not None:
            self.headers["X-NUCLIA-NUAKEY"] = f"Bearer {nuclia_settings.nuclia_service_account}"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self.session.close()

    async def status(self) -> AuthInfoResponse:
        url = self.base_url + "/authorizer/info"
        async with self.session.get(url, headers=self.headers) as resp:
            resp_text = await resp.text()
            check_status(resp, resp_text)
            return AuthInfoResponse.model_validate_json(resp_text)
