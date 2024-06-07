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
import aiohttp

from . import exceptions


def check_status(resp: aiohttp.ClientResponse, resp_text: str) -> None:
    if resp.status < 300:
        return
    elif resp.status == 402:
        raise exceptions.AccountLimitException(f"Account limits exceeded: {resp_text}")
    elif resp.status == 404:
        raise exceptions.NotFoundException(f"Resource not found: {resp_text}")
    elif resp.status in (401, 403):
        raise exceptions.AuthorizationException(f"Unauthorized to access: {resp.status}")
    elif resp.status == 429:
        raise exceptions.RateLimitException("Rate limited")
    else:
        raise exceptions.ClientException(f"Unknown error: {resp.status} - {resp_text}")
