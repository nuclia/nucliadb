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
import httpx

PYPI_JSON_API = "https://pypi.org/pypi/{package_name}/json"


class PyPi:
    def __init__(self, timeout_seconds: int = 2):
        self.session = httpx.AsyncClient(timeout=timeout_seconds)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self):
        await self.session.aclose()

    async def get_latest_version(self, package_name: str) -> str:
        response = await self.session.get(
            PYPI_JSON_API.format(package_name=package_name),
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()["info"]["version"]
