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

import httpx
import pkg_resources
from async_lru import alru_cache

logger = logging.getLogger(__name__)


VERSION_CACHE_TTL_SECONDS = 30 * 60  # 30 minutes
WAIT_AFTER_ERROR_SECONDS = 5 * 60  # 5 minutes
PYPI_JSON_API = "https://pypi.org/pypi/{package_name}/json"

NUCLIADB_PKG = "nucliadb"
NUCLIADB_ADMIN_ASSETS_PKG = "nucliadb-admin-assets"
WATCHED_PYPI_PACKAGES = [NUCLIADB_PKG, NUCLIADB_ADMIN_ASSETS_PKG]


async def get_versions() -> dict[str, dict[str, str]]:
    return {
        package: {
            "installed": release(get_package_version(package)),
            "latest": release(await cached_get_latest_package_version(package)),
        }
        for package in WATCHED_PYPI_PACKAGES
    }


def installed_nucliadb() -> str:
    return release(get_package_version(NUCLIADB_PKG))


def latest_nucliadb() -> str:
    return release(get_latest_package_version(NUCLIADB_PKG))


def release(version: str) -> str:
    # Remove automatic post versions and only keep explicit
    # release that are cut on important changes/bugfixes
    return version.split(".post")[0]


def get_package_version(package_name: str) -> str:
    return pkg_resources.get_distribution(package_name).version


def get_latest_package_version(package_name: str) -> str:
    pypi = PyPi()
    try:
        return pypi.get_latest_version(package_name)
    finally:
        pypi.close()


@alru_cache(maxsize=1, ttl=VERSION_CACHE_TTL_SECONDS)
async def cached_get_latest_package_version(package_name: str) -> str:
    pypi = PyPiAsync()
    try:
        return await pypi.get_latest_version(package_name)
    finally:
        await pypi.aclose()


class PyPi:
    def __init__(self):
        self.session = httpx.Client()

    def close(self):
        self.session.close()

    def get_latest_version(self, package_name: str) -> str:
        response = self.session.get(
            PYPI_JSON_API.format(package_name=package_name),
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()["info"]["version"]


class PyPiAsync:
    def __init__(self):
        self._asession = None

    @property
    def asession(self):
        if self._asession is None:
            self._asession = httpx.AsyncClient()
        return self._asession

    async def aclose(self):
        if self._asession is None:
            return
        try:
            await self._asession.aclose()
        except Exception:
            pass

    async def get_latest_version(self, package_name: str) -> str:
        response = await self.asession.get(
            PYPI_JSON_API.format(package_name=package_name),
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()["info"]["version"]
