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
import enum
import logging
from typing import Optional

import pkg_resources
from cachetools import TTLCache

from nucliadb.common.http_clients.pypi import PyPi, PyPiAsync

logger = logging.getLogger(__name__)


CACHE_TTL_SECONDS = 30 * 60  # 30 minutes
CACHE = TTLCache(maxsize=128, ttl=CACHE_TTL_SECONDS)  # type: ignore


class StandalonePackages(enum.Enum):
    NUCLIADB = "nucliadb"
    NUCLIADB_ADMIN_ASSETS = "nucliadb-admin-assets"


WatchedPackages = [pkg.value for pkg in StandalonePackages]


def installed_nucliadb() -> str:
    return get_package_version(StandalonePackages.NUCLIADB.value)


def nucliadb_updates_available() -> bool:
    installed = installed_nucliadb()
    latest = latest_nucliadb()
    if latest is None:
        return False
    return is_newer_release(installed, latest)


def latest_nucliadb() -> Optional[str]:
    latest = get_latest_package_version(StandalonePackages.NUCLIADB.value)
    if latest is None:
        return None
    return latest


def is_newer_release(installed: str, latest: str) -> bool:
    """
    Returns true if the latest version is newer than the installed version.
    >>> is_newer_release("1.2.3", "1.2.4")
    True
    >>> is_newer_release("1.2.3", "1.2.3")
    False
    >>> is_newer_release("1.2.3", "1.2.3.post1")
    False
    """
    parsed_installed = pkg_resources.parse_version(_release(installed))
    parsed_latest = pkg_resources.parse_version(_release(latest))
    return parsed_latest > parsed_installed


def _release(version: str) -> str:
    """
    Strips the .postX part of the version so that wecan compare major.minor.patch only.

    >>> _release("1.2.3")
    '1.2.3'
    >>> _release("1.2.3.post1")
    '1.2.3'
    """
    return version.split(".post")[0]


def get_package_version(package_name: str) -> str:
    return pkg_resources.get_distribution(package_name).version


def get_latest_package_version(package: str) -> Optional[str]:
    result = CACHE.get(package, None)
    if result is None:
        try:
            result = _get_latest_package_version(package)
        except Exception as exc:
            logger.warning("Error getting latest nucliadb version", exc_info=exc)
            return None
        CACHE[package] = result
    return result


async def async_get_latest_package_version(package: str) -> Optional[str]:
    result = CACHE.get(package, None)
    if result is None:
        try:
            result = await _async_get_latest_package_version(package)
        except Exception as exc:
            logger.warning("Error getting latest nucliadb version", exc_info=exc)
            return None
        CACHE[package] = result
    return result


def _get_latest_package_version(package_name: str) -> str:
    with PyPi() as pypi:
        return pypi.get_latest_version(package_name)


async def _async_get_latest_package_version(package_name: str) -> str:
    async with PyPiAsync() as pypi:
        return await pypi.get_latest_version(package_name)
