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

from unittest import mock

import pkg_resources
import pytest

from nucliadb.standalone.versions import (
    get_latest_version,
    installed_nucliadb,
    is_newer_release,
    latest_nucliadb,
)


@pytest.mark.parametrize(
    "installed,latest,expected",
    [
        ("1.1.1", "1.1.1.post1", False),
        ("1.1.1", "1.1.1", False),
        ("1.1.1", "1.1.0", False),
        ("1.1.1", "1.0.1", False),
        ("1.1.1", "0.1.1", False),
        ("1.1.1", "1.1.2", True),
        ("1.1.1", "1.2.1", True),
        ("1.1.1", "2.1.1", True),
    ],
)
def test_is_newer_release(installed, latest, expected):
    assert is_newer_release(installed, latest) is expected


def test_installed_nucliadb():
    pkg_resources.parse_version(installed_nucliadb())


@pytest.fixture()
def pypi_mock():
    version = "1.0.0"
    with mock.patch(
        "nucliadb.standalone.versions._get_latest_version", return_value=version
    ):
        yield


async def test_latest_nucliadb(pypi_mock):
    assert await latest_nucliadb() == "1.0.0"


async def test_get_latest_version(pypi_mock):
    assert await get_latest_version("foobar") == "1.0.0"
