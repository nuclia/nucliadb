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

import pytest

from nucliadb.standalone.versions import is_newer_release


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
