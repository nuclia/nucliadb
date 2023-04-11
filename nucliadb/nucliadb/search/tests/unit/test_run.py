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
from unittest.mock import patch

import pytest

from nucliadb.search.app import application
from nucliadb.search.run import run


@pytest.fixture(scope="function")
def run_fastapi_with_metrics():
    with patch("nucliadb.search.run.run_fastapi_with_metrics") as mocked:
        yield mocked


def test_run_with_metrics(run_fastapi_with_metrics):
    run()

    run_fastapi_with_metrics.assert_called_once_with(application)
