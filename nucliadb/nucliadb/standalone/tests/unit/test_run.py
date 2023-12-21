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
from unittest import mock

import pytest

from nucliadb.standalone.run import run, run_async_nucliadb
from nucliadb.standalone.settings import Settings


@pytest.fixture(scope="function", autouse=True)
def mocked_deps():
    with mock.patch("uvicorn.Server.run"), mock.patch(
        "pydantic_argparse.ArgumentParser.parse_typed_args", return_value=Settings()
    ), mock.patch(
        "nucliadb.standalone.run.get_latest_nucliadb", return_value="1.0.0"
    ), mock.patch(
        "uvicorn.Server.startup"
    ):
        yield


def test_run():
    run()


async def test_run_async_nucliadb():
    await run_async_nucliadb(Settings())
