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
import os

import asyncpg
import pytest
from pytest_lazy_fixtures import lazy_fixture

from nucliadb_utils.storages.pg import PostgresStorage
from nucliadb_utils.store import MAIN
from nucliadb_utils.utilities import Utility

pytest_plugins = [
    "pytest_docker_fixtures",
    "nucliadb_utils.tests.gcs",
    "nucliadb_utils.tests.nats",
    "nucliadb_utils.tests.s3",
    "nucliadb_utils.tests.local",
]


@pytest.fixture(scope="function")
async def pg_storage(pg):
    dsn = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/postgres"
    storage = PostgresStorage(dsn)
    MAIN[Utility.STORAGE] = storage
    conn = await asyncpg.connect(dsn)
    await conn.execute(
        """
DROP table IF EXISTS kb_files;
DROP table IF EXISTS kb_files_fileparts;
"""
    )
    await conn.close()
    await storage.initialize()
    yield storage
    await storage.finalize()
    if Utility.STORAGE in MAIN:
        del MAIN[Utility.STORAGE]


def get_testing_storage_backend(default="gcs"):
    return os.environ.get("TESTING_STORAGE_BACKEND", default)


def lazy_storage_fixture():
    backend = get_testing_storage_backend()
    if backend == "gcs":
        return [lazy_fixture.lf("gcs_storage")]
    elif backend == "s3":
        return [lazy_fixture.lf("s3_storage")]
    elif backend == "pg":
        return [lazy_fixture.lf("pg_storage")]
    else:
        print(f"Unknown storage backend {backend}, using gcs")
        return [lazy_fixture.lf("gcs_storage")]


@pytest.fixture(scope="function", params=lazy_storage_fixture())
async def storage(request):
    """
    Generic storage fixture that allows us to run the same tests for different storage backends.
    """
    return request.param
