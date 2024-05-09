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

from nucliadb.common import locking
from nucliadb.standalone.migrations import run_migrations

STANDALONE_MIGRATIONS = "nucliadb.standalone.migrations"


@pytest.fixture(scope="function")
def is_worker_node():
    with mock.patch(
        f"{STANDALONE_MIGRATIONS}.is_worker_node", return_value=True
    ) as mocked:
        yield mocked


@pytest.fixture(scope="function")
def run_migrator():
    with mock.patch(f"{STANDALONE_MIGRATIONS}.run_migrator") as mocked:
        yield mocked


def test_run_migrations_runs_on_worker_nodes_only(is_worker_node, run_migrator):
    is_worker_node.return_value = False

    run_migrations()

    run_migrator.assert_not_called()

    is_worker_node.return_value = True

    run_migrations()

    run_migrator.assert_called_once()


def test_run_migrations_waits_for_lock(is_worker_node, run_migrator):
    run_migrator.side_effect = [locking.ResourceLocked, None]

    run_migrations()

    assert run_migrator.call_count == 2
