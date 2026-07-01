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
"""
Integration tests for the `logs_foreign_key_error` decorator behaviour on
resources_v2 write operations.

These tests verify that writing to an ORM table whose parent row does not yet
exist (simulating a resource that has not been backfilled yet) does not raise an
exception but does emit a warning log, so that the dual-write migration path
cannot break normal operation.
"""

import logging

import pytest

from nucliadb.common.datamanagers import resources_v2
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import resources_pb2

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_basic_missing_parent_logs_warning_and_does_not_raise(
    maindb_driver: Driver,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Writing a kb_resources row whose kbid has no parent kbs row must not raise;
    instead a WARNING is logged by logs_foreign_key_error.
    """
    kbid = KnowledgeBox.new_unique_kbid()
    rid = Resource.new_unique_rid()

    with caplog.at_level(logging.WARNING, logger="nucliadb.common.datamanagers.utils"):
        async with maindb_driver.rw_transaction() as txn:
            # No kbs row exists for kbid — FK violation expected to be swallowed
            await resources_v2.set_basic(
                txn,
                kbid=kbid,
                rid=rid,
                basic=resources_pb2.Basic(slug="test-slug"),
            )
            await txn.commit()

    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("Foreign key violation" in r.message for r in warning_records), (
        f"Expected a FK warning log, got: {[r.message for r in warning_records]}"
    )
