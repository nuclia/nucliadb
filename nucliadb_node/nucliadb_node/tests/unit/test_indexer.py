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

from typing import AsyncIterator
from unittest.mock import AsyncMock, Mock

import pytest
from nats.aio.client import Msg
from nucliadb_protos.nodewriter_pb2 import IndexMessage, IndexMessageSource

from nucliadb_node.indexer import WorkUnit
from nucliadb_utils.nats import MessageProgressUpdater


class TestIndexerWorkUnit:
    @pytest.fixture
    def processor_index_message(self) -> AsyncIterator[IndexMessage]:
        index_message = Mock()
        index_message.source = IndexMessageSource.PROCESSOR
        yield index_message

    @pytest.fixture
    def writer_index_message(self) -> AsyncIterator[IndexMessage]:
        index_message = Mock()
        index_message.source = IndexMessageSource.WRITER
        yield index_message

    @pytest.fixture
    def msg(self) -> AsyncIterator[Msg]:
        seqid = 1
        msg = AsyncMock()
        msg.reply = f"x.x.x.x.x.{seqid}"
        yield msg

    @pytest.fixture
    def message_progress_updater(self) -> AsyncIterator[MessageProgressUpdater]:
        mpu = AsyncMock()
        yield mpu

    @pytest.fixture
    def processor_work_unit(
        self,
        msg: Msg,
        processor_index_message: IndexMessage,
        message_progress_updater: MessageProgressUpdater,
    ) -> WorkUnit:
        yield WorkUnit(
            nats_msg=msg,
            index_message=processor_index_message,
            message_progress_updater=message_progress_updater,
        )

    @pytest.fixture
    def writer_work_unit(
        self,
        msg: Msg,
        writer_index_message: IndexMessage,
        message_progress_updater: MessageProgressUpdater,
    ) -> WorkUnit:
        yield WorkUnit(
            nats_msg=msg,
            index_message=writer_index_message,
            message_progress_updater=message_progress_updater,
        )

    def test_priority_comparaisons_same_seqid(
        self,
        processor_work_unit: WorkUnit,
        writer_work_unit: WorkUnit,
    ):
        """Writer is more prioritary, but priorities are ordered lower values first"""
        processor_work_unit.nats_msg.reply = f"x.x.x.x.x.1"
        writer_work_unit.nats_msg.reply = f"x.x.x.x.x.1"

        assert processor_work_unit > writer_work_unit
        assert writer_work_unit != processor_work_unit
        assert processor_work_unit == processor_work_unit

    def test_priority_comparaisons_with_different_seqid(
        self,
        processor_work_unit: WorkUnit,
        writer_work_unit: WorkUnit,
    ):
        processor_work_unit.nats_msg.reply = f"x.x.x.x.x.1"
        writer_work_unit.nats_msg.reply = f"x.x.x.x.x.2"

        assert processor_work_unit > writer_work_unit
        assert writer_work_unit != processor_work_unit
        assert processor_work_unit == processor_work_unit
