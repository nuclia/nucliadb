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
import asyncio
import uuid

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.processor import Processor
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import writer_pb2 as wpb
from tests.utils.broker_messages import BrokerMessageBuilder


pytestmark = pytest.mark.usefixtures("dummy_nidx_utility")


def _single_field_writer_message(kbid: str, rid: str, field_id: str) -> wpb.BrokerMessage:
    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        source=wpb.BrokerMessage.MessageSource.WRITER,
    )
    text_field = bmb.field_builder(field_id, rpb.FieldType.TEXT)
    text_field.with_extracted_text(f"text for {field_id}")
    bm = bmb.build()
    bm.texts[field_id].body = f"text for {field_id}"

    # Simulate a minimal field-scoped update (no resource-level payloads).
    bm.ClearField("basic")
    bm.ClearField("origin")
    bm.ClearField("extra")
    bm.ClearField("security")
    bm.ClearField("user_relations")
    return bm


def _processor_message(kbid: str, rid: str, field_id: str) -> wpb.BrokerMessage:
    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        source=wpb.BrokerMessage.MessageSource.PROCESSOR,
    )
    text_field = bmb.field_builder(field_id, rpb.FieldType.TEXT)
    text_field.with_extracted_text(f"processor text for {field_id}")
    return bmb.build()


def _two_field_writer_message(kbid: str, rid: str) -> wpb.BrokerMessage:
    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        source=wpb.BrokerMessage.MessageSource.WRITER,
    )
    bmb.field_builder("field-a", rpb.FieldType.TEXT).with_extracted_text("a")
    bmb.field_builder("field-b", rpb.FieldType.TEXT).with_extracted_text("b")
    bm = bmb.build()
    bm.texts["field-a"].body = "a"
    bm.texts["field-b"].body = "b"

    bm.ClearField("basic")
    bm.ClearField("origin")
    bm.ClearField("extra")
    bm.ClearField("security")
    bm.ClearField("user_relations")
    return bm


async def _create_existing_resource(kbid: str, rid: str) -> None:
    basic = rpb.Basic()
    basic.metadata.status = rpb.Metadata.Status.PENDING
    async with datamanagers.with_rw_transaction() as txn:
        await datamanagers.resources.set(txn, kbid=kbid, rid=rid, basic=basic, shard="shard")
        await txn.commit()


async def _hold_locks(
    processor: Processor,
    maindb_driver: Driver,
    *,
    message: wpb.BrokerMessage,
    kbid: str,
    rid: str,
    resource_exists: bool,
    started: asyncio.Event,
    release: asyncio.Event,
) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await processor._acquire_txn_locks(
            txn,
            message=message,
            kbid=kbid,
            rid=rid,
            resource_exists=resource_exists,
        )
        started.set()
        await release.wait()


async def _wait_to_acquire_locks(
    processor: Processor,
    maindb_driver: Driver,
    *,
    message: wpb.BrokerMessage,
    kbid: str,
    rid: str,
    resource_exists: bool,
    acquired: asyncio.Event,
) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await processor._acquire_txn_locks(
            txn,
            message=message,
            kbid=kbid,
            rid=rid,
            resource_exists=resource_exists,
        )
        acquired.set()


@pytest.mark.deploy_modes("standalone")
async def test_single_field_writer_different_fields_can_lock_in_parallel(
    processor: Processor,
    maindb_driver: Driver,
    knowledgebox: str,
) -> None:
    rid = str(uuid.uuid4())
    await _create_existing_resource(knowledgebox, rid)

    msg_a = _single_field_writer_message(knowledgebox, rid, "field-a")
    msg_b = _single_field_writer_message(knowledgebox, rid, "field-b")

    assert processor._should_use_single_field_locking(msg_a, resource_exists=True)[0] is True
    assert processor._should_use_single_field_locking(msg_b, resource_exists=True)[0] is True

    started = asyncio.Event()
    release = asyncio.Event()
    acquired = asyncio.Event()

    holder = asyncio.create_task(
        _hold_locks(
            processor,
            maindb_driver,
            message=msg_a,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            started=started,
            release=release,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)

    waiter = asyncio.create_task(
        _wait_to_acquire_locks(
            processor,
            maindb_driver,
            message=msg_b,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            acquired=acquired,
        )
    )

    try:
        await asyncio.wait_for(acquired.wait(), timeout=3.0)
    finally:
        release.set()
        await holder
        await waiter


@pytest.mark.deploy_modes("standalone")
async def test_single_field_writer_same_field_is_serialized(
    processor: Processor,
    maindb_driver: Driver,
    knowledgebox: str,
) -> None:
    rid = str(uuid.uuid4())
    await _create_existing_resource(knowledgebox, rid)

    msg = _single_field_writer_message(knowledgebox, rid, "field-a")

    started = asyncio.Event()
    release = asyncio.Event()
    acquired = asyncio.Event()

    holder = asyncio.create_task(
        _hold_locks(
            processor,
            maindb_driver,
            message=msg,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            started=started,
            release=release,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)

    waiter = asyncio.create_task(
        _wait_to_acquire_locks(
            processor,
            maindb_driver,
            message=msg,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            acquired=acquired,
        )
    )

    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(acquired.wait(), timeout=0.2)
        release.set()
        await asyncio.wait_for(acquired.wait(), timeout=1.0)
    finally:
        await holder
        await waiter


@pytest.mark.deploy_modes("standalone")
async def test_processor_message_uses_exclusive_resource_lock(
    processor: Processor,
    maindb_driver: Driver,
    knowledgebox: str,
) -> None:
    rid = str(uuid.uuid4())
    await _create_existing_resource(knowledgebox, rid)

    processor_msg = _processor_message(knowledgebox, rid, "field-a")
    writer_msg = _single_field_writer_message(knowledgebox, rid, "field-b")

    started = asyncio.Event()
    release = asyncio.Event()
    acquired = asyncio.Event()

    holder = asyncio.create_task(
        _hold_locks(
            processor,
            maindb_driver,
            message=processor_msg,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            started=started,
            release=release,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)

    waiter = asyncio.create_task(
        _wait_to_acquire_locks(
            processor,
            maindb_driver,
            message=writer_msg,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            acquired=acquired,
        )
    )

    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(acquired.wait(), timeout=0.2)
        release.set()
        await asyncio.wait_for(acquired.wait(), timeout=1.0)
    finally:
        await holder
        await waiter


@pytest.mark.deploy_modes("standalone")
async def test_multi_field_writer_falls_back_to_exclusive_resource_lock(
    processor: Processor,
    maindb_driver: Driver,
    knowledgebox: str,
) -> None:
    rid = str(uuid.uuid4())
    await _create_existing_resource(knowledgebox, rid)

    multi_field = _two_field_writer_message(knowledgebox, rid)
    single_field = _single_field_writer_message(knowledgebox, rid, "field-c")

    started = asyncio.Event()
    release = asyncio.Event()
    acquired = asyncio.Event()

    holder = asyncio.create_task(
        _hold_locks(
            processor,
            maindb_driver,
            message=multi_field,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            started=started,
            release=release,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)

    waiter = asyncio.create_task(
        _wait_to_acquire_locks(
            processor,
            maindb_driver,
            message=single_field,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=True,
            acquired=acquired,
        )
    )

    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(acquired.wait(), timeout=0.2)
        release.set()
        await asyncio.wait_for(acquired.wait(), timeout=1.0)
    finally:
        await holder
        await waiter


@pytest.mark.deploy_modes("standalone")
async def test_single_field_writer_on_new_resource_uses_resource_lock(
    processor: Processor,
    maindb_driver: Driver,
    knowledgebox: str,
) -> None:
    rid = str(uuid.uuid4())

    msg_a = _single_field_writer_message(knowledgebox, rid, "field-a")
    msg_b = _single_field_writer_message(knowledgebox, rid, "field-b")

    started = asyncio.Event()
    release = asyncio.Event()
    acquired = asyncio.Event()

    holder = asyncio.create_task(
        _hold_locks(
            processor,
            maindb_driver,
            message=msg_a,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=False,
            started=started,
            release=release,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)

    waiter = asyncio.create_task(
        _wait_to_acquire_locks(
            processor,
            maindb_driver,
            message=msg_b,
            kbid=knowledgebox,
            rid=rid,
            resource_exists=False,
            acquired=acquired,
        )
    )

    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(acquired.wait(), timeout=0.2)
        release.set()
        await asyncio.wait_for(acquired.wait(), timeout=1.0)
    finally:
        await holder
        await waiter