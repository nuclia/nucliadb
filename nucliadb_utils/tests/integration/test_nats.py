import asyncio

import nats
import pytest
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from nucliadb_utils.nats import MessageProgressUpdater


@pytest.mark.asyncio
async def test_nats_progress_updater(natsd):
    ack_wait = 5
    message_acked = asyncio.Event()
    message_redelivered = asyncio.Event()

    async def message_handler(msg: Msg):
        async with MessageProgressUpdater(msg, ack_wait * 0.66):
            if msg.metadata.num_delivered > 1:
                message_redelivered.set()
                await msg.ack()
                return
            await asyncio.sleep(2 * ack_wait)
            await msg.ack()
            message_acked.set()

    nc = NATS()
    await nc.connect(natsd)
    js = nc.jetstream()
    await js.add_stream(name="my_stream", subjects=["my_subject"])
    await js.subscribe(
        "my_subject",
        flow_control=True,
        cb=message_handler,
        config=nats.js.api.ConsumerConfig(
            deliver_policy=nats.js.api.DeliverPolicy.BY_START_SEQUENCE,
            opt_start_seq=1,
            ack_policy=nats.js.api.AckPolicy.EXPLICIT,
            max_ack_pending=10,
            max_deliver=10,
            ack_wait=ack_wait,
            idle_heartbeat=1,
        ),
    )
    assert not message_acked.is_set()
    await nc.publish("my_subject", b"hello")   
    
    # Check that the message was acked
    await message_acked.wait()

    # Make sure no redeliveries happened
    assert not message_redelivered.is_set()

    await nc.close()

