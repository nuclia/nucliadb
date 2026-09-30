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

import backoff
from fastapi import HTTPException

from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils import const
from nucliadb_utils.transaction import (
    MaxTransactionSizeExceededError,
    StreamingServerError,
    TransactionCommitTimeoutError,
)
from nucliadb_utils.utilities import get_transaction_utility


async def commit(writer: BrokerMessage, partition: int, wait: bool = True) -> None:
    try:
        await transaction_commit(writer, partition, wait)
    except TransactionCommitTimeoutError:
        raise HTTPException(
            status_code=501,
            detail="Inconsistent write. This resource will not be processed and may not be stored.",
        )
    except MaxTransactionSizeExceededError:
        raise HTTPException(
            status_code=413,
            detail="Transaction size exceeded. The resource is too large to be stored. Consider using file fields or split into multiple requests.",
        )
    except StreamingServerError:
        raise HTTPException(
            status_code=504,
            detail="Timeout waiting for the streaming server to respond. Please back off and retry.",
        )


@backoff.on_exception(
    backoff.expo,
    (StreamingServerError,),
    jitter=backoff.random_jitter,
    max_tries=3,
)
async def transaction_commit(writer: BrokerMessage, partition: int, wait: bool = True):
    transaction = get_transaction_utility()
    await transaction.commit(
        writer,
        partition,
        wait=wait,
        target_subject=const.Streams.INGEST.subject.format(partition=partition),
    )
