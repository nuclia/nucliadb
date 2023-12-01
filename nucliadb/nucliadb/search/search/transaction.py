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

import contextlib

from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.txn_utils import get_transaction


@contextlib.contextmanager
async def transaction_context_manager() -> Transaction:
    txn = await get_transaction()
    yield txn


def shared_search_transaction():
    """
    Used to decorate endpoint functions so that we are sure that
    the shared transaction context var is started when the endpoint
    starts and always aborted after the request handling has finished.
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            async with transaction_context_manager():
                return await func(*args, **kwargs)

        return wrapper

    return decorator
