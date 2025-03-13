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
Atomic datamanagers

This module aims to provide a simple way to call a datamanager function in a
single transaction, avoiding the need of encapsulating like in this example:

```
async def <function>(...):
    async with datamanagers.with_transaction() as txn:
        await datamanagers.<module>.<function>(...)
```

Or simply a more handy way to call an datamanager operation without caring about
it's transaction

"""

from functools import wraps
from typing import Awaitable, Callable, TypeVar

from typing_extensions import Concatenate, ParamSpec

from nucliadb.common.maindb.driver import Transaction

from . import kb as kb_dm
from . import labels as labels_dm
from . import resources as resources_dm
from . import search_configurations as search_configurations_dm
from . import synonyms as synonyms_dm
from .utils import with_ro_transaction, with_transaction

P = ParamSpec("P")
T = TypeVar("T")


def ro_txn_wrap(fun: Callable[Concatenate[Transaction, P], Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    @wraps(fun)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async with with_ro_transaction() as txn:
            return await fun(txn, *args, **kwargs)

    return wrapper


def rw_txn_wrap(fun: Callable[Concatenate[Transaction, P], Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    @wraps(fun)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async with with_transaction() as txn:
            result = await fun(txn, *args, **kwargs)
            await txn.commit()
            return result

    return wrapper


class kb:
    exists_kb = ro_txn_wrap(kb_dm.exists_kb)
    get_config = ro_txn_wrap(kb_dm.get_config)
    get_external_index_provider_metadata = ro_txn_wrap(kb_dm.get_external_index_provider_metadata)


class resources:
    get_resource_uuid_from_slug = ro_txn_wrap(resources_dm.get_resource_uuid_from_slug)
    resource_exists = ro_txn_wrap(resources_dm.resource_exists)
    slug_exists = ro_txn_wrap(resources_dm.slug_exists)


class labelset:
    get = ro_txn_wrap(labels_dm.get_labelset)
    set = rw_txn_wrap(labels_dm.set_labelset)
    delete = rw_txn_wrap(labels_dm.delete_labelset)
    get_all = ro_txn_wrap(labels_dm.get_labels)


class synonyms:
    get = ro_txn_wrap(synonyms_dm.get)
    set = rw_txn_wrap(synonyms_dm.set)


class search_configurations:
    get = ro_txn_wrap(search_configurations_dm.get)
