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

"""A

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
from typing import _ParamSpec  # type: ignore

from nucliadb.common import datamanagers

# XXX: we are using the not exported _ParamSpec to support 3.9. Whenever we
# upgrade to >= 3.10 we'll be able to use ParamSpecKwargs and improve the
# typing. We are abusing of ParamSpec anywat to better support text editors, so
# we also need to ignore some mypy complains

P = _ParamSpec("P")


def ro_txn_wrap(fun: P) -> P:  # type: ignore
    @wraps(fun)
    async def wrapper(**kwargs: P.kwargs):
        async with datamanagers.with_transaction(read_only=True) as txn:
            return await fun(txn, **kwargs)

    return wrapper


def rw_txn_wrap(fun: P) -> P:  # type: ignore
    @wraps(fun)
    async def wrapper(**kwargs: P.kwargs):
        async with datamanagers.with_transaction() as txn:
            return await fun(txn, **kwargs)

    return wrapper


class kb:
    exists_kb = ro_txn_wrap(datamanagers.kb.exists_kb)


class resources:
    get_resource_uuid_from_slug = ro_txn_wrap(
        datamanagers.resources.get_resource_uuid_from_slug
    )
    resource_exists = ro_txn_wrap(datamanagers.resources.resource_exists)
    slug_exists = ro_txn_wrap(datamanagers.resources.slug_exists)
