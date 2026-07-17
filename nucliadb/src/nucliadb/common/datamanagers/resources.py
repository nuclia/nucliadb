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
from collections.abc import AsyncGenerator

import backoff

from nucliadb.common.datamanagers import fields_v2, resources_v2
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import resources_pb2


async def resource_exists(txn: Transaction, *, kbid: str, rid: str) -> bool:
    return await resources_v2.exists(txn, kbid=kbid, rid=rid)


# id and slug


async def get_resource_uuid_from_slug(txn: Transaction, *, kbid: str, slug: str) -> str | None:
    return await resources_v2.get_resource_uuid_from_slug(txn, kbid=kbid, slug=slug)


async def slug_exists(txn: Transaction, *, kbid: str, slug: str) -> bool:
    return await resources_v2.slug_exists(txn, kbid=kbid, slug=slug)


async def set_slug(txn: Transaction, *, kbid: str, rid: str, slug: str) -> None:
    await resources_v2.set_slug(txn, kbid=kbid, rid=rid, slug=slug)


async def modify_slug(txn: Transaction, *, kbid: str, rid: str, new_slug: str) -> str:
    return await resources_v2.modify_slug(txn, kbid=kbid, rid=rid, new_slug=new_slug)


# resource-shard


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
async def get_resource_shard_id(
    txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
) -> str | None:
    return await resources_v2.get_resource_shard_id(txn, kbid=kbid, rid=rid, for_update=for_update)


async def set_resource_shard_id(txn: Transaction, *, kbid: str, rid: str, shard: str):
    await resources_v2.set_resource_shard_id(txn, kbid=kbid, rid=rid, shard=shard)


# Basic


async def get_basic(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Basic | None:
    return await resources_v2.get_basic(txn, kbid=kbid, rid=rid)


async def set_basic(txn: Transaction, *, kbid: str, rid: str, basic: resources_pb2.Basic):
    await resources_v2.set_basic(txn, kbid=kbid, rid=rid, basic=basic)


# Origin


async def get_origin(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Origin | None:
    return await resources_v2.get_origin(txn, kbid=kbid, rid=rid)


async def set_origin(txn: Transaction, *, kbid: str, rid: str, origin: resources_pb2.Origin):
    await resources_v2.set_origin(txn, kbid=kbid, rid=rid, origin=origin)


# Extra


async def get_extra(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Extra | None:
    return await resources_v2.get_extra(txn, kbid=kbid, rid=rid)


async def set_extra(txn: Transaction, *, kbid: str, rid: str, extra: resources_pb2.Extra):
    await resources_v2.set_extra(txn, kbid=kbid, rid=rid, extra=extra)


# Security


async def get_security(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Security | None:
    return await resources_v2.get_security(txn, kbid=kbid, rid=rid)


async def set_security(txn: Transaction, *, kbid: str, rid: str, security: resources_pb2.Security):
    await resources_v2.set_security(txn, kbid=kbid, rid=rid, security=security)


# KB resource ids (this functions use internal transactions, breaking the
# datamanager contract. We should rethink them at some point)


async def iterate_resource_ids(*, kbid: str) -> AsyncGenerator[str, None]:
    """
    Currently, the implementation of this is optimizing for reducing
    how long a transaction will be open since the caller controls
    how long each item that is yielded will be processed.

    For this reason, it is not using the `txn` argument passed in.
    """
    async for rid in resources_v2.iterate_resource_ids(kbid=kbid):
        yield rid


# KB resource count (materialized key)


async def calculate_number_of_resources(txn: Transaction, *, kbid: str) -> int:
    """
    Calculate the number of resources in a knowledgebox.

    This is usually not very fast at all.

    Long term, we could think about implementing a counter; however,
    right now, a counter would be difficult, require a lot of
    refactoring and not worth much value for the APIs we need
    this feature for.

    Finally, we could also query this data from the node; however,
    it is not the source of truth for the value so it is not ideal
    to move it to the node.
    """
    return await resources_v2.calculate_number_of_resources(txn, kbid=kbid)


# Fields (materialized key with all field ids)


async def get_all_field_ids(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.AllFieldIDs:
    return await fields_v2.get_all_field_ids(txn, kbid=kbid, rid=rid)


async def has_field(txn: Transaction, *, kbid: str, rid: str, field_id: resources_pb2.FieldID) -> bool:
    return await fields_v2.has_field(txn, kbid=kbid, rid=rid, field_id=field_id)


async def delete(txn: Transaction, *, kbid: str, rid: str) -> None:
    await resources_v2.delete(txn, kbid=kbid, rid=rid)
