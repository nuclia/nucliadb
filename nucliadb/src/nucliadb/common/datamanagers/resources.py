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
from typing import TYPE_CHECKING, AsyncGenerator, Optional

import backoff

from nucliadb.common.datamanagers.utils import get_kv_pb
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.exceptions import ConflictError, NotFoundError

# These should be refactored
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb_protos import resources_pb2
from nucliadb_utils.utilities import get_storage

from .utils import with_ro_transaction

if TYPE_CHECKING:
    from nucliadb.ingest.orm.resource import Resource as ResourceORM


KB_RESOURCE_BASIC = "/kbs/{kbid}/r/{uuid}"
KB_RESOURCE_BASIC_FS = "/kbs/{kbid}/r/{uuid}/basic"  # Only used on FS driver
KB_RESOURCE_ORIGIN = "/kbs/{kbid}/r/{uuid}/origin"
KB_RESOURCE_EXTRA = "/kbs/{kbid}/r/{uuid}/extra"
KB_RESOURCE_SECURITY = "/kbs/{kbid}/r/{uuid}/security"
KB_RESOURCE_RELATIONS = "/kbs/{kbid}/r/{uuid}/relations"

KB_RESOURCE_SLUG_BASE = "/kbs/{kbid}/s/"
KB_RESOURCE_SLUG = f"{KB_RESOURCE_SLUG_BASE}{{slug}}"

KB_RESOURCE_FIELDS = "/kbs/{kbid}/r/{uuid}/f/"

KB_RESOURCE_ALL_FIELDS = "/kbs/{kbid}/r/{uuid}/allfields"
KB_MATERIALIZED_RESOURCES_COUNT = "/kbs/{kbid}/materialized/resources/count"

KB_RESOURCE_SHARD = "/kbs/{kbid}/r/{uuid}/shard"


async def resource_exists(txn: Transaction, *, kbid: str, rid: str) -> bool:
    basic = await get_basic_raw(txn, kbid=kbid, rid=rid)
    return basic is not None


# id and slug


async def get_resource_uuid_from_slug(txn: Transaction, *, kbid: str, slug: str) -> Optional[str]:
    encoded_uuid = await txn.get(KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug, for_update=False))
    if not encoded_uuid:
        return None
    return encoded_uuid.decode()


async def slug_exists(txn: Transaction, *, kbid: str, slug: str) -> bool:
    key = KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug)
    encoded_slug: Optional[bytes] = await txn.get(key)
    return encoded_slug not in (None, b"")


async def modify_slug(txn: Transaction, *, kbid: str, rid: str, new_slug: str) -> str:
    basic = await get_basic(txn, kbid=kbid, rid=rid)
    if basic is None:
        raise NotFoundError()
    old_slug = basic.slug

    uuid_for_new_slug = await get_resource_uuid_from_slug(txn, kbid=kbid, slug=new_slug)
    if uuid_for_new_slug is not None:
        if uuid_for_new_slug == rid:
            # Nothing to change
            return old_slug
        else:
            raise ConflictError(f"Slug {new_slug} already exists")
    key = KB_RESOURCE_SLUG.format(kbid=kbid, slug=old_slug)
    await txn.delete(key)
    key = KB_RESOURCE_SLUG.format(kbid=kbid, slug=new_slug)
    await txn.set(key, rid.encode())
    basic.slug = new_slug
    await set_basic(txn, kbid=kbid, rid=rid, basic=basic)
    return old_slug


# resource-shard


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
async def get_resource_shard_id(
    txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
) -> Optional[str]:
    shard = await txn.get(KB_RESOURCE_SHARD.format(kbid=kbid, uuid=rid, for_update=for_update))
    if shard is not None:
        return shard.decode()
    else:
        return None


async def set_resource_shard_id(txn: Transaction, *, kbid: str, rid: str, shard: str):
    await txn.set(KB_RESOURCE_SHARD.format(kbid=kbid, uuid=rid), shard.encode())


# Basic


async def get_basic(txn: Transaction, *, kbid: str, rid: str) -> Optional[resources_pb2.Basic]:
    raw = await get_basic_raw(txn, kbid=kbid, rid=rid)
    if raw is None:
        return None
    basic = resources_pb2.Basic()
    basic.ParseFromString(raw)
    return basic


async def get_basic_raw(txn: Transaction, *, kbid: str, rid: str) -> Optional[bytes]:
    if ingest_settings.driver == "local":
        raw_basic = await txn.get(KB_RESOURCE_BASIC_FS.format(kbid=kbid, uuid=rid))
    else:
        raw_basic = await txn.get(KB_RESOURCE_BASIC.format(kbid=kbid, uuid=rid))
    return raw_basic


async def set_basic(txn: Transaction, *, kbid: str, rid: str, basic: resources_pb2.Basic):
    if ingest_settings.driver == "local":
        await txn.set(
            KB_RESOURCE_BASIC_FS.format(kbid=kbid, uuid=rid),
            basic.SerializeToString(),
        )
    else:
        await txn.set(
            KB_RESOURCE_BASIC.format(kbid=kbid, uuid=rid),
            basic.SerializeToString(),
        )


# Origin


async def get_origin(txn: Transaction, *, kbid: str, rid: str) -> Optional[resources_pb2.Origin]:
    key = KB_RESOURCE_ORIGIN.format(kbid=kbid, uuid=rid)
    return await get_kv_pb(txn, key, resources_pb2.Origin)


async def set_origin(txn: Transaction, *, kbid: str, rid: str, origin: resources_pb2.Origin):
    key = KB_RESOURCE_ORIGIN.format(kbid=kbid, uuid=rid)
    await txn.set(key, origin.SerializeToString())


# Extra


async def get_extra(txn: Transaction, *, kbid: str, rid: str) -> Optional[resources_pb2.Extra]:
    key = KB_RESOURCE_EXTRA.format(kbid=kbid, uuid=rid)
    return await get_kv_pb(txn, key, resources_pb2.Extra)


async def set_extra(txn: Transaction, *, kbid: str, rid: str, extra: resources_pb2.Extra):
    key = KB_RESOURCE_EXTRA.format(kbid=kbid, uuid=rid)
    await txn.set(key, extra.SerializeToString())


# Security


async def get_security(txn: Transaction, *, kbid: str, rid: str) -> Optional[resources_pb2.Security]:
    key = KB_RESOURCE_SECURITY.format(kbid=kbid, uuid=rid)
    return await get_kv_pb(txn, key, resources_pb2.Security)


async def set_security(txn: Transaction, *, kbid: str, rid: str, security: resources_pb2.Security):
    key = KB_RESOURCE_SECURITY.format(kbid=kbid, uuid=rid)
    await txn.set(key, security.SerializeToString())


# Relations


async def get_relations(txn: Transaction, *, kbid: str, rid: str) -> Optional[resources_pb2.Relations]:
    key = KB_RESOURCE_RELATIONS.format(kbid=kbid, uuid=rid)
    return await get_kv_pb(txn, key, resources_pb2.Relations)


async def set_relations(txn: Transaction, *, kbid: str, rid: str, relations: resources_pb2.Relations):
    key = KB_RESOURCE_RELATIONS.format(kbid=kbid, uuid=rid)
    await txn.set(key, relations.SerializeToString())


# KB resource ids (this functions use internal transactions, breaking the
# datamanager contract. We should rethink them at some point)


async def iterate_resource_ids(*, kbid: str) -> AsyncGenerator[str, None]:
    """
    Currently, the implementation of this is optimizing for reducing
    how long a transaction will be open since the caller controls
    how long each item that is yielded will be processed.

    For this reason, it is not using the `txn` argument passed in.
    """
    batch = []
    async for slug in _iter_resource_slugs(kbid=kbid):
        batch.append(slug)
        if len(batch) >= 200:
            for rid in await _get_resource_ids_from_slugs(kbid=kbid, slugs=batch):
                yield rid
            batch = []
    if len(batch) > 0:
        for rid in await _get_resource_ids_from_slugs(kbid=kbid, slugs=batch):
            yield rid


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
async def _iter_resource_slugs(*, kbid: str) -> AsyncGenerator[str, None]:
    async with with_ro_transaction() as txn:
        async for key in txn.keys(match=KB_RESOURCE_SLUG_BASE.format(kbid=kbid), count=-1):
            yield key.split("/")[-1]


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
async def _get_resource_ids_from_slugs(kbid: str, slugs: list[str]) -> list[str]:
    async with with_ro_transaction() as txn:
        rids = await txn.batch_get([KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug) for slug in slugs])
    return [rid.decode() for rid in rids if rid is not None]


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
    return await txn.count(KB_RESOURCE_SLUG_BASE.format(kbid=kbid))


async def get_number_of_resources(txn: Transaction, *, kbid: str) -> int:
    """
    Return cached number of resources in a knowledgebox.
    """
    raw_value = await txn.get(KB_MATERIALIZED_RESOURCES_COUNT.format(kbid=kbid), for_update=False)
    if raw_value is None:
        return -1
    return int(raw_value)


async def set_number_of_resources(txn: Transaction, kbid: str, value: int) -> None:
    await txn.set(KB_MATERIALIZED_RESOURCES_COUNT.format(kbid=kbid), str(value).encode())


# Fields (materialized key with all field ids)


async def get_all_field_ids(
    txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
) -> Optional[resources_pb2.AllFieldIDs]:
    key = KB_RESOURCE_ALL_FIELDS.format(kbid=kbid, uuid=rid)
    return await get_kv_pb(txn, key, resources_pb2.AllFieldIDs, for_update=for_update)


async def set_all_field_ids(
    txn: Transaction, *, kbid: str, rid: str, allfields: resources_pb2.AllFieldIDs
):
    key = KB_RESOURCE_ALL_FIELDS.format(kbid=kbid, uuid=rid)
    await txn.set(key, allfields.SerializeToString())


async def has_field(txn: Transaction, *, kbid: str, rid: str, field_id: resources_pb2.FieldID) -> bool:
    fields = await get_all_field_ids(txn, kbid=kbid, rid=rid)
    if fields is None:
        return False
    for resource_field_id in fields.fields:
        if field_id == resource_field_id:
            return True
    return False


# ORM mix (this functions shouldn't belong here)


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
async def get_resource(txn: Transaction, *, kbid: str, rid: str) -> Optional["ResourceORM"]:
    """
    Not ideal to return Resource type here but refactoring would
    require a lot of changes.

    At least this isolated that dependency here.
    """
    # prevent circulat imports -- this is not ideal that we have the ORM mix here.
    from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM

    kb_orm = KnowledgeBoxORM(txn, await get_storage(), kbid)
    return await kb_orm.get(rid)
