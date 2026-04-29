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
from nucliadb.common.maindb.driver import Transaction
from nucliadb_models.kv_schemas import KBKVSchemas, KVSchema

KB_KV_SCHEMAS = "/kbs/{kbid}/kv_schemas"


async def get_all(txn: Transaction, *, kbid: str) -> KBKVSchemas:
    key = KB_KV_SCHEMAS.format(kbid=kbid)
    data = await txn.get(key, for_update=False)
    if not data:
        return KBKVSchemas()
    return KBKVSchemas.model_validate_json(data)


async def get(txn: Transaction, *, kbid: str, name: str) -> KVSchema | None:
    schemas = await get_all(txn, kbid=kbid)
    return schemas.schemas.get(name)


async def set(txn: Transaction, *, kbid: str, schema: KVSchema) -> None:
    key = KB_KV_SCHEMAS.format(kbid=kbid)
    # Use for_update=True to acquire an optimistic lock before writing
    data = await txn.get(key, for_update=True)
    if data:
        schemas = KBKVSchemas.model_validate_json(data)
    else:
        schemas = KBKVSchemas()
    schemas.schemas[schema.name] = schema
    await txn.set(key, schemas.model_dump_json().encode())


async def delete(txn: Transaction, *, kbid: str, name: str) -> bool:
    """Returns True if deleted, False if not found."""
    key = KB_KV_SCHEMAS.format(kbid=kbid)
    data = await txn.get(key, for_update=True)
    if not data:
        return False
    schemas = KBKVSchemas.model_validate_json(data)
    if name not in schemas.schemas:
        return False
    del schemas.schemas[name]
    await txn.set(key, schemas.model_dump_json().encode())
    return True
