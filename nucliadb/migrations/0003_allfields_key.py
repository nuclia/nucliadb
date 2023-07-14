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
from typing import Optional

from nucliadb_protos.resources_pb2 import AllFieldIDs, FieldID

from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb.migrator import logger
from nucliadb.migrator.context import ExecutionContext


async def migrate(context: ExecutionContext) -> None:
    ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    rdm = ResourcesDataManager(context.kv_driver, context.blob_storage)
    async for resource_id in rdm.iterate_resource_ids(kbid):
        resource = await rdm.get_resource(resource_id)

        if not await needs_migration(resource):
            logger.warning(f"Resource {resource_id} already has all fields key")
            continue

        async with context.kv_driver.transaction() as txn:
            resource.txn = txn
            await migrate_resource(txn, resource)
            await txn.commit()


async def needs_migration(resource: ORMResource) -> bool:
    fields: Optional[AllFieldIDs] = await resource.get_all_field_ids()
    return fields is None


async def migrate_resource(resource: ORMResource) -> None:
    all_fields = AllFieldIDs()
    async for (field_type, field_id) in resource._scan_fields_ids():
        fid = FieldID(field_type=field_type, field=field_id)
        all_fields.fields.append(fid)
    await resource.set_all_field_ids(all_fields)
