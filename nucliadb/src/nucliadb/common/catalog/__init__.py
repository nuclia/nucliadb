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

from nidx_protos.noderesources_pb2 import Resource as IndexMessage

from nucliadb.common.catalog.dummy import DummyCatalog
from nucliadb.common.catalog.interface import Catalog, CatalogQuery
from nucliadb.common.catalog.pg import PGCatalog
from nucliadb.common.catalog.utils import build_catalog_resource_data
from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.settings import CatalogConfig, settings
from nucliadb_models.search import CatalogFacetsRequest, Resources
from nucliadb_utils.exceptions import ConfigurationError


def get_catalog() -> Catalog:
    if settings.catalog == CatalogConfig.UNSET:
        return DummyCatalog()
    elif settings.catalog == CatalogConfig.PG:
        return PGCatalog()
    else:
        raise ConfigurationError(f"Unknown catalog configuration: {settings.catalog}")


async def catalog_update(txn: Transaction, kbid: str, resource: Resource, index_message: IndexMessage):
    catalog = get_catalog()
    resource_data = build_catalog_resource_data(resource, index_message)
    await catalog.update(txn, kbid, resource.uuid, resource_data)


async def catalog_delete(txn: Transaction, kbid: str, rid: str):
    catalog = get_catalog()
    await catalog.delete(txn, kbid, rid)


async def catalog_search(query: CatalogQuery) -> Resources:
    catalog = get_catalog()
    return await catalog.search(query)


async def catalog_facets(kbid: str, request: CatalogFacetsRequest) -> dict[str, int]:
    catalog = get_catalog()
    return await catalog.facets(kbid, request)
