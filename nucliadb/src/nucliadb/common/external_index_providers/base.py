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
import abc
import logging

from nucliadb_protos.noderesources_pb2 import Resource

logger = logging.getLogger(__name__)


class ExternalIndexManager(abc.ABC, metaclass=abc.ABCMeta):
    type: str

    def __init__(self, kbid: str):
        self.kbid = kbid

    async def delete_resource(self, resource_uuid: str) -> None:
        logger.info(
            "Deleting resource to external index",
            extra={
                "kbid": self.kbid,
                "rid": resource_uuid,
                "provider": self.type,
            },
        )
        await self._delete_resource(resource_uuid)

    async def index_resource(self, resource_uuid: str, resource_data: Resource) -> None:
        logger.info(
            "Indexing resource to external index",
            extra={
                "kbid": self.kbid,
                "rid": resource_uuid,
                "provider": self.type,
            },
        )
        await self._index_resource(resource_uuid, resource_data)

    @abc.abstractmethod
    async def _delete_resource(self, resource_uuid: str) -> None: ...

    @abc.abstractmethod
    async def _index_resource(self, resource_uuid: str, resource_data: Resource) -> None: ...
