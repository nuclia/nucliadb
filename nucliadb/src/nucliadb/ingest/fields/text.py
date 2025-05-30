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

import hashlib
from typing import Optional

from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.exceptions import FieldAuthorNotFound
from nucliadb_protos.resources_pb2 import FieldAuthor, FieldText


class Text(Field[FieldText]):
    pbklass = FieldText
    value: FieldText
    type: str = "t"

    async def generated_by(self) -> FieldAuthor:
        value = await self.get_value()
        if value is None:
            raise FieldAuthorNotFound("Field has no value, can't know who generated it")
        return value.generated_by

    async def set_value(self, payload: FieldText):
        if payload.md5 == "":
            payload.md5 = hashlib.md5(payload.body.encode()).hexdigest()
        await self.db_set_value(payload)

    async def get_value(self) -> Optional[FieldText]:
        return await self.db_get_value()
