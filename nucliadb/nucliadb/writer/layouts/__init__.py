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
from typing import Any, Callable, Coroutine, Dict

from nucliadb_protos.resources_pb2 import FieldLayout

import nucliadb.models as models
from nucliadb_utils.storages.storage import Storage

VERSION: Dict[
    int,
    Callable[
        [models.InputLayoutField, str, str, str, Storage],
        Coroutine[Any, Any, FieldLayout],
    ],
] = {}

import nucliadb.writer.layouts.v1  # noqa isort:skip


async def serialize_blocks(
    layout_field: models.InputLayoutField,
    kbid: str,
    uuid: str,
    field: str,
    storage: Storage,
) -> FieldLayout:
    if layout_field.format in VERSION:
        layout = await VERSION[layout_field.format](
            layout_field, kbid, uuid, field, storage
        )
    else:
        raise KeyError("Invalid version")
    return layout
