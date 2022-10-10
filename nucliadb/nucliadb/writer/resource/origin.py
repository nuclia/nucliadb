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
from nucliadb_protos.resources_pb2 import Origin

from nucliadb.models import InputOrigin


def parse_origin(origin: Origin, origin_payload: InputOrigin):
    if origin_payload.source_id:
        origin.source_id = origin_payload.source_id
    if origin_payload.url:
        origin.url = origin_payload.url
    if origin_payload.created:
        origin.created.FromDatetime(origin_payload.created)
    if origin_payload.modified:
        origin.modified.FromDatetime(origin_payload.modified)
    if origin_payload.tags:
        origin.tags.extend(origin_payload.tags)
    if origin_payload.colaborators:
        origin.colaborators.extend(origin_payload.colaborators)
    if origin_payload.filename:
        origin.filename = origin_payload.filename
    if origin_payload.related:
        origin.related.extend(origin_payload.related)
    origin.source = Origin.Source.API
