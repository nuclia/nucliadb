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
from nucliadb.ingest.chitchat import build_member_from_json
from nucliadb.ingest.orm.node import NodeType


def test_build_member_from_json():
    member_serial = {
        "id": "node1",
        "address": "10.0.0.1",
        "type": "Io",
        "is_self": "",
        "load_score": "11.52",
        "shard_count": "10",
    }
    member = build_member_from_json(member_serial)

    assert member.node_id == "node1"
    assert member.listen_addr == "10.0.0.1"
    assert member.type == NodeType.IO
    assert member.load_score == 11.52
    assert member.shard_count == 10
    assert not member.is_self

    member_serial["load_score"] = "invalid"
    member_serial["shard_count"] = "invalid"
    member = build_member_from_json(member_serial)
    assert member.load_score == 0
    assert member.shard_count == 0

    member_serial.pop("load_score")
    member_serial.pop("shard_count")
    member = build_member_from_json(member_serial)
    assert member.load_score == 0
    assert member.shard_count == 0
