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


import datetime

from nucliadb.reader.api.models import ResourceField
from nucliadb_models import Conversation
from nucliadb_models.common import FieldTypeName
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import FieldRef, FieldType, Message


def test_serialize_conversation_field():
    pb = PBConversation()
    m1 = Message()
    m1.timestamp.FromDatetime(datetime.datetime.now())
    m1.content.attachments_fields.append(FieldRef(field_type=FieldType.LINK, field_id="mylink"))
    pb.messages.append(m1)
    resource_field = ResourceField(field_id="conv", field_type=FieldTypeName.CONVERSATION)
    resource_field.value = Conversation.from_message(pb)
    assert resource_field.value.messages[0].content.attachments_fields[0].field_id == "mylink"
    assert (
        resource_field.value.messages[0].content.attachments_fields[0].field_type == FieldTypeName.LINK
    )
