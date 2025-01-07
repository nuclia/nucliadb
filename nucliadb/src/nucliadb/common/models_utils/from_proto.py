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


from google.protobuf.json_format import MessageToDict

from nucliadb_models.common import FieldID, FieldTypeName
from nucliadb_models.metadata import UserFieldMetadata
from nucliadb_protos import resources_pb2


def field_type_name_from_proto(field_type: resources_pb2.FieldType.ValueType) -> FieldTypeName:
    return {
        resources_pb2.FieldType.LINK: FieldTypeName.LINK,
        resources_pb2.FieldType.FILE: FieldTypeName.FILE,
        resources_pb2.FieldType.TEXT: FieldTypeName.TEXT,
        resources_pb2.FieldType.GENERIC: FieldTypeName.GENERIC,
        resources_pb2.FieldType.CONVERSATION: FieldTypeName.CONVERSATION,
    }[field_type]


def field_type_from_proto(field_type: resources_pb2.FieldType.ValueType) -> FieldID.FieldType:
    return {
        resources_pb2.FieldType.LINK: FieldID.FieldType.LINK,
        resources_pb2.FieldType.FILE: FieldID.FieldType.FILE,
        resources_pb2.FieldType.TEXT: FieldID.FieldType.TEXT,
        resources_pb2.FieldType.GENERIC: FieldID.FieldType.GENERIC,
        resources_pb2.FieldType.CONVERSATION: FieldID.FieldType.CONVERSATION,
    }[field_type]


def user_field_metadata_from_proto(message: resources_pb2.UserFieldMetadata) -> UserFieldMetadata:
    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        including_default_value_fields=True,
        use_integers_for_enums=True,
    )
    value["selections"] = [
        MessageToDict(
            selections,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
            use_integers_for_enums=True,
        )
        for selections in message.page_selections
    ]
    value["field"]["field_type"] = field_type_name_from_proto(value["field"]["field_type"]).value
    return UserFieldMetadata(**value)
