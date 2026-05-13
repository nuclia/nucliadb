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

from typing import overload

from nucliadb_models.common import FieldID, FieldTypeName
from nucliadb_models.search import FeedbackTasks, NucliaDBClientType
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_protos import knowledgebox_pb2, resources_pb2
from nucliadb_protos.audit_pb2 import ClientType, TaskType


def client_type(obj: NucliaDBClientType) -> ClientType.ValueType:
    return ClientType.Value(obj.name)


def feedback_task(obj: FeedbackTasks) -> TaskType.ValueType:
    return TaskType.Value(obj.name)


@overload
def field_type(obj: FieldTypeName) -> resources_pb2.FieldType.ValueType: ...


@overload
def field_type(obj: FieldID.FieldType) -> resources_pb2.FieldType.ValueType: ...


@overload
def field_type(obj: str) -> resources_pb2.FieldType.ValueType: ...


def field_type(obj: FieldTypeName | FieldID.FieldType | str) -> resources_pb2.FieldType.ValueType:
    """Convert field type to protobuf FieldType.ValueType.

    Supports FieldTypeName and FieldID.FieldType enums, also strings.

    Args:
        obj: Either a string, a FieldTypeName or FieldID.FieldType enum value

    Returns:
        The corresponding resources_pb2.FieldType.ValueType
    """
    field_type_map = {
        FieldTypeName.LINK: resources_pb2.FieldType.LINK,
        FieldID.FieldType.LINK: resources_pb2.FieldType.LINK,
        "u": resources_pb2.FieldType.LINK,
        FieldTypeName.FILE: resources_pb2.FieldType.FILE,
        FieldID.FieldType.FILE: resources_pb2.FieldType.FILE,
        "f": resources_pb2.FieldType.FILE,
        FieldTypeName.TEXT: resources_pb2.FieldType.TEXT,
        FieldID.FieldType.TEXT: resources_pb2.FieldType.TEXT,
        "t": resources_pb2.FieldType.TEXT,
        FieldTypeName.GENERIC: resources_pb2.FieldType.GENERIC,
        FieldID.FieldType.GENERIC: resources_pb2.FieldType.GENERIC,
        "a": resources_pb2.FieldType.GENERIC,
        FieldTypeName.CONVERSATION: resources_pb2.FieldType.CONVERSATION,
        FieldID.FieldType.CONVERSATION: resources_pb2.FieldType.CONVERSATION,
        "c": resources_pb2.FieldType.CONVERSATION,
        FieldTypeName.KEY_VALUE: resources_pb2.FieldType.KEY_VALUE,
        FieldID.FieldType.KEY_VALUE: resources_pb2.FieldType.KEY_VALUE,
        "k": resources_pb2.FieldType.KEY_VALUE,
    }
    return field_type_map[obj]


def kb_synonyms(obj: KnowledgeBoxSynonyms) -> knowledgebox_pb2.Synonyms:
    pbsyn = knowledgebox_pb2.Synonyms()
    for term, term_synonyms in obj.synonyms.items():
        pbsyn.terms[term].synonyms.extend(term_synonyms)
    return pbsyn
