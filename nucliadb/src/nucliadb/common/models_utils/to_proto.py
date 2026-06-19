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
from nucliadb_models.conversation import MessageFormat
from nucliadb_models.search import FeedbackTasks, NucliaDBClientType
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_protos import knowledgebox_pb2, resources_pb2
from nucliadb_protos.audit_pb2 import ClientType, TaskType


def client_type(obj: NucliaDBClientType) -> ClientType.ValueType:
    return ClientType.Value(obj.name)


def feedback_task(obj: FeedbackTasks) -> TaskType.ValueType:
    return TaskType.Value(obj.name)


def field_type_name(obj: FieldTypeName) -> resources_pb2.FieldType.ValueType:
    return {
        FieldTypeName.LINK: resources_pb2.FieldType.LINK,
        FieldTypeName.FILE: resources_pb2.FieldType.FILE,
        FieldTypeName.TEXT: resources_pb2.FieldType.TEXT,
        FieldTypeName.GENERIC: resources_pb2.FieldType.GENERIC,
        FieldTypeName.CONVERSATION: resources_pb2.FieldType.CONVERSATION,
        FieldTypeName.KEY_VALUE: resources_pb2.FieldType.KEY_VALUE,
    }[obj]


@overload
def field_type(obj: str) -> resources_pb2.FieldType.ValueType: ...


@overload
def field_type(obj: FieldID.FieldType) -> resources_pb2.FieldType.ValueType: ...


def field_type(obj: FieldID.FieldType | str) -> resources_pb2.FieldType.ValueType:
    return {
        FieldTypeName.LINK.abbreviation(): resources_pb2.FieldType.LINK,
        FieldTypeName.FILE.abbreviation(): resources_pb2.FieldType.FILE,
        FieldTypeName.TEXT.abbreviation(): resources_pb2.FieldType.TEXT,
        FieldTypeName.GENERIC.abbreviation(): resources_pb2.FieldType.GENERIC,
        FieldTypeName.CONVERSATION.abbreviation(): resources_pb2.FieldType.CONVERSATION,
        FieldTypeName.KEY_VALUE.abbreviation(): resources_pb2.FieldType.KEY_VALUE,
        FieldID.FieldType.LINK: resources_pb2.FieldType.LINK,
        FieldID.FieldType.FILE: resources_pb2.FieldType.FILE,
        FieldID.FieldType.TEXT: resources_pb2.FieldType.TEXT,
        FieldID.FieldType.GENERIC: resources_pb2.FieldType.GENERIC,
        FieldID.FieldType.CONVERSATION: resources_pb2.FieldType.CONVERSATION,
        FieldID.FieldType.KEY_VALUE: resources_pb2.FieldType.KEY_VALUE,
    }[obj]


def kb_synonyms(obj: KnowledgeBoxSynonyms) -> knowledgebox_pb2.Synonyms:
    pbsyn = knowledgebox_pb2.Synonyms()
    for term, term_synonyms in obj.synonyms.items():
        pbsyn.terms[term].synonyms.extend(term_synonyms)
    return pbsyn


def conversation_message_format(fmt: MessageFormat) -> resources_pb2.MessageContent.Format.ValueType:
    # The models are not in sync, so we need to do this conversion manually!
    if fmt == MessageFormat.JSON:
        return resources_pb2.MessageContent.Format.JSON
    elif fmt == MessageFormat.KEEP_MARKDOWN:
        return resources_pb2.MessageContent.Format.MARKDOWN
    else:
        return resources_pb2.MessageContent.Format.Value(fmt.value)
