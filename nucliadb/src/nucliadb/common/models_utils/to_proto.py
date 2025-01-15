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

from nucliadb_models.common import FieldID, FieldTypeName
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
    }[obj]


def field_type(obj: FieldID.FieldType) -> resources_pb2.FieldType.ValueType:
    return {
        FieldID.FieldType.LINK: resources_pb2.FieldType.LINK,
        FieldID.FieldType.FILE: resources_pb2.FieldType.FILE,
        FieldID.FieldType.TEXT: resources_pb2.FieldType.TEXT,
        FieldID.FieldType.GENERIC: resources_pb2.FieldType.GENERIC,
        FieldID.FieldType.CONVERSATION: resources_pb2.FieldType.CONVERSATION,
    }[obj]


def kb_synonyms(obj: KnowledgeBoxSynonyms) -> knowledgebox_pb2.Synonyms:
    pbsyn = knowledgebox_pb2.Synonyms()
    for term, term_synonyms in obj.synonyms.items():
        pbsyn.terms[term].synonyms.extend(term_synonyms)
    return pbsyn
