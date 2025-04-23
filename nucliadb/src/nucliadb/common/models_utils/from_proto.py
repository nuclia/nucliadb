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


from typing import Any

from google.protobuf.json_format import MessageToDict

from nucliadb_models.common import Classification, FieldID, FieldTypeName, QuestionAnswers
from nucliadb_models.conversation import Conversation, FieldConversation
from nucliadb_models.entities import EntitiesGroup, EntitiesGroupSummary, Entity
from nucliadb_models.extracted import (
    ExtractedText,
    FieldComputedMetadata,
    FieldMetadata,
    FieldQuestionAnswers,
    FileExtractedData,
    LargeComputedMetadata,
    LinkExtractedData,
    VectorObject,
)
from nucliadb_models.file import FieldFile
from nucliadb_models.internal.shards import KnowledgeboxShards
from nucliadb_models.link import FieldLink
from nucliadb_models.metadata import (
    ComputedMetadata,
    Extra,
    FieldClassification,
    Metadata,
    Origin,
    Relation,
    RelationMetadata,
    RelationNodeType,
    RelationType,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_models.resource import KnowledgeBoxConfig
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_models.text import FieldText
from nucliadb_protos import knowledgebox_pb2, resources_pb2, utils_pb2, writer_pb2


def field_type_name(field_type: resources_pb2.FieldType.ValueType) -> FieldTypeName:
    return {
        resources_pb2.FieldType.LINK: FieldTypeName.LINK,
        resources_pb2.FieldType.FILE: FieldTypeName.FILE,
        resources_pb2.FieldType.TEXT: FieldTypeName.TEXT,
        resources_pb2.FieldType.GENERIC: FieldTypeName.GENERIC,
        resources_pb2.FieldType.CONVERSATION: FieldTypeName.CONVERSATION,
    }[field_type]


def field_type(field_type: resources_pb2.FieldType.ValueType) -> FieldID.FieldType:
    return {
        resources_pb2.FieldType.LINK: FieldID.FieldType.LINK,
        resources_pb2.FieldType.FILE: FieldID.FieldType.FILE,
        resources_pb2.FieldType.TEXT: FieldID.FieldType.TEXT,
        resources_pb2.FieldType.GENERIC: FieldID.FieldType.GENERIC,
        resources_pb2.FieldType.CONVERSATION: FieldID.FieldType.CONVERSATION,
    }[field_type]


def user_field_metadata(message: resources_pb2.UserFieldMetadata) -> UserFieldMetadata:
    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
        use_integers_for_enums=True,
    )
    value["field"]["field_type"] = field_type_name(value["field"]["field_type"]).value
    return UserFieldMetadata(**value)


def computed_metadata(message: resources_pb2.ComputedMetadata) -> ComputedMetadata:
    values: dict[str, list[FieldClassification]] = {"field_classifications": []}
    for fc in message.field_classifications:
        values["field_classifications"].append(
            FieldClassification(
                field=FieldID(
                    field=fc.field.field,
                    field_type=field_type(fc.field.field_type),
                ),
                classifications=[
                    Classification(label=c.label, labelset=c.labelset) for c in fc.classifications
                ],
            )
        )
    return ComputedMetadata(**values)


def user_metadata(message: resources_pb2.UserMetadata) -> UserMetadata:
    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    return UserMetadata(**value)


RelationNodeTypeMap: dict[RelationNodeType, utils_pb2.RelationNode.NodeType.ValueType] = {
    RelationNodeType.ENTITY: utils_pb2.RelationNode.NodeType.ENTITY,
    RelationNodeType.LABEL: utils_pb2.RelationNode.NodeType.LABEL,
    RelationNodeType.RESOURCE: utils_pb2.RelationNode.NodeType.RESOURCE,
    RelationNodeType.USER: utils_pb2.RelationNode.NodeType.USER,
}

RelationNodeTypePbMap: dict[utils_pb2.RelationNode.NodeType.ValueType, RelationNodeType] = {
    utils_pb2.RelationNode.NodeType.ENTITY: RelationNodeType.ENTITY,
    utils_pb2.RelationNode.NodeType.LABEL: RelationNodeType.LABEL,
    utils_pb2.RelationNode.NodeType.RESOURCE: RelationNodeType.RESOURCE,
    utils_pb2.RelationNode.NodeType.USER: RelationNodeType.USER,
}


RelationTypePbMap: dict[utils_pb2.Relation.RelationType.ValueType, RelationType] = {
    utils_pb2.Relation.RelationType.ABOUT: RelationType.ABOUT,
    utils_pb2.Relation.RelationType.CHILD: RelationType.CHILD,
    utils_pb2.Relation.RelationType.COLAB: RelationType.COLAB,
    utils_pb2.Relation.RelationType.ENTITY: RelationType.ENTITY,
    utils_pb2.Relation.RelationType.OTHER: RelationType.OTHER,
    utils_pb2.Relation.RelationType.SYNONYM: RelationType.SYNONYM,
}

RelationTypeMap: dict[RelationType, utils_pb2.Relation.RelationType.ValueType] = {
    RelationType.ABOUT: utils_pb2.Relation.RelationType.ABOUT,
    RelationType.CHILD: utils_pb2.Relation.RelationType.CHILD,
    RelationType.COLAB: utils_pb2.Relation.RelationType.COLAB,
    RelationType.ENTITY: utils_pb2.Relation.RelationType.ENTITY,
    RelationType.OTHER: utils_pb2.Relation.RelationType.OTHER,
    RelationType.SYNONYM: utils_pb2.Relation.RelationType.SYNONYM,
}


def convert_pb_relation_to_api(rel: utils_pb2.Relation) -> dict[str, Any]:
    return {
        "relation": RelationTypePbMap[rel.relation],
        "from": convert_pb_relation_node_to_api(rel.source),
        "to": convert_pb_relation_node_to_api(rel.to),
        "label": rel.relation_label,
        "metadata": relation_metadata(rel.metadata),
    }


def convert_pb_relation_node_to_api(
    relation_node: utils_pb2.RelationNode,
) -> dict[str, Any]:
    return {
        "type": RelationNodeTypePbMap[relation_node.ntype],
        "value": relation_node.value,
        "group": relation_node.subtype,
    }


def relation_metadata(message: utils_pb2.RelationMetadata) -> RelationMetadata:
    return RelationMetadata(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def relation(message: utils_pb2.Relation) -> Relation:
    value = convert_pb_relation_to_api(message)
    return Relation(**value)


def origin(message: resources_pb2.Origin) -> Origin:
    data = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    # old field was "colaborators" and we want to keep pb field name
    # to avoid migration
    data["collaborators"] = data.pop("colaborators", [])
    return Origin(**data)


def extra(message: resources_pb2.Extra) -> Extra:
    return Extra(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=False,
        )
    )


def metadata(message: resources_pb2.Metadata) -> Metadata:
    return Metadata(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def field_question_answers(
    message: resources_pb2.FieldQuestionAnswers,
) -> FieldQuestionAnswers:
    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    return FieldQuestionAnswers(**value)


def question_answers(message: resources_pb2.QuestionAnswers) -> QuestionAnswers:
    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    return QuestionAnswers(**value)


def extracted_text(message: resources_pb2.ExtractedText) -> ExtractedText:
    return ExtractedText(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def vector_object(message: resources_pb2.VectorObject) -> VectorObject:
    return VectorObject(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def large_computed_metadata(
    message: resources_pb2.LargeComputedMetadata,
) -> LargeComputedMetadata:
    return LargeComputedMetadata(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def link_extracted_data(message: resources_pb2.LinkExtractedData) -> LinkExtractedData:
    return LinkExtractedData(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def file_extracted_data(message: resources_pb2.FileExtractedData) -> FileExtractedData:
    return FileExtractedData(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def shorten_fieldmetadata(
    message: resources_pb2.FieldComputedMetadata,
) -> None:
    large_fields = ["ner", "relations", "positions", "classifications", "entities"]
    for field in large_fields:
        message.metadata.ClearField(field)  # type: ignore
    for metadata in message.split_metadata.values():
        for field in large_fields:
            metadata.ClearField(field)  # type: ignore


def field_computed_metadata(
    message: resources_pb2.FieldComputedMetadata, shortened: bool = False
) -> FieldComputedMetadata:
    if shortened:
        shorten_fieldmetadata(message)
    metadata = field_metadata(message.metadata)
    split_metadata = {
        split: field_metadata(metadata_split) for split, metadata_split in message.split_metadata.items()
    }
    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    value["metadata"] = metadata
    value["split_metadata"] = split_metadata
    return FieldComputedMetadata(**value)


def field_metadata(
    message: resources_pb2.FieldMetadata,
) -> FieldMetadata:
    # Backwards compatibility with old entities format
    # TODO: Remove once deprecated fields are removed
    # If we recieved processor entities in the new field and the old field is empty, we copy them to the old field
    if "processor" in message.entities and len(message.positions) == 0 and len(message.ner) == 0:
        message.ner.update({ent.text: ent.label for ent in message.entities["processor"].entities})
        for ent in message.entities["processor"].entities:
            message.positions[ent.label + "/" + ent.text].entity = ent.text
            message.positions[ent.label + "/" + ent.text].position.extend(
                [
                    resources_pb2.Position(
                        start=position.start,
                        end=position.end,
                    )
                    for position in ent.positions
                ]
            )

    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    value["relations"] = [
        convert_pb_relation_to_api(rel) for relations in message.relations for rel in relations.relations
    ]
    return FieldMetadata(**value)


def conversation(message: resources_pb2.Conversation) -> Conversation:
    as_dict = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    for conv_message in as_dict.get("messages", []):
        for attachment_field in conv_message.get("content", {}).get("attachments_fields", []):
            attachment_field["field_type"] = attachment_field["field_type"].lower()
    return Conversation(**as_dict)


def field_conversation(message: resources_pb2.FieldConversation) -> FieldConversation:
    return FieldConversation(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def entity(message: knowledgebox_pb2.Entity) -> Entity:
    return Entity(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def entities_group(
    message: knowledgebox_pb2.EntitiesGroup,
) -> EntitiesGroup:
    entities_group = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    entities_group["entities"] = {}

    for name, ent in message.entities.items():
        if not ent.deleted:
            entities_group["entities"][name] = entity(ent)

    return EntitiesGroup(**entities_group)


def entities_group_summary(
    message: knowledgebox_pb2.EntitiesGroupSummary,
) -> EntitiesGroupSummary:
    return EntitiesGroupSummary(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def field_file(message: resources_pb2.FieldFile) -> FieldFile:
    instance = FieldFile(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )
    instance.external = (  # type: ignore
        message.file.source == resources_pb2.CloudFile.Source.EXTERNAL
    )
    return instance


def field_link(message: resources_pb2.FieldLink) -> FieldLink:
    return FieldLink(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def field_text(message: resources_pb2.FieldText) -> FieldText:
    return FieldText(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )


def knowledgebox_config(message: knowledgebox_pb2.KnowledgeBoxConfig) -> KnowledgeBoxConfig:
    as_dict = MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    # Calculate external index provider metadata
    # that is shown on read requests
    eip = as_dict.pop("external_index_provider", None)
    if eip:
        as_dict["configured_external_index_provider"] = {"type": eip["type"].lower()}
    return KnowledgeBoxConfig(**as_dict)


def kb_synonyms(message: knowledgebox_pb2.Synonyms) -> KnowledgeBoxSynonyms:
    return KnowledgeBoxSynonyms(
        **dict(
            synonyms={
                term: list(term_synonyms.synonyms) for term, term_synonyms in message.terms.items()
            }
        )
    )


def kb_shards(message: writer_pb2.Shards) -> KnowledgeboxShards:
    return KnowledgeboxShards(
        **MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )
