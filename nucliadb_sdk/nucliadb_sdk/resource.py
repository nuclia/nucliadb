import base64
from typing import Optional
from uuid import uuid4

from nucliadb_models.common import Classification, FieldID
from nucliadb_models.common import File as NDBModelsFile
from nucliadb_models.file import FileField
from nucliadb_models.metadata import Origin, TokenSplit, UserFieldMetadata, UserMetadata
from nucliadb_models.resource import Resource
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString, SlugString
from nucliadb_models.vectors import UserVector, UserVectorWrapper, VectorSets
from nucliadb_models.writer import CreateResourcePayload, UpdateResourcePayload
from nucliadb_sdk.entities import Entities
from nucliadb_sdk.file import File
from nucliadb_sdk.labels import Label, Labels
from nucliadb_sdk.vectors import Vectors


def create_resource(
    key: Optional[str] = None,
    text: Optional[str] = None,
    binary: Optional[File] = None,
    labels: Optional[Labels] = None,
    entities: Optional[Entities] = None,
    vectors: Optional[Vectors] = None,
    vectorsets: Optional[VectorSets] = None,
    icon: Optional[str] = None,
) -> CreateResourcePayload:
    create_payload = CreateResourcePayload()
    create_payload.origin = Origin(source=Origin.Source.PYSDK)
    if key is not None:
        create_payload.slug = SlugString(key)
    if icon is not None:
        create_payload.icon = icon
    main_field = None
    if text is not None:
        create_payload.texts[FieldIdString("text")] = TextField(body=text)
        main_field = FieldID(field_type=FieldID.FieldType.TEXT, field="text")
    if binary is not None:
        create_payload.files[FieldIdString("file")] = FileField(
            file=NDBModelsFile(
                filename=binary.filename,
                content_type=binary.content_type,
                payload=base64.b64encode(binary.data),
            )
        )
        if main_field is None:
            main_field = FieldID(field_type=FieldID.FieldType.FILE, field="file")

    if main_field is None:
        raise AttributeError("Missing field")

    if labels is not None:
        classifications = []
        for label in labels:
            if isinstance(label, Label):
                classifications.append(
                    Classification(
                        labelset=label.labelset if label.labelset is not None else "",
                        label=label.label,
                    )
                )
            elif isinstance(label, str):
                classifications.append(Classification(labelset="", label=label))

        create_payload.usermetadata = UserMetadata(classifications=classifications)

    if entities is not None and text is not None:
        tokens = []
        for entity in entities:
            for position in entity.positions:
                tokens.append(
                    TokenSplit(
                        token=entity.value,
                        klass=entity.type,
                        start=position[0],
                        end=position[1],
                    )
                )

        create_payload.fieldmetadata = []
        create_payload.fieldmetadata.append(
            UserFieldMetadata(
                token=tokens,
                field=main_field,
            )
        )

    if vectors is not None:
        uvsw = []
        uvw = UserVectorWrapper(field=main_field)
        uvw.vectors = {}
        generic_positions = (0, len(text) if text is not None else 0)
        for vector in vectors:
            vector_id = vector.key if vector.key is not None else uuid4().hex
            if vectorsets is not None and vector.vectorset not in vectorsets.vectorsets:
                raise KeyError("Vectorset is not enabled")
            uvw.vectors[vector.vectorset] = {
                vector_id: UserVector(
                    vector=vector.value,
                    positions=generic_positions
                    if vector.positions is None
                    else vector.positions,
                )
            }
        uvsw.append(uvw)
        create_payload.uservectors = uvsw

    return create_payload


def update_resource(
    resource: Resource,
    text: Optional[str] = None,
    binary: Optional[File] = None,
    labels: Optional[Labels] = None,
    entities: Optional[Entities] = None,
    vectors: Optional[Vectors] = None,
    vectorsets: Optional[VectorSets] = None,
) -> UpdateResourcePayload:
    upload_payload = UpdateResourcePayload()

    main_field = None
    if text is not None:
        upload_payload.texts[FieldIdString("text")] = TextField(body=text)
        main_field = FieldID(field_type=FieldID.FieldType.TEXT, field="text")
    if binary is not None:
        upload_payload.files[FieldIdString("file")] = FileField(
            file=NDBModelsFile(
                filename=binary.filename,
                content_type=binary.content_type,
                payload=base64.b64encode(binary.data),
            )
        )
        main_field = FieldID(field_type=FieldID.FieldType.FILE, field="file")

    if main_field is None and resource.data is not None:
        if (
            resource.data.texts is not None
            and FieldIdString("text") in resource.data.texts
        ):
            main_field = FieldID(field_type=FieldID.FieldType.TEXT, field="text")

        elif (
            resource.data.files is not None
            and FieldIdString("file") in resource.data.files
        ):
            main_field = FieldID(field_type=FieldID.FieldType.FILE, field="file")

    if labels is not None:
        classifications = []
        for label in labels:
            if isinstance(label, Label):
                classifications.append(
                    Classification(
                        labelset=label.labelset if label.labelset is not None else "",
                        label=label.label,
                    )
                )
            elif isinstance(label, str):
                classifications.append(Classification(labelset="", label=label))

        upload_payload.usermetadata = UserMetadata(classifications=classifications)

    if entities is not None and text is not None:
        tokens = []
        for entity in entities:
            for position in entity.positions:
                tokens.append(
                    TokenSplit(
                        token=entity.value,
                        klass=entity.type,
                        start=position[0],
                        end=position[1],
                    )
                )

        upload_payload.fieldmetadata = []
        upload_payload.fieldmetadata.append(
            UserFieldMetadata(
                token=tokens,
                field=FieldID(field_type=FieldID.FieldType.TEXT, field="text"),
            )
        )

    if vectors is not None:
        uvsw = []
        uvw = UserVectorWrapper(field=main_field)
        uvw.vectors = {}
        generic_positions = (0, len(text) if text is not None else 0)
        for vector in vectors:
            vector_id = vector.key if vector.key is not None else uuid4().hex
            if vectorsets is not None and vector.vectorset not in vectorsets.vectorsets:
                raise KeyError("Vectorset is not enabled")

            uvw.vectors[vector.vectorset] = {
                vector_id: UserVector(
                    vector=vector.value,
                    positions=generic_positions
                    if vector.positions is None
                    else vector.positions,
                )
            }
        uvsw.append(uvw)
        upload_payload.uservectors = uvsw

    return upload_payload
