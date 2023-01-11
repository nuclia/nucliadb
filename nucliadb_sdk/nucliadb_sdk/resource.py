import base64
from typing import Callable, Dict, List, Optional, Union, cast
from uuid import uuid4

import numpy as np

from nucliadb_models.common import Classification, FieldID
from nucliadb_models.common import File as NDBModelsFile
from nucliadb_models.file import FileField
from nucliadb_models.link import LinkField
from nucliadb_models.metadata import Origin, TokenSplit, UserFieldMetadata, UserMetadata
from nucliadb_models.resource import Resource
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString, SlugString
from nucliadb_models.vectors import UserVector, UserVectorWrapper, VectorSet, VectorSets
from nucliadb_models.writer import (
    GENERIC_MIME_TYPE,
    CreateResourcePayload,
    UpdateResourcePayload,
)
from nucliadb_sdk import DEFAULT_LABELSET, logger
from nucliadb_sdk.entities import Entities
from nucliadb_sdk.file import File
from nucliadb_sdk.labels import Label, Labels
from nucliadb_sdk.vectors import Vector, Vectors


def create_resource(
    key: Optional[str] = None,
    text: Optional[str] = None,
    binary: Optional[Union[File, str]] = None,
    labels: Optional[Labels] = None,
    entities: Optional[Entities] = None,
    vectors: Optional[Union[Vectors, Dict[str, Union[np.ndarray, List[float]]]]] = None,
    vectorsets: Optional[VectorSets] = None,
    icon: Optional[str] = None,
) -> CreateResourcePayload:
    create_payload = CreateResourcePayload()
    create_payload.origin = Origin(source=Origin.Source.PYSDK)
    if key is not None:
        create_payload.slug = SlugString(key)
    if icon is not None:
        create_payload.icon = icon
    else:
        create_payload.icon = GENERIC_MIME_TYPE
    main_field = None
    if text is not None:
        create_payload.texts[FieldIdString("text")] = TextField(body=text)
        main_field = FieldID(field_type=FieldID.FieldType.TEXT, field="text")
    if binary is not None:
        if isinstance(binary, str):
            with open(binary, "rb") as binary_file:
                data = binary_file.read()
                binary = File(data=data, filename=binary.split("/")[-1])
        assert isinstance(binary, File)

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
                        labelset=label.labelset
                        if label.labelset is not None
                        else DEFAULT_LABELSET,
                        label=label.label,
                    )
                )
            elif isinstance(label, str):
                if label.count("/") != 1:
                    labelset = DEFAULT_LABELSET
                    label_str = label
                else:
                    labelset, label_str = label.split("/")
                classifications.append(
                    Classification(labelset=labelset, label=label_str)
                )

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
        if isinstance(vectors, dict):
            new_vectors = []
            for key, value in vectors.items():
                if isinstance(value, np.ndarray):
                    list_value = value.tolist()
                else:
                    list_value = value
                new_vectors.append(Vector(value=list_value, vectorset=key))
            vectors = new_vectors
        elif isinstance(vectors, list):
            for vector_element in vectors:
                if isinstance(vector_element.value, np.ndarray):
                    vector_element.value = vector_element.value.tolist()

        uvsw = []
        uvw = UserVectorWrapper(field=main_field)
        uvw.vectors = {}
        generic_positions = (0, len(text) if text is not None else 0)
        for vector in vectors:
            vector_id = vector.key if vector.key is not None else uuid4().hex
            if vectorsets is not None and vector.vectorset not in vectorsets.vectorsets:
                logger.warn("Vectorset is not created, we will create it for you")
                vectorsets.vectorsets[vector.vectorset] = VectorSet(
                    dimension=len(vector.value)
                )
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
    binary: Optional[Union[File, str]] = None,
    labels: Optional[Labels] = None,
    entities: Optional[Entities] = None,
    vectors: Optional[Union[Vectors, Dict[str, Union[np.ndarray, List[float]]]]] = None,
    vectorsets: Optional[VectorSets] = None,
) -> UpdateResourcePayload:
    upload_payload = UpdateResourcePayload()

    main_field = None
    if text is not None:
        upload_payload.texts[FieldIdString("text")] = TextField(body=text)
        main_field = FieldID(field_type=FieldID.FieldType.TEXT, field="text")
    if binary is not None:
        if isinstance(binary, str):
            with open(binary, "rb") as binary_file:
                data = binary_file.read()
                binary = File(data=data, filename=binary.split("/")[-1])

        assert isinstance(binary, File)
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
                        labelset=label.labelset
                        if label.labelset is not None
                        else DEFAULT_LABELSET,
                        label=label.label,
                    )
                )
            elif isinstance(label, str):
                if label.count("/") != 1:
                    logger.warn(f"Labelset default linked to label {label}")
                    labelset = DEFAULT_LABELSET
                    label_str = label
                else:
                    labelset, label_str = label.split("/")

                classifications.append(
                    Classification(labelset=labelset, label=label_str)
                )

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
        if isinstance(vectors, dict):
            new_vectors = []
            for key, value in vectors.items():
                if isinstance(value, np.ndarray):
                    list_value = value.tolist()
                else:
                    list_value = value
                new_vectors.append(Vector(value=list_value, vectorset=key))
            vectors = new_vectors

        uvsw = []
        uvw = UserVectorWrapper(field=main_field)
        uvw.vectors = {}
        generic_positions = (0, len(text) if text is not None else 0)
        for vector in vectors:
            vector_id = vector.key if vector.key is not None else uuid4().hex
            if vectorsets is not None and vector.vectorset not in vectorsets.vectorsets:
                logger.warn("Vectorset is not created, we will create it for you")
                vectorsets.vectorsets[vector.vectorset] = VectorSet(
                    dimension=len(vector.value)
                )

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


def from_resource_to_payload(
    item: Resource,
    download: Callable[[str], bytes],
    update: bool = False,
):
    if update:
        payload: Union[
            UpdateResourcePayload, CreateResourcePayload
        ] = UpdateResourcePayload()
        payload.slug = item.slug  # type: ignore
    else:
        payload = CreateResourcePayload()
        payload.slug = item.id  # type: ignore

    payload.title = item.title
    payload.summary = item.summary
    if item.icon is not None:
        payload.icon = item.icon
    payload.thumbnail = item.thumbnail
    payload.layout = item.layout

    payload.usermetadata = item.usermetadata
    payload.fieldmetadata = item.fieldmetadata

    payload.origin = item.origin

    if item.data is not None and item.data.texts is not None:
        for field, field_payload in item.data.texts.items():
            if field_payload is not None and field_payload.value is not None:
                payload.texts[cast(FieldIdString, field)] = TextField(
                    body=field_payload.value.body, format=field_payload.value.format
                )

    if item.data is not None and item.data.links is not None:
        for field, link_payload in item.data.links.items():
            if link_payload is not None and link_payload.value is not None:
                payload.links[cast(FieldIdString, field)] = LinkField(
                    uri=link_payload.value.uri,
                    headers=link_payload.value.headers,
                    cookies=link_payload.value.cookies,
                    language=link_payload.value.language,
                    localstorage=link_payload.value.localstorage,
                )

    if item.data is not None and item.data.files is not None:
        for field, file_payload in item.data.files.items():
            if (
                file_payload.value is not None
                and file_payload.value is not None
                and file_payload.value.file is not None
                and file_payload.value.file.uri is not None
            ):
                data = download(file_payload.value.file.uri)
                payload.files[cast(FieldIdString, field)] = FileField(
                    language=file_payload.value.language,
                    password=file_payload.value.password,
                    file=NDBModelsFile(
                        payload=base64.b64encode(data),
                        filename=file_payload.value.file.filename,
                        content_type=file_payload.value.file.content_type,
                    ),
                )

    if item.data is not None and item.data.layouts is not None:
        raise NotImplementedError()

    if item.data is not None and item.data.conversations is not None:
        raise NotImplementedError()

    if item.data is not None and item.data.keywordsets is not None:
        raise NotImplementedError()

    if item.data is not None and item.data.datetimes is not None:
        raise NotImplementedError()

    return payload
