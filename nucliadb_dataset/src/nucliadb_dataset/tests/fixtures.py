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

import re
import tempfile
from typing import AsyncIterator, Iterator, Optional

import docker  # type: ignore
import grpc
import pytest
from grpc import aio

from nucliadb_models.common import FieldID, UserClassification
from nucliadb_models.entities import CreateEntitiesGroupPayload, Entity
from nucliadb_models.labels import Label, LabelSet, LabelSetKind
from nucliadb_models.metadata import TokenSplit, UserFieldMetadata, UserMetadata
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString, SlugString
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_sdk.v2.sdk import NucliaDB

DOCKER_ENV_GROUPS = re.search(r"//([^:]+)", docker.from_env().api.base_url)
DOCKER_HOST: Optional[str] = DOCKER_ENV_GROUPS.group(1) if DOCKER_ENV_GROUPS else None


@pytest.fixture(scope="function")
def upload_data_field_classification(sdk: NucliaDB, kb: KnowledgeBoxObj):
    sdk.set_labelset(
        kbid=kb.uuid,
        labelset="labelset1",
        content=LabelSet(
            title="labelset1",
            kind=[LabelSetKind.RESOURCES],
            labels=[Label(title="A"), Label(title="B")],
        ),
    )

    sdk.set_labelset(
        kbid=kb.uuid,
        labelset="labelset2",
        content=LabelSet(
            title="labelset2",
            kind=[LabelSetKind.RESOURCES],
            labels=[
                Label(title="C"),
            ],
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc1"),
            texts={FieldIdString("text"): TextField(body="This is my lovely text")},
            usermetadata=UserMetadata(
                classifications=[UserClassification(labelset="labelset1", label="A")]
            ),
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc2"),
            texts={FieldIdString("text"): TextField(body="This is my lovely text2")},
            usermetadata=UserMetadata(
                classifications=[
                    UserClassification(labelset="labelset1", label="B"),
                    UserClassification(labelset="labelset2", label="C"),
                ]
            ),
        ),
    )

    return kb


@pytest.fixture(scope="function")
def upload_data_paragraph_classification(sdk: NucliaDB, kb: KnowledgeBoxObj):
    sdk.set_labelset(
        kbid=kb.uuid,
        labelset="labelset1",
        content=LabelSet(
            title="labelset1",
            kind=[LabelSetKind.PARAGRAPHS],
            labels=[Label(title="label1"), Label(title="label2")],
        ),
    )

    sdk.set_labelset(
        kbid=kb.uuid,
        labelset="labelset2",
        content=LabelSet(
            title="labelset2",
            kind=[LabelSetKind.PARAGRAPHS],
            labels=[
                Label(title="label1"),
                Label(title="label2"),
            ],
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc1"),
            texts={FieldIdString("text"): TextField(body="This is my lovely text")},
            usermetadata=UserMetadata(
                classifications=[
                    UserClassification(labelset="labelset1", label="label1"),
                ]
            ),
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc2"),
            texts={FieldIdString("text"): TextField(body="This is my lovely text2")},
            usermetadata=UserMetadata(
                classifications=[
                    UserClassification(labelset="labelset1", label="label1"),
                    UserClassification(labelset="labelset1", label="label2"),
                ]
            ),
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc3"),
            texts={FieldIdString("text"): TextField(body="Yet another lovely text")},
            usermetadata=UserMetadata(
                classifications=[
                    UserClassification(labelset="labelset1", label="label1"),
                    UserClassification(labelset="labelset2", label="label2"),
                ]
            ),
        ),
    )
    return kb


@pytest.fixture(scope="function")
def upload_data_token_classification(sdk: NucliaDB, kb: KnowledgeBoxObj):
    sdk.create_entitygroup(
        kbid=kb.uuid,
        content=CreateEntitiesGroupPayload(
            group="PERSON",
            entities={
                "ramon": Entity(value="Ramon"),
                "carmen": Entity(value="Carmen Iniesta"),
                "eudald": Entity(value="Eudald Camprubi"),
            },
            title="Animals",
            color="black",
        ),
    )

    sdk.create_entitygroup(
        kbid=kb.uuid,
        content=CreateEntitiesGroupPayload(
            group="ANIMAL",
            entities={
                "cheetah": Entity(value="cheetah"),
                "tiger": Entity(value="tiger"),
                "lion": Entity(value="lion"),
            },
            title="Animals",
            color="black",
        ),
    )
    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc1"),
            texts={FieldIdString("text"): TextField(body="Ramon This is my lovely text")},
            fieldmetadata=[
                UserFieldMetadata(
                    token=[TokenSplit(klass="PERSON", token="Ramon", start=0, end=5)],
                    field=FieldID(field_type=FieldID.FieldType.TEXT, field="text"),
                )
            ],
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc2"),
            texts={
                FieldIdString("text"): TextField(
                    body="Carmen Iniesta shows an amazing classifier to Eudald Camprubi"
                )
            },
            fieldmetadata=[
                UserFieldMetadata(
                    token=[
                        TokenSplit(klass="PERSON", token="Carmen Iniesta", start=0, end=14),
                        TokenSplit(klass="PERSON", token="Eudald Camprubi", start=46, end=61),
                    ],
                    field=FieldID(field_type=FieldID.FieldType.TEXT, field="text"),
                )
            ],
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            slug=SlugString("doc3"),
            texts={
                FieldIdString("text"): TextField(
                    body="Which is the fastest animal, a lion, a tiger or a cheetah?"
                )
            },
            fieldmetadata=[
                UserFieldMetadata(
                    token=[
                        TokenSplit(klass="ANIMAL", token="lion", start=31, end=35),
                        TokenSplit(klass="ANIMAL", token="tiger", start=39, end=44),
                        TokenSplit(klass="ANIMAL", token="cheetah", start=50, end=57),
                    ],
                    field=FieldID(field_type=FieldID.FieldType.TEXT, field="text"),
                )
            ],
        ),
    )

    return kb


@pytest.fixture(scope="function")
def text_editors_kb(sdk: NucliaDB, kb: KnowledgeBoxObj):
    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            title="GNU Emacs",
            slug=SlugString("doc-emacs"),
            summary="An extensible, customizable, free/libre text editor - and more",
            texts={
                FieldIdString("text"): TextField(
                    body="Text won't appear as we are not mocking processing"
                )
            },
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            title="vi",
            slug=SlugString("doc-vi"),
            summary="A screen-oriented text editor originally created for the Unix operating system",
            texts={
                FieldIdString("text"): TextField(
                    body="Text won't appear as we are not mocking processing"
                )
            },
        ),
    )
    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            title="VIM",
            slug=SlugString("doc-vim"),
            summary="Vi IMproved, a programmer's text editor",
            texts={
                FieldIdString("text"): TextField(
                    body="Text won't appear as we are not mocking processing"
                )
            },
        ),
    )
    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            title="ex",
            slug=SlugString("doc-ex"),
            summary="Line editor for Unix systems originally written by Bill Joy in 1976",
            texts={
                FieldIdString("text"): TextField(
                    body="Text won't appear as we are not mocking processing"
                )
            },
        ),
    )
    return kb


@pytest.fixture(scope="function")
def temp_folder():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
@pytest.mark.asyncio
async def ingest_stub(nucliadb) -> AsyncIterator[WriterStub]:
    channel = aio.insecure_channel(f"{nucliadb.host}:{nucliadb.grpc}")
    stub = WriterStub(channel)
    yield stub
    await channel.close(grace=True)


@pytest.fixture
def ingest_stub_sync(nucliadb) -> Iterator[WriterStub]:
    channel = grpc.insecure_channel(f"{nucliadb.host}:{nucliadb.grpc}")
    stub = WriterStub(channel)
    yield stub
    channel.close()
