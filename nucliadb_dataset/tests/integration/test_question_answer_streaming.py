# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import uuid
from datetime import datetime

import pytest

from integration.utils import export_dataset
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet
from nucliadb_protos.resources_pb2 import (
    Answers,
    CloudFile,
    FieldID,
    FieldQuestionAnswerWrapper,
    FieldType,
    Metadata,
    Origin,
    QuestionAnswer,
)
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    OpStatusWriter,
    Paragraph,
)
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_sdk.v2.sdk import NucliaDB


def test_question_answer_streaming(sdk: NucliaDB, qa_kb: KnowledgeBoxObj):
    trainset = TrainSet()
    trainset.type = TaskType.QUESTION_ANSWER_STREAMING
    trainset.batch_size = 10

    partitions = export_dataset(sdk, trainset, qa_kb)
    assert len(partitions) == 1

    loaded_array = partitions[0]
    assert len(loaded_array) == 3

    assert loaded_array["question"][0] == loaded_array["question"][1]
    assert loaded_array["question"][0] != loaded_array["question"][2]

    assert len(set(loaded_array["answer"])) == 3

    assert len(loaded_array["question_paragraphs"][0]) == 1
    assert len(loaded_array["question_paragraphs"][1]) == 1
    assert len(loaded_array["question_paragraphs"][2]) == 0

    assert len(loaded_array["answer_paragraphs"][0]) == 2
    assert len(loaded_array["answer_paragraphs"][1]) == 1
    assert len(loaded_array["answer_paragraphs"][2]) == 1


@pytest.fixture
def qa_kb(sdk: NucliaDB, kb: KnowledgeBoxObj, ingest_stub_sync: WriterStub) -> KnowledgeBoxObj:
    bm = smb_wonder_bm(kb.uuid)

    inject_message(ingest_stub_sync, bm)
    # wait for processing
    time.sleep(0.1)

    return kb


def smb_wonder_bm(kbid: str) -> BrokerMessage:
    rid = str(uuid.uuid4())
    now = datetime.now()

    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = f"{rid}-slug"
    bm.type = BrokerMessage.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.WRITER

    bm.basic.icon = "text/plain"
    bm.basic.thumbnail = "doc"
    bm.basic.metadata.useful = True
    bm.basic.metadata.language = "en"
    bm.basic.metadata.status = Metadata.Status.PROCESSED
    bm.basic.metadata.metadata["key"] = "value"
    bm.basic.created.FromDatetime(now)
    bm.basic.modified.FromDatetime(now)

    bm.origin.source = Origin.Source.API
    bm.origin.source_id = "My Source"
    bm.origin.created.FromDatetime(now)
    bm.origin.modified.FromDatetime(now)

    # title
    title = "Super Mario Bros. Wonder"
    bm.basic.title = title

    title_field_id = FieldID(
        field="title",
        field_type=FieldType.GENERIC,
    )

    title_et = ExtractedTextWrapper(
        field=title_field_id,
    )
    title_et.body.text = title
    bm.extracted_text.append(title_et)

    title_fcmw = FieldComputedMetadataWrapper(
        field=title_field_id,
    )
    title_fcmw.metadata.metadata.last_index.FromDatetime(now)
    title_fcmw.metadata.metadata.last_understanding.FromDatetime(now)
    title_fcmw.metadata.metadata.last_extract.FromDatetime(now)
    title_fcmw.metadata.metadata.paragraphs.append(
        Paragraph(
            start=0,
            end=len(title),
            kind=Paragraph.TypeParagraph.TITLE,
        )
    )
    bm.field_metadata.append(title_fcmw)

    # summary
    summary = "SMB Wonder: the new Mario game from Nintendo"
    bm.basic.summary = summary

    summary_field_id = FieldID(
        field="summary",
        field_type=FieldType.GENERIC,
    )

    summary_et = ExtractedTextWrapper(
        field=summary_field_id,
    )
    summary_et.body.text = summary
    bm.extracted_text.append(summary_et)

    summary_fcmw = FieldComputedMetadataWrapper(
        field=summary_field_id,
    )
    summary_fcmw.metadata.metadata.last_index.FromDatetime(now)
    summary_fcmw.metadata.metadata.last_understanding.FromDatetime(now)
    summary_fcmw.metadata.metadata.last_extract.FromDatetime(now)
    summary_fcmw.metadata.metadata.paragraphs.append(
        Paragraph(
            start=0,
            end=len(summary),
            kind=Paragraph.TypeParagraph.DESCRIPTION,
        )
    )
    bm.field_metadata.append(summary_fcmw)

    # file field
    file_field_id = "smb-wonder"
    file_field_field_id = FieldID(
        field=file_field_id,
        field_type=FieldType.FILE,
    )

    paragraphs = [
        "Super Mario Bros. Wonder (SMB Wonder) is a 2023 platform game developed and published by Nintendo.\n",
        "SMB Wonder is a side-scrolling plaftorm game.\n",
        "As one of eight player characters, the player completes levels across the Flower Kingdom.",
    ]

    file_field_et = ExtractedTextWrapper(
        field=file_field_field_id,
    )
    file_field_et.body.text = "".join(paragraphs)
    bm.extracted_text.append(file_field_et)

    file_field_fcmw = FieldComputedMetadataWrapper(field=file_field_field_id)
    file_field_fcmw.metadata.metadata.last_index.FromDatetime(now)
    file_field_fcmw.metadata.metadata.last_understanding.FromDatetime(now)
    file_field_fcmw.metadata.metadata.last_extract.FromDatetime(now)
    start = 0
    for paragraph in paragraphs:
        end = start + len(paragraph)
        file_field_fcmw.metadata.metadata.paragraphs.append(Paragraph(start=start, end=end))
        start = end
    bm.field_metadata.append(file_field_fcmw)

    bm.files[file_field_id].added.FromDatetime(now)
    bm.files[file_field_id].file.source = CloudFile.Source.EXTERNAL

    # Q&A
    fqaw = FieldQuestionAnswerWrapper(field=file_field_field_id)

    start = 0
    end = len(paragraphs[0])
    paragraph_0_id = f"{rid}/f/{file_field_id}/{start}-{end}"

    start = len(paragraphs[0])
    end = len(paragraphs[0]) + len(paragraphs[1])
    paragraph_1_id = f"{rid}/f/{file_field_id}/{start}-{end}"

    qa1 = QuestionAnswer()
    qa1.question.text = "What is SMB Wonder?"
    qa1.question.language = "en"
    qa1.question.ids_paragraphs.append(paragraph_0_id)

    qa1_a1 = Answers()
    qa1_a1.text = "SMB Wonder is a side-scrolling Nintendo Switch game"
    qa1_a1.language = "en"
    qa1_a1.ids_paragraphs.extend([paragraph_0_id, paragraph_1_id])
    qa1.answers.append(qa1_a1)

    qa1_a2 = Answers()
    qa1_a2.text = "It's the new Mario game for Nintendo Switch"
    qa1_a2.language = "en"
    qa1_a2.ids_paragraphs.append(paragraph_0_id)
    qa1.answers.append(qa1_a2)

    fqaw.question_answers.question_answers.question_answer.append(qa1)

    qa2 = QuestionAnswer()
    qa2.question.text = "Give me an example of side-scrolling game"
    qa2.question.language = "en"

    qa2_a1 = Answers()
    qa2_a1.text = "SMB Wonder game"
    qa2_a1.language = "en"
    qa2_a1.ids_paragraphs.append(paragraph_1_id)
    qa2.answers.append(qa2_a1)

    fqaw.question_answers.question_answers.question_answer.append(qa2)

    bm.question_answers.append(fqaw)

    return bm


def inject_message(ingest_stub_sync: WriterStub, message: BrokerMessage):
    resp = ingest_stub_sync.ProcessMessage(iter([message]))
    assert resp.status == OpStatusWriter.Status.OK
