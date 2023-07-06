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

from typing import AsyncIterator, List

from nucliadb_protos.dataset_pb2 import (
    Label,
    MultipleTextSameLabels,
    SentenceClassificationBatch,
    TrainSet,
)
from nucliadb_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.cluster.abc import AbstractIndexNode
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train import logger
from nucliadb.train.generators.utils import get_resource_from_cache_or_db


async def get_sentences(kbid: str, result: str) -> List[str]:
    if result.count("/") == 4:
        rid, field_type, field, split_str, _ = result.split("/")
        split = int(split_str)
    else:
        rid, field_type, field, _ = result.split("/")
        split = None

    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return []

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    field_metadata = await field_obj.get_field_metadata()
    if extracted_text is None:
        logger.warning(
            f"{rid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return []

    splitted_texts = []

    if split is not None:
        text = extracted_text.split_text[split]
        for paragraph in field_metadata.split_metadata[split].paragraphs:
            if paragraph.key == "":
                key = f"{rid}/{field_type}/{field}/{paragraph.start}-{paragraph.end}"
            else:
                key = paragraph.key
            if key == result:
                for sentence in paragraph.sentences:
                    splitted_text = text[sentence.start : sentence.end]
                    splitted_texts.append(splitted_text)
    else:
        text = extracted_text.text
        for paragraph in field_metadata.metadata.paragraphs:
            if paragraph.key == "":
                key = f"{rid}/{field_type}/{field}/{paragraph.start}-{paragraph.end}"
            else:
                key = paragraph.key
            if key == result:
                for sentence in paragraph.sentences:
                    splitted_text = text[sentence.start : sentence.end]
                    splitted_texts.append(splitted_text)
    return splitted_texts


async def generate_sentence_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
) -> AsyncIterator[SentenceClassificationBatch]:
    labelsets = []
    # Query how many paragraphs has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    for label in trainset.filter.labels:
        labelset = f"/l/{trainset.filter.labels[0]}"
        labelsets.append(labelset)
        request.filter.tags.append(labelset)
    batch = SentenceClassificationBatch()

    async for paragraph_item in node.stream_get_paragraphs(request):
        text_labels: List[str] = []
        for label in paragraph_item.labels:
            for labelset in labelsets:
                if label.startswith(labelset):
                    text_labels.append(label)

        tl = MultipleTextSameLabels()
        sentences_text = await get_sentences(kbid, paragraph_item.id)

        if len(sentences_text) == 0:
            continue
        for sentence_text in sentences_text:
            tl.text.append(sentence_text)
        if len(tl.text):
            for label in text_labels:
                _, _, labelset, label_title = label.split("/")
                tl.labels.append(Label(labelset=labelset, label=label_title))
        batch.data.append(tl)

        if len(batch.data) % 10 == 0:
            print(len(batch.data))
        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = SentenceClassificationBatch()

    if len(batch.data):
        yield batch
