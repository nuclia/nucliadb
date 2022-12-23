from typing import AsyncIterator, List

from nucliadb_protos.nodereader_pb2 import (
    StreamRequest,
)
from nucliadb_protos.train_pb2 import (
    Label,
    MultipleTextSameLabels,
    ParagraphClassificationBatch,
    SentenceClassificationBatch,
    TextLabel,
    TrainSet,
)

from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train import logger
from nucliadb.train.generators.utils import get_resource_from_cache


async def get_sentences(kbid: str, result: str) -> str:

    if result.count("/") == 5:
        rid, field_type, field, split_str, start_end = result.split("/")
        split = int(split_str)
        start_str, end_str = start_end.split("-")
    else:
        rid, field_type, field, start_end = result.split("/")
        split = None
        start_str, end_str = start_end.split("-")
    start = int(start_str)
    end = int(end_str)

    orm_resource = await get_resource_from_cache(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return ""

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    field_metadata = await field_obj.get_field_metadata()
    if extracted_text is None:
        logger.warn(
            f"{rid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return ""

    splitted_texts = []

    if split is not None:
        text = extracted_text.split_text[split]
        paragraph = next(
            filter(
                lambda x: x.key == result,
                field_metadata.split_metadata[split].paragraphs,
            )
        )
        for sentence in paragraph.sentences:
            splitted_text = text[sentence.start : sentence.end]
            splitted_texts.append(splitted_text)
    else:
        paragraph = next(
            filter(
                lambda x: x.key == result,
                field_metadata.metadata.paragraphs,
            )
        )
        for sentence in paragraph.sentences:
            splitted_text = text[sentence.start : sentence.end]
            splitted_texts.append(splitted_text)
    return splitted_texts


async def generate_sentence_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    node: Node,
    shard_replica_id: str,
) -> AsyncIterator[ParagraphClassificationBatch]:

    labelsets = []
    # Query how many paragraphs has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    for label in trainset.filter.labels:
        labelset = f"/l/{trainset.filter.labels[0]}"
        labelsets.append(labelset)
        request.filter.tags.append(labelset)
    request.reload = True
    batch = SentenceClassificationBatch()

    async for paragraph_item in node.stream_get_paragraphs(request):
        text_labels: List[str] = []
        for label in paragraph_item.labels:
            for labelset in labelsets:
                if label.startswith(labelset):
                    text_labels.append(label)

        tl = MultipleTextSameLabels()
        sentences_text = await get_sentences(kbid, paragraph_item.id)

        for sentence_text in sentences_text:
            tl.text.append = sentence_text
        for label in text_labels:
            _, _, labelset, label_title = label.split("/")
            tl.labels.append(Label(labelset=labelset, label=label_title))
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = SentenceClassificationBatch()

    if len(batch.data):
        yield batch
