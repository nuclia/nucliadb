from functools import partial
from typing import Generator
import tensorflow as tf  # type: ignore
from nucliadb_protos.train_pb2 import ParagraphClassificationBatch, TrainSet, Type

from nucliadb_dataset.streamer import Streamer
from nucliadb_sdk.knowledgebox import KnowledgeBox


def tensorflow_paragraph_classifier_transformer(
    tokenizer, mlb, pcb: ParagraphClassificationBatch
) -> Generator[tf.TensorSpec, tf.RaggedTensorSpec]:
    for data in pcb.data:
        X = tokenizer.encode(data.text)
        Y = mlb.fit_transoform(data.labels)
        yield X, Y


def test_tf_dataset(knowledgebox: KnowledgeBox):
    # First create a dataset
    # NER / LABELER
    # create a URL

    trainset = TrainSet()
    trainset.type = Type.PARAGRAPH_CLASSIFICATION

    streamer = Streamer(trainset, knowledgebox.client)

    tokenizer = BertTokenizder.....
    mlb = MultiLabelBinarizer.....

    mapping_function = partial(tensorflow_paragraph_classifier_transformer, tokenizer, mlb)

    streamer.map = mapping_function

    _ = tf.data.Dataset.from_generator(
        streamer.get_data,
        output_signature=(
            tf.TensorSpec(shape=(), dtype=tf.int32),
            tf.RaggedTensorSpec(shape=(2, None), dtype=tf.int32),
        ),
    )
