import tensorflow as tf  # type: ignore
from nucliadb_protos.train_pb2 import TrainSet, Type

from nucliadb_dataset.streamer import Streamer
from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_tf_dataset(knowledgebox: KnowledgeBox):
    # First create a dataset
    # NER / LABELER
    # create a URL

    trainset = TrainSet()
    trainset.type = Type.PARAGRAPH_CLASSIFICATION

    streamer = Streamer(trainset, knowledgebox.client)

    _ = tf.data.Dataset.from_generator(
        streamer.get_data,
        output_signature=(
            tf.TensorSpec(shape=(), dtype=tf.int32),
            tf.RaggedTensorSpec(shape=(2, None), dtype=tf.int32),
        ),
    )
