from nucliadb_dataset.streamer import Streamer
from pyarrow import fs

from datasets import IterableDataset

from nucliadb_dataset import DSTYPE
from nucliadb_protos.train_pb2 import TrainSet
import tensorflow as tf


def test_tf_dataset(knowledgebox: KnowledgeBox):
    # First create a dataset
    # NER / LABELER
    # create a URL

    trainset = TrainSet()
    trainset.kbid = "XXX"
    trainset.type = TrainSet.Type.PARAGRAPH_CLASSIFICATION

    streamer = Streamer()

    ds = tf.data.Dataset.from_generator(
        streamer.get_data,
        output_signature=(
            tf.TensorSpec(shape=(), dtype=tf.int32),
            tf.RaggedTensorSpec(shape=(2, None), dtype=tf.int32),
        ),
    )
