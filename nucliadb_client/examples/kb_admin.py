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

from typing import Dict, Iterator, List, Optional

try:
    import torch
except ImportError:
    torch = None  # type: ignore

import time

from grpc import insecure_channel
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxID
from nucliadb_protos.train_pb2 import GetSentencesRequest
from nucliadb_protos.train_pb2 import TrainSentence
from nucliadb_protos.train_pb2 import TrainSentence as Sentence
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import SetVectorsRequest

try:
    import transformers  # type: ignore
except ImportError:
    transformers = None

from datetime import datetime

from stashify_protos.protos.knowledgebox_pb2 import IndexConfig, IndexRequest  # type: ignore
from stashify_protos.protos.knowledgebox_pb2_grpc import LearningServiceStub  # type: ignore

from nucliadb.models.common import FieldTypeName
from nucliadb_client.client import NucliaDBClient

VECTORS_DIMENSION = 128

DEFAULT_SENTENCE_TRANSFORMER = "all-MiniLM-L6-v2"
ALL_FIELD_TYPES = [field_name.value for field_name in FieldTypeName]


class VectorsRecomputer:
    def __init__(self, modelid=None):
        if torch is None:
            raise ImportError("torch lib is required")

        if transformers is None:
            raise ImportError("transformers lib is required")

        self.modelid = modelid
        self.loaded = False
        self.models_folder = "../stashify-models/embbed-sentence"

    def load_model(self, modelid=None):
        if modelid and self.modelid != modelid:
            self.modelid = modelid
            self.loaded = False

        if self.modelid is None:
            raise ValueError("Need to specify a modelid!")

        if not self.loaded:
            model_path = f"{self.models_folder}/{self.modelid}"
            tprint(f"Loading sentence tokenizer model: {model_path}")
            self._tokenizer = transformers.AutoTokenizer.from_pretrained(
                model_path, model_max_length=VECTORS_DIMENSION
            )
            config = transformers.AutoConfig.from_pretrained(model_path)
            self._model = transformers.AutoModel.from_pretrained(
                model_path, config=config
            )
            self.loaded = True

    @property
    def tokenizer(self):
        return self._tokenizer

    @property
    def model(self):
        return self._model

    def get_sentence_start_end(self, sentence: Sentence):
        [start, end] = sentence.sentence.split("/")[-1].split("-")
        return int(start), int(end)

    def get_paragraph_start_end(self, sentence: Sentence):
        [start, end] = sentence.paragraph.split("/")[-1].split("-")
        return int(start), int(end)

    def compute_sentence_vector(self, sentence: Sentence) -> Vector:
        vector = Vector()
        start, end = self.get_sentence_start_end(sentence)
        pstart, pend = self.get_paragraph_start_end(sentence)

        embeddings = self.embedder([sentence.metadata.text])
        as_array = embeddings.detach().numpy()[0]
        vector.start = start
        vector.end = end
        vector.start_paragraph = pstart
        vector.end_paragraph = pend
        vector.vector.extend(as_array)
        return vector

    def embedder(self, sentences):
        encoded_query_input = self.tokenizer(
            sentences, padding=True, truncation=True, return_tensors="pt"
        )
        with torch.no_grad():
            encoded_query_output = self.model(**encoded_query_input)

        sentence_embeddings = self.mean_pooling(
            encoded_query_output, encoded_query_input["attention_mask"]
        )
        return sentence_embeddings

    def mean_pooling(self, model_output, attention_mask):
        token_embeddings = model_output[
            0
        ]  # First element of model_output contains all token embeddings
        input_mask_expanded = (
            attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        )
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
            input_mask_expanded.sum(1), min=1e-9
        )


class KBNotFoundError(Exception):
    ...


def tprint(somestring: str):
    # TODO: Change this for python logging logs
    time = datetime.now().isoformat()
    print(f"[{time}] {somestring}")


class KnowledgeBoxAdmin:
    def __init__(
        self,
        host,
        grpc,
        http,
        train_port=None,
        reader_host=None,
        writer_host=None,
        search_host=None,
        train_host=None,
        grpc_host=None,
        dry_run: bool = False,
        learning_grpc=None,
    ):
        self.client = NucliaDBClient(
            host=host,
            grpc=grpc,
            http=http,
            train=train_port,
            reader_host=reader_host,
            writer_host=writer_host,
            search_host=search_host,
            train_host=train_host,
            grpc_host=grpc_host,
        )
        self.dry_run = dry_run
        self.vr: Optional[VectorsRecomputer] = None
        self.learning_channel = learning_grpc
        self.learning_stub = None
        if self.learning_channel:
            self.learning_stub = LearningServiceStub(
                insecure_channel(self.learning_channel)
            )

    def set_kb(self, kbid: str):
        self.kb = self.client.get_kb(kbid=kbid)
        if self.kb is None:
            raise KBNotFoundError("KB not found!")
        self.kbid = kbid
        return self.kb

    def reprocess(self):
        for resource in self.kb.iter_resources():  # type: ignore
            tprint(f"Reprocessing rid={resource.rid} ... ")
            if not self.dry_run:
                resource.reprocess()

    def reindex(self):
        for resource in self.kb.iter_resources():  # type: ignore
            tprint(f"Reindexing rid={resource.rid} ... ")
            if not self.dry_run:
                resource.reindex(vectors=True)

    def clean_index(self):
        tprint(f"Cleaning and upgrading index...")
        if not self.dry_run:
            req = KnowledgeBoxID(uuid=self.kbid)
            self.client.writer_stub.CleanAndUpgradeKnowledgeBoxIndex(req)

    def iterate_sentences(
        self, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainSentence]:
        request = GetSentencesRequest()
        request.kb.uuid = self.kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        for sentence in self.client.train_stub.GetSentences(request):  # type: ignore
            yield sentence

    def get_sentences_count(self) -> Optional[int]:
        counters = self.kb.counters()  # type: ignore
        if counters is None:
            return None
        return counters.sentences

    def maybe_some_logging(
        self, recomputed: int, total: Optional[int], elapsed_time: float
    ):
        # Some logging first
        if recomputed % 500 == 0:
            speed = int((recomputed / elapsed_time) * 60)
            if total is not None:
                progress = int((recomputed / total) * 100)
                tprint(
                    f"Recomputing {recomputed}-th sentence of kb ({progress}%) at {speed} sentences/min"
                )
            else:
                tprint(
                    f"Recomputing {recomputed}-th sentence of kb at {speed} sentences/min"
                )

    def get_kb_sentence_model(self) -> str:
        if self.learning_stub is None:
            raise ValueError(
                "Need to specify the learning grpc channel with: --learning-grpc"
            )

        req = IndexRequest()
        req.kb = self.kbid
        config: IndexConfig = self.learning_stub.GetConfig(req)
        return config.sentence_model

    def recompute_vectors(self, vr: VectorsRecomputer):
        """
        Iterates all sentences of a KB and recomputes its vectors
        """
        modelid = self.get_kb_sentence_model()
        tprint(f"Sentence model id: {modelid}")

        try:
            vr.load_model(modelid=modelid)
        except OSError:
            tprint(f"You need to download the {modelid} model in {vr.models_folder}")
            exit(0)

        start = time.time()
        sentences_count = self.get_sentences_count()

        previous_sentence = None
        sentence = None
        field_vectors = []
        field_split_vectors: Dict[str, List[Vector]] = {}
        total_recomputed = 0

        for sentence in self.iterate_sentences(labels=False, entities=False, text=True):

            self.maybe_some_logging(
                total_recomputed, sentences_count, time.time() - start
            )

            if previous_sentence is None:
                tprint(
                    f"Recomputing sentences of field {sentence.field.field} of rid={sentence.uuid}"
                )
                previous_sentence = sentence

            elif should_set_field_vectors(previous_sentence, sentence):
                self.set_field_vectors(
                    previous_sentence, field_vectors, field_split_vectors
                )
                tprint(
                    f"Recomputing sentences of field {sentence.field.field} of rid={sentence.uuid}"
                )
                previous_sentence = sentence
                field_vectors = []
                field_split_vectors = {}

            if is_split_field(sentence):
                vector = vr.compute_sentence_vector(sentence)
                split = get_split_subfield(sentence)
                split_vectors = field_split_vectors.setdefault(split, [])
                split_vectors.append(vector)
            else:
                vector = vr.compute_sentence_vector(sentence)
                field_vectors.append(vector)
                total_recomputed += 1

        if sentence is not None and (field_vectors or field_split_vectors):
            self.set_field_vectors(sentence, field_vectors, field_split_vectors)

    def set_field_vectors(
        self,
        sentence: Sentence,
        vectors: List[Vector],
        split_vectors: Dict[str, List[Vector]],
    ):
        req = SetVectorsRequest()
        req.kbid = self.kbid
        req.rid = sentence.uuid
        req.field.CopyFrom(sentence.field)
        for key, svectors in split_vectors.items():
            req.vectors.split_vectors[key].vectors.extend(svectors)
        req.vectors.vectors.vectors.extend(vectors)

        tprint(f"Setting vectors for field {req.field.field} of rid={req.rid}")
        if not self.dry_run:
            self.client.writer_stub.SetVectors(req)


def should_set_field_vectors(previous: Sentence, new: Sentence) -> bool:
    return (
        previous.uuid != new.uuid
        or previous.field.field_type != new.field.field_type
        or previous.field.field != new.field.field
    )


def is_split_field(sentence: Sentence) -> bool:
    id_count = len(sentence.paragraph.split("/"))
    if id_count == 4:
        return False
    elif id_count == 5:
        return True
    else:
        raise ValueError(sentence.paragraph)


def get_split_subfield(sentence: Sentence) -> str:
    (
        _,
        _,
        _,
        subfield,
        _,
        _,
    ) = sentence.paragraph.split("/")
    return subfield
