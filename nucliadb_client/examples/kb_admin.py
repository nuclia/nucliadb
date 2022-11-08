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

try:
    import torch
except ImportError:
    torch = None  # type: ignore

from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxID
from nucliadb_protos.train_pb2 import TrainSentence as Sentence
from nucliadb_protos.utils_pb2 import Vector

try:
    import transformers  # type: ignore
except ImportError:
    transformers = None

from nucliadb.models.common import FIELD_TYPES_MAP, FieldTypeName
from nucliadb_client.client import NucliaDBClient
from nucliadb_client.resource import Resource

VECTORS_DIMENSION = 128

DEFAULT_SENTENCE_TRANSFORMER = "all-MiniLM-L6-v2"
ALL_FIELD_TYPES = [field_name.value for field_name in FieldTypeName]


class VectorsRecompute:
    def __init__(self, modelid: str):
        if torch is None:
            raise ImportError("torch lib is required")

        if transformers is None:
            raise ImportError("transformers lib is required")

        self.load_model(modelid)

    def load_model(self, modelid):
        print(f"Loading sentence tokenizer model: {modelid}")
        self._tokenizer = transformers.AutoTokenizer.from_pretrained(
            modelid, model_max_length=VECTORS_DIMENSION
        )
        config = transformers.AutoConfig.from_pretrained(modelid)
        self._model = transformers.AutoModel.from_pretrained(modelid, config=config)

    @property
    def tokenizer(self):
        return self._tokenizer

    @property
    def model(self):
        return self._model

    def recompute_for_resource(self, resource: Resource):
        print("\tGetting existing vectors...")

        previous_vectors = self.get_existing_vectors(resource)

        for index, sentence in enumerate(resource.iter_sentences()):
            existing_vectors = self.get_sentence_vectors(sentence, previous_vectors)
            if existing_vectors is None:
                continue

            new_vectors = self.compute_sentence_vectors(sentence, existing_vectors)
            resource.add_vectors(
                sentence.field.field, sentence.field.field_type, new_vectors
            )

        print(f"\tComputed vectors for {index + 1} sentences.")

    def get_existing_vectors(self, resource: Resource):
        response = resource.get(
            field_type=ALL_FIELD_TYPES,
            show=["basic"],
            extracted=["vectors"],
            timeout=100,
        )
        return response.data

    def get_sentence_vectors(self, sentence: Sentence, existing_vectors):
        field_type = FIELD_TYPES_MAP[sentence.field.field_type]
        if field_type.value == "generic":
            print("Generic fields are not supported yet. Skipping...")
            return None

        attr = field_type.value + "s"
        field_id = sentence.field.field
        try:
            return getattr(existing_vectors, attr)[field_id].extracted.vectors
        except (AttributeError, KeyError):
            print(
                f"Previous vectors not found in sentence: {sentence.sentence} in field {field_id} of type {field_type.value}. Skipping..."  # noqav
            )
            return None

    def compute_sentence_vectors(self, sentence: Sentence, old_vector_data):
        new_vector = Vector()
        old_vector = old_vector_data.vectors.vectors[0]

        # Copy over all vector metadata
        new_vector.start = old_vector.start
        new_vector.end = old_vector.end
        new_vector.start_paragraph = old_vector.start_paragraph
        new_vector.end_paragraph = old_vector.end_paragraph

        embeddings = self.embedder([sentence.metadata.text])
        as_array = embeddings.detach().numpy()[0]
        new_vector.vector.extend(as_array)
        return [new_vector]

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

    def set_kb(self, kbid: str):
        self.kb = self.client.get_kb(kbid=kbid)
        if self.kb is None:
            raise KBNotFoundError("KB not found!")
        self.kbid = kbid
        return self.kb

    def reprocess(self, offset: int = 0):
        for index, resource in enumerate(self.kb.iter_resources()):  # type: ignore
            if index < offset:
                print(f"{index}: Skipping resource: {resource.rid}")
                continue

            print(f"{index}: Sending to reprocess resource: {resource.rid}")
            resource.reprocess()

    def reindex(self, offset: int = 0):
        for index, resource in enumerate(self.kb.iter_resources()):  # type: ignore
            if index < offset:
                print(f"{index}: Skipping resource: {resource.rid}")
                continue

            print(f"{index}: Sending to reindex resource: {resource.rid}")
            resource.reindex()

    def clean_index(self):
        req = KnowledgeBoxID(uuid=self.kbid)
        self.client.writer_stub.CleanAndUpgradeKnowledgeBoxIndex(req)

    def recompute_vectors(
        self, modelid: str = DEFAULT_SENTENCE_TRANSFORMER, offset: int = 0
    ):
        vr = VectorsRecompute(modelid)

        for index, resource in enumerate(self.kb.iter_resources()):  # type: ignore
            if index < offset:
                print(f"{index}: Skipping resource: {resource.rid}")
                continue

            print(f"{index}: Recomputing vectors for resource: {resource.rid}")
            vr.recompute_for_resource(resource)
            if not self.dry_run:
                resource.sync_commit()
                print("\tCommited!")
