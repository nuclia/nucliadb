# Copyright 2021 Bosutech XXI S.L.
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

from google.protobuf.timestamp_pb2 import Timestamp
from nidx_protos.nodereader_pb2 import SearchRequest

from nucliadb_protos.audit_pb2 import AuditField, AuditRequest, ChatContext, RetrievedContext
from nucliadb_protos.resources_pb2 import FieldID


class AuditStorage:
    initialized: bool = False

    def report_and_send(
        self,
        *,
        kbid: str,
        audit_type: AuditRequest.AuditType.Value,  # type: ignore
        when: Timestamp | None = None,
        user: str | None = None,
        origin: str | None = None,
        rid: str | None = None,
        field_metadata: list[FieldID] | None = None,
        audit_fields: list[AuditField] | None = None,
    ):
        raise NotImplementedError()

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    def visited(
        self,
        kbid: str,
        uuid: str,
        user: str,
        origin: str,
    ):
        raise NotImplementedError()

    def send(self, msg: AuditRequest):
        raise NotImplementedError()

    def search(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        search: SearchRequest,
        timeit: float,
        resources: int,
        retrieval_rephrased_question: str | None = None,
    ):
        raise NotImplementedError()

    def chat(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        question: str,
        rephrased_question: str | None,
        retrieval_rephrased_question: str | None,
        chat_context: list[ChatContext],
        retrieved_context: list[RetrievedContext],
        answer: str | None,
        reasoning: str | None,
        learning_id: str | None,
        status_code: int,
        model: str | None,
        rephrase_time: float | None = None,
        generative_answer_time: float | None = None,
        generative_answer_first_chunk_time: float | None = None,
        generative_reasoning_first_chunk_time: float | None = None,
    ):
        raise NotImplementedError()

    def retrieve(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        retrieval_time: float,
    ):
        raise NotImplementedError()

    def augment(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        augment_time: float,
    ):
        raise NotImplementedError()

    def report_storage(self, kbid: str, paragraphs: int, fields: int, bytes: int):
        raise NotImplementedError()

    def report_resources(
        self,
        *,
        kbid: str,
        resources: int,
    ):
        raise NotImplementedError()

    def delete_kb(self, kbid: str):
        raise NotImplementedError()

    def feedback(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        learning_id: str,
        good: bool,
        task: int,
        feedback: str | None,
        text_block_id: str | None,
    ):
        raise NotImplementedError()
