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
        raise NotImplementedError

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
        raise NotImplementedError

    def send(self, msg: AuditRequest):
        raise NotImplementedError

    def search(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        search: SearchRequest,
        timeit: float,
        resources: int,
        retrieval_rephrased_question: str | None = None,
    ):
        raise NotImplementedError

    def chat(
        self,
        kbid: str,
        user: str,
        client: int,
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
        raise NotImplementedError

    def retrieve(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        retrieval_time: float,
    ):
        # TODO(decoupled-ask): implement audit for /retrieve
        ...

    def report_storage(self, kbid: str, paragraphs: int, fields: int, bytes: int):
        raise NotImplementedError

    def report_resources(
        self,
        *,
        kbid: str,
        resources: int,
    ):
        raise NotImplementedError

    def delete_kb(self, kbid: str):
        raise NotImplementedError

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
        raise NotImplementedError
