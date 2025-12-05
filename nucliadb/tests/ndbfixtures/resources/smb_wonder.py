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
import base64
import random

from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.ids import ParagraphId
from nucliadb.writer.api.v1.router import KB_PREFIX
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync


async def smb_wonder_resource(
    kbid: str, nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub
) -> str:
    """Resource with a PDF file field, extracted vectors and question and
    answers.

    """
    slug = "smb-wonder"
    field_id = "smb-wonder"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "slug": slug,
            "title": "SMB Wonder",
            "files": {
                field_id: {
                    "language": "en",
                    "file": {
                        "filename": "smb_wonder.pdf",
                        "content_type": "application/pdf",
                        "payload": base64.b64encode(b"Super Mario Wonder: the game").decode(),
                    },
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    vectorsets = {}
    async with datamanagers.with_ro_transaction() as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vectorsets[vectorset_id] = vs
    # use a controlled random seed for vector generation
    random.seed(63)

    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        slug=slug,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    bmb.with_title("Super Mario Bros. Wonder")
    bmb.with_summary("SMB Wonder: the new Mario game from Nintendo")

    field_builder = bmb.field_builder(field_id, FieldType.FILE)
    # ids are hardcoded so it's easier to copy while testing and changes on them may break tests
    paragraphs = [
        (
            ParagraphId.from_string(f"{rid}/f/smb-wonder/0-99"),
            "Super Mario Bros. Wonder (SMB Wonder) is a 2023 platform game developed and published by Nintendo.\n",  # noqa
        ),
        (
            ParagraphId.from_string(f"{rid}/f/smb-wonder/99-145"),
            "SMB Wonder is a side-scrolling plaftorm game.\n",
        ),
        (
            ParagraphId.from_string(f"{rid}/f/smb-wonder/145-234"),
            "As one of eight player characters, the player completes levels across the Flower Kingdom.",  # noqa
        ),
    ]
    for expected_paragraph_id, paragraph in paragraphs:
        paragraph_id, _ = field_builder.add_paragraph(
            paragraph,
            vectors={
                vectorset_id: [
                    random.random() for _ in range(config.vectorset_index_config.vector_dimension)
                ]
                for i, (vectorset_id, config) in enumerate(vectorsets.items())
            },
        )
        assert paragraph_id == expected_paragraph_id

    # add Q&A

    question = "What is SMB Wonder?"
    field_builder.add_question_answer(
        question=question,
        question_paragraph_ids=[paragraphs[0][0].full()],
        answer="SMB Wonder is a side-scrolling Nintendo Switch game",
        answer_paragraph_ids=[paragraphs[0][0].full(), paragraphs[1][0].full()],
    )
    field_builder.add_question_answer(
        question=question,
        question_paragraph_ids=[paragraphs[0][0].full()],
        answer="It's the new Mario game for Nintendo Switch",
        answer_paragraph_ids=[paragraphs[0][0].full()],
    )

    question = "Give me an example of side-scrolling game"
    field_builder.add_question_answer(
        question=question,
        answer="SMB Wonder game",
        answer_paragraph_ids=[paragraphs[1][0].full()],
    )

    bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid
