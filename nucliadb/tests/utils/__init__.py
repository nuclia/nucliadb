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
import uuid
from typing import Optional

from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos.writer_pb2 import BrokerMessage, OpStatusWriter
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import mark_dirty


def broker_resource(
    kbid: str,
    rid: Optional[str] = None,
    slug: Optional[str] = None,
    source: BrokerMessage.MessageSource.ValueType = BrokerMessage.MessageSource.WRITER,
) -> BrokerMessage:
    """
    Returns a broker resource with barebones metadata.
    """
    rid = rid or str(uuid.uuid4()).replace("-", "")
    slug = slug or f"{rid}slug1"

    bmb = BrokerMessageBuilder(kbid=kbid, rid=rid, slug=slug, source=source)
    bmb.with_title("Title Resource")
    bmb.with_summary("Summary of document")
    bm = bmb.build()
    return bm


def broker_resource_with_title_paragraph(
    kbid: str,
    rid: Optional[str] = None,
    slug: Optional[str] = None,
) -> BrokerMessage:
    """
    Returns a broker resource with barebones metadata.
    """
    rid = rid or str(uuid.uuid4()).replace("-", "")
    slug = slug or f"{rid}slug1"

    bmb = BrokerMessageBuilder(kbid=kbid, rid=rid, slug=slug)

    title_builder = bmb.with_title("Title Resource")
    title_builder.with_extracted_paragraph_metadata(rpb.Paragraph(start=0, end=5))

    bmb.with_summary("Summary of document")

    bm = bmb.build()
    return bm


async def inject_message(
    writer: WriterStub,
    message: BrokerMessage,
    timeout: Optional[float] = None,
    wait_for_ready: Optional[bool] = None,
):
    # We should be able to enable this once all tests have been migrated
    # for ev in message.field_vectors:
    #     assert ev.vectorset_id, "Vectorset ID must be set in ExtractedVectorsWrapper!"
    #     assert len(ev.vectorset_id) > 0, "Vectorset ID must be set in ExtractedVectorsWrapper!"

    await mark_dirty()
    resp = await writer.ProcessMessage([message], timeout=timeout, wait_for_ready=wait_for_ready)  # type: ignore
    assert resp.status == OpStatusWriter.Status.OK
