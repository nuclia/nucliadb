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
from datetime import datetime

from nucliadb_protos.writer_pb2 import BrokerMessage, OpStatusWriter
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb_protos import resources_pb2 as rpb


def broker_resource(
    kbid: str, rid=None, slug=None, title=None, summary=None
) -> BrokerMessage:
    """
    Returns a broker resource with barebones metadata.
    """
    rid = rid or str(uuid.uuid4())
    slug = slug or f"{rid}slug1"
    bm: BrokerMessage = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        slug=slug,
        type=BrokerMessage.AUTOCOMMIT,
    )
    title = title or "Title Resource"
    summary = summary or "Summary of document"
    bm.basic.icon = "text/plain"
    bm.basic.title = title
    bm.basic.summary = summary
    bm.basic.thumbnail = "doc"
    bm.basic.layout = "default"
    bm.basic.metadata.useful = True
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    bm.origin.source = rpb.Origin.Source.WEB

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = title
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = summary
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.source = BrokerMessage.MessageSource.WRITER
    return bm


async def inject_message(writer: WriterStub, message: BrokerMessage):
    resp = await writer.ProcessMessage([message])  # type: ignore
    assert resp.status == OpStatusWriter.Status.OK
