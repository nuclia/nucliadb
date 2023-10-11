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


import uuid
from datetime import datetime

from nucliadb_protos.resources_pb2 import (
    Metadata,
    ParagraphAnnotation,
    UserFieldMetadata,
)
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_protos import resources_pb2 as rpb


def bm_with_paragraph_labels(kbid: str) -> BrokerMessage:
    bm = BrokerMessage()
    bm.kbid = kbid
    rid = str(uuid.uuid4())
    bm.uuid = rid
    bm.slug = f"{rid}-slug"
    bm.type = BrokerMessage.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.WRITER

    bm.basic.icon = "text/plain"
    bm.basic.title = "Title Resource"
    bm.basic.summary = "Summary of document"
    bm.basic.thumbnail = "doc"
    bm.basic.layout = "default"
    bm.basic.metadata.useful = True
    bm.basic.metadata.status = Metadata.Status.PROCESSED
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    bm.origin.source = rpb.Origin.Source.WEB

    c4 = rpb.Classification()
    c4.label = "label_user"
    c4.labelset = "labelset_resources"

    bm.basic.usermetadata.classifications.append(c4)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer? Do you want to go shooping? This is a test!"  # noqa
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    bm.extracted_text.append(etw)

    c3 = rpb.Classification()
    c3.label = "label_user"
    c3.labelset = "labelset_paragraphs"

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/47-64"  # Designed to be the RID at indexing time
    ufm = UserFieldMetadata()
    ufm.paragraphs.append(pa)

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/0-45"  # Designed to be the RID at indexing time
    ufm.paragraphs.append(pa)

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/65-93"  # Designed to be the RID at indexing time
    ufm.paragraphs.append(pa)

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/93-109"  # Designed to be the RID at indexing time
    ufm.paragraphs.append(pa)

    ufm.field.field = "file"
    ufm.field.field_type = rpb.FieldType.FILE
    bm.basic.fieldmetadata.append(ufm)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of document"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Title Resource"
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    c1 = rpb.Classification()
    c1.label = "label_machine"
    c1.labelset = "labelset_paragraphs"

    c2 = rpb.Classification()
    c2.label = "label_machine"
    c2.labelset = "labelset_resources"

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    p1 = rpb.Paragraph(
        start=0,
        end=45,
    )
    p1.classifications.append(c1)
    p2 = rpb.Paragraph(
        start=47,
        end=64,
    )
    p2.classifications.append(c1)

    p3 = rpb.Paragraph(
        start=65,
        end=93,
    )
    p3.classifications.append(c1)

    p4 = rpb.Paragraph(
        start=93,
        end=109,
    )
    p4.classifications.append(c1)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)
    fcm.metadata.metadata.paragraphs.append(p3)
    fcm.metadata.metadata.paragraphs.append(p4)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.ner["Ramon"] = "PERSON"

    fcm.metadata.metadata.classifications.append(c2)
    bm.field_metadata.append(fcm)

    ev = rpb.ExtractedVectorsWrapper()
    ev.field.field = "file"
    ev.field.field_type = rpb.FieldType.FILE

    return bm
