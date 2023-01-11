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

from typing import List

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import BrokerMessage


def clone_collect_cf(binaries: List[CloudFile], origin: CloudFile):
    cf = CloudFile()
    cf.CopyFrom(origin)
    # Mark the cloud file of the broker message being exported as export source
    # so that it's clear that is part of an export while importing.
    origin.source = CloudFile.Source.EXPORT
    binaries.append(cf)


def collect_cfs(bm: BrokerMessage, binaries: List[CloudFile]):
    for file_field in bm.files.values():
        if file_field.HasField("file"):
            clone_collect_cf(binaries, file_field.file)

    for conversation in bm.conversations.values():
        for message in conversation.messages:
            for attachment in message.content.attachments:
                clone_collect_cf(binaries, attachment)

    for layout in bm.layouts.values():
        for block in layout.body.blocks.values():
            if block.HasField("file"):
                clone_collect_cf(binaries, block.file)

    for field_extracted_data in bm.file_extracted_data:
        if field_extracted_data.HasField("file_thumbnail"):
            clone_collect_cf(binaries, field_extracted_data.file_thumbnail)
        if field_extracted_data.HasField("file_preview"):
            clone_collect_cf(binaries, field_extracted_data.file_preview)
        for file_generated in field_extracted_data.file_generated.values():
            clone_collect_cf(binaries, file_generated)
        for page in field_extracted_data.file_pages_previews.pages:
            clone_collect_cf(binaries, page)

    for link_extracted_data in bm.link_extracted_data:
        if link_extracted_data.HasField("link_thumbnail"):
            clone_collect_cf(binaries, link_extracted_data.link_thumbnail)
        if link_extracted_data.HasField("link_preview"):
            clone_collect_cf(binaries, link_extracted_data.link_preview)
        if link_extracted_data.HasField("link_image"):
            clone_collect_cf(binaries, link_extracted_data.link_image)

    for field_metadata in bm.field_metadata:
        if field_metadata.metadata.metadata.HasField("thumbnail"):
            clone_collect_cf(binaries, field_metadata.metadata.metadata.thumbnail)
