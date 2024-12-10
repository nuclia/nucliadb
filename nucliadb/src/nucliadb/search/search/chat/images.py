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

import base64
from io import BytesIO
from typing import Optional

from nucliadb.common.ids import ParagraphId
from nucliadb.ingest.fields.file import File
from nucliadb.search import SERVICE_NAME
from nucliadb_models.search import Image
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage


async def get_page_image(kbid: str, paragraph_id: ParagraphId, page_number: int) -> Optional[Image]:
    storage = await get_storage(service_name=SERVICE_NAME)
    sf = storage.file_extracted(
        kbid=kbid,
        uuid=paragraph_id.rid,
        field_type=paragraph_id.field_id.type,
        field=paragraph_id.field_id.key,
        key=f"generated/extracted_images_{page_number}.png",
    )
    image_bytes = (await sf.storage.downloadbytes(sf.bucket, sf.key)).read()
    if not image_bytes:
        return None
    image = Image(
        b64encoded=base64.b64encode(image_bytes).decode(),
        content_type="image/png",
    )
    return image


async def get_paragraph_image(kbid: str, paragraph_id: ParagraphId, reference: str) -> Optional[Image]:
    storage = await get_storage(service_name=SERVICE_NAME)
    sf = storage.file_extracted(
        kbid=kbid,
        uuid=paragraph_id.rid,
        field_type=paragraph_id.field_id.type,
        field=paragraph_id.field_id.key,
        key=f"generated/{reference}",
    )
    image_bytes = (await sf.storage.downloadbytes(sf.bucket, sf.key)).read()
    if not image_bytes:
        return None
    image = Image(
        b64encoded=base64.b64encode(image_bytes).decode(),
        content_type="image/png",
    )
    return image


async def get_file_thumbnail_image(file: File) -> Optional[Image]:
    fed = await file.get_file_extracted_data()
    if fed is None or not fed.HasField("file_thumbnail"):
        return None
    storage: Storage = await get_storage(service_name=SERVICE_NAME)
    image_bytes: BytesIO = await storage.downloadbytescf(fed.file_thumbnail)
    value = image_bytes.getvalue()
    if len(value) == 0:
        return None
    image = Image(
        b64encoded=base64.b64encode(value).decode(),
        # We assume the thumbnail is always generated as jpeg by Nuclia processing
        content_type="image/jpeg",
    )
    return image
