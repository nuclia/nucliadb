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

from nucliadb.search import SERVICE_NAME
from nucliadb_models.search import Image
from nucliadb_utils.utilities import get_storage


async def get_page_image(kbid: str, paragraph_id: str, page: int) -> Image:
    storage = await get_storage(service_name=SERVICE_NAME)

    rid, field_type_letter, field_id, _ = paragraph_id.split("/")[:4]

    sf = storage.file_extracted(
        kbid, rid, field_type_letter, field_id, f"generated/extracted_images_{page}.png"
    )
    image = Image(
        b64encoded=base64.b64encode((await sf.storage.downloadbytes(sf.bucket, sf.key)).read()).decode(),
        content_type="image/png",
    )

    return image


async def get_paragraph_image(kbid: str, paragraph_id: str, reference: str) -> Image:
    storage = await get_storage(service_name=SERVICE_NAME)

    rid, field_type_letter, field_id, _ = paragraph_id.split("/")[:4]

    sf = storage.file_extracted(kbid, rid, field_type_letter, field_id, f"generated/{reference}")
    image = Image(
        b64encoded=base64.b64encode((await sf.storage.downloadbytes(sf.bucket, sf.key)).read()).decode(),
        content_type="image/png",
    )

    return image
