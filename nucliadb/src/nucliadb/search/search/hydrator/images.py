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
from typing import Optional, cast

from nucliadb.common.ids import FIELD_TYPE_STR_TO_NAME, FieldId, ParagraphId
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.file import File
from nucliadb.search import SERVICE_NAME
from nucliadb_models.common import FieldTypeName
from nucliadb_models.search import Image
from nucliadb_protos import resources_pb2
from nucliadb_utils.utilities import get_storage


async def paragraph_source_image(kbid: str, paragraph: resources_pb2.Paragraph) -> Optional[Image]:
    """Certain paragraphs are extracted from images using techniques like OCR or
    inception. If that's the case, return the original image for this paragraph.

    """
    source_image = paragraph.representation.reference_file

    if paragraph.kind not in (
        resources_pb2.Paragraph.TypeParagraph.OCR,
        resources_pb2.Paragraph.TypeParagraph.INCEPTION,
    ):
        return None

    field_id = ParagraphId.from_string(paragraph.key).field_id

    # Paragraphs extracted from an image store its original image representation
    # in the reference file. The path is incomplete though, as it's stored in
    # the `generated` folder
    image = await download_image(
        kbid,
        field_id,
        f"generated/{source_image}",
        # XXX: we assume all reference files are PNG images, but this actually
        # depends on learning so it's a dangerous assumption. We should check it
        # by ourselves
        mime_type="image/png",
    )
    return image


async def download_image(
    kbid: str, field_id: FieldId, image_path: str, *, mime_type: str
) -> Optional[Image]:
    storage = await get_storage(service_name=SERVICE_NAME)
    sf = storage.file_extracted(
        kbid,
        field_id.rid,
        field_id.type,
        field_id.key,
        image_path,
    )
    raw_image = (await storage.downloadbytes(sf.bucket, sf.key)).getvalue()
    if not raw_image:
        return None
    return Image(content_type=mime_type, b64encoded=base64.b64encode(raw_image).decode())


async def download_page_preview(field: Field, page: int) -> Optional[Image]:
    """Download a specific page preview for a field and return it as an Image.
    As not all fields have previews, this function can return None.

    Page previews are uploaded by learning and shared through a known path with.
    nucliadb

    """
    field_type = FIELD_TYPE_STR_TO_NAME[field.type]

    if field_type == FieldTypeName.FILE:
        field = cast(File, field)
        metadata = await field.get_file_extracted_data()

        if metadata is None:
            return None

        assert page <= len(metadata.file_pages_previews.positions), (
            f"paragraph page number {page} should be less or equal to the total file pages previews {len(metadata.file_pages_previews.positions)}"
        )
        image = await download_image(
            field.kbid,
            field.field_id,
            f"generated/extracted_images_{page}.png",
            mime_type="image/png",
        )

    elif field_type == FieldTypeName.LINK:
        # TODO: in case of links, we want to return the link preview, that is a
        # link converted to PDF and screenshotted
        # REVIEW: link preview is an image or a PDF?
        image = None

    elif (
        field_type == FieldTypeName.TEXT
        or field_type == FieldTypeName.CONVERSATION
        or field_type == FieldTypeName.GENERIC
    ):
        # these fields don't have previews
        image = None

    else:  # pragma: no cover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return image
