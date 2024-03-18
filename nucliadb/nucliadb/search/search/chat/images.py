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
        b64encoded=base64.b64encode(
            (await sf.storage.downloadbytes(sf.bucket, sf.key)).read()
        ).decode(),
        content_type="image/png",
    )

    return image


async def get_paragraph_image(kbid: str, paragraph_id: str, reference: str) -> Image:
    storage = await get_storage(service_name=SERVICE_NAME)

    rid, field_type_letter, field_id, _ = paragraph_id.split("/")[:4]

    sf = storage.file_extracted(
        kbid, rid, field_type_letter, field_id, f"generated/{reference}"
    )
    image = Image(
        b64encoded=base64.b64encode(
            (await sf.storage.downloadbytes(sf.bucket, sf.key)).read()
        ).decode(),
        content_type="image/png",
    )

    return image
