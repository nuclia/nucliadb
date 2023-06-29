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
from typing import Union

from fastapi import Body, Header, Request, Response
from fastapi_versioning import version
from nucliadb_protos.resources_pb2 import FieldComputedMetadata
from nucliadb_protos.utils_pb2 import ExtractedText

from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import SERVICE_NAME, logger
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import SendToPredictError
from nucliadb.search.search.exceptions import ResourceNotFoundError
from nucliadb.search.utilities import get_predict
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import AskRequest, AskResponse, TextBlocks
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.utilities import get_storage

ASK_EXAMPLES = {
    "Ask a Resource": {
        "summary": "Ask a question to the document",
        "description": "Ask a question to the document. The whole document is sent as context to the generative AI",
        "value": {
            "question": "Does this document contain personal information?",
        },
    }
}


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/resource/{{rid}}/ask",
    status_code=200,
    name="Ask a Resource",
    summary="Ask a question to a resource",
    description="Ask to the complete content of the resource",
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def resource_ask_endpoint(
    request: Request,
    response: Response,
    kbid: str,
    rid: str,
    item: AskRequest = Body(
        examples=ASK_EXAMPLES, description="Ask a question payload"
    ),
    x_nucliadb_user: str = Header("", description="User Id", include_in_schema=False),
) -> Union[AskResponse, HTTPClientError]:
    try:
        return await resource_ask(kbid, rid, item, user_id=x_nucliadb_user)
    except ResourceNotFoundError:
        return HTTPClientError(status_code=404, detail="Resource not found")
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except SendToPredictError:
        return HTTPClientError(status_code=503, detail="Ask service not available")


async def resource_ask(
    kbid: str,
    rid: str,
    item: AskRequest,
    user_id: str,
) -> AskResponse:
    blocks = await get_resource_text_blocks(kbid, rid)

    predict = get_predict()
    answer = await predict.ask_document(kbid, item.question, blocks, user_id)

    return AskResponse(answer=answer)


async def get_resource_text_blocks(kbid: str, rid: str) -> TextBlocks:
    """
    Iterate over all fields of the resource and get its extracted text.
    Slice file extracted texts by paragraphs.
    """
    blocks = []
    driver = get_driver()
    storage = await get_storage(service_name=SERVICE_NAME)
    async with driver.transaction() as txn:
        kb = KnowledgeBox(txn, storage, kbid)
        orm_resource = await kb.get(rid)
        if orm_resource is None:
            raise ResourceNotFoundError()

        for field_type, field_id in await orm_resource.get_fields_ids():
            field_obj = await orm_resource.get_field(field_id, field_type, load=False)
            etxt = await field_obj.get_extracted_text()
            if etxt is None:
                logger.warning(
                    f"Skipping field {field_id}, as it does not have extracted text yet!"
                )
                continue

            fcm = await field_obj.get_field_metadata()
            if fcm is None:
                logger.warning(f"Field metadata not found for {field_id}")
                blocks.append(get_field_blocks(etxt))
            else:
                blocks.append(get_field_blocks_split_by_paragraphs(etxt, fcm))
    return blocks


def get_field_blocks_split_by_paragraphs(
    etxt: ExtractedText, fcm: FieldComputedMetadata
) -> list[str]:
    block = []
    for paragraph in fcm.metadata.paragraphs:
        block.append(etxt.text[paragraph.start : paragraph.end])

    for split, metadata in fcm.split_metadata.items():
        for split_paragraph in metadata.paragraphs:
            split_text = etxt.split_text.get(split)
            if split_text is None:
                logger.warning(f"Split {split} not found in extracted text")
                continue
            block.append(split_text[split_paragraph.start : split_paragraph.end])
    return block


def get_field_blocks(etxt: ExtractedText) -> list[str]:
    blocks = []
    if etxt.text:
        blocks.append(etxt.text)
    for split_etxt in etxt.split_text.values():
        if split_etxt:
            blocks.append(split_etxt)
    return blocks
