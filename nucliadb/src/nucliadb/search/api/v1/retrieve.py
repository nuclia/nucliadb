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

from time import time

from fastapi import Header, HTTPException, Request
from fastapi_versioning import version

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.models_utils import to_proto
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.query_parser.parsers.retrieve import parse_retrieve
from nucliadb.search.search.retrieval import text_block_search
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.retrieval import (
    Metadata,
    RetrievalMatch,
    RetrievalRequest,
    RetrievalResponse,
    Scores,
)
from nucliadb_models.search import NucliaDBClientType
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_audit


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/retrieve",
    status_code=200,
    description="Search text blocks on a Knowledge Box",
    include_in_schema=False,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def _retrieve_endpoint(
    request: Request,
    kbid: str,
    item: RetrievalRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> RetrievalResponse:
    return await retrieve_endpoint(
        kbid,
        item,
        x_ndb_client=x_ndb_client,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


async def retrieve_endpoint(
    kbid: str,
    item: RetrievalRequest,
    *,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> RetrievalResponse:
    audit = get_audit()
    start_time = time()

    try:
        retrieval = await parse_retrieve(kbid, item)
    except InvalidQueryError as err:
        raise HTTPException(
            status_code=422,
            detail=str(err),
        )

    text_blocks, pb_query, _, _ = await text_block_search(kbid, retrieval)

    # cut the top K, we may have more due to extra results used for rank fusion
    text_blocks = text_blocks[: retrieval.top_k]

    # convert to response models
    matches = [text_block_match_to_retrieval_match(text_block) for text_block in text_blocks]

    if audit is not None:
        retrieval_time = time() - start_time
        audit.retrieve(
            kbid,
            x_nucliadb_user,
            to_proto.client_type(x_ndb_client),
            x_forwarded_for,
            retrieval_time,
            # TODO(decoupled-ask): add interesting things to audit
        )

    return RetrievalResponse(matches=matches)


def text_block_match_to_retrieval_match(item: TextBlockMatch) -> RetrievalMatch:
    return RetrievalMatch(
        id=item.paragraph_id.full(),
        score=Scores(
            value=item.current_score.score,
            source=item.current_score.source,
            type=item.current_score.type,
            history=item.scores,
        ),
        metadata=Metadata(
            field_labels=item.field_labels,
            paragraph_labels=item.paragraph_labels,
            is_an_image=item.is_an_image,
            is_a_table=item.is_a_table,
            source_file=item.representation_file,
            page=item.position.page_number,
            in_page_with_visual=item.page_with_visual,
        ),
    )
