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
import logging
from typing import Optional, Union

from pydantic import BaseModel

from nucliadb.common.datamanagers.atomic import kb
from nucliadb_models.search import BaseSearchRequest, MinScore
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

logger = logging.getLogger(__name__)


async def filter_hidden_resources(kbid: str, show_hidden: bool) -> Optional[bool]:
    kb_config = await kb.get_config(kbid=kbid)
    hidden_enabled = kb_config and kb_config.hidden_resources_enabled
    if hidden_enabled and not show_hidden:
        return False
    else:
        return None  # None = No filtering, show all resources


def is_empty_query(request: BaseSearchRequest) -> bool:
    return len(request.query) == 0


def has_user_vectors(request: BaseSearchRequest) -> bool:
    return request.vector is not None and len(request.vector) > 0


def is_exact_match_only_query(request: BaseSearchRequest) -> bool:
    """
    '"something"' -> True
    'foo "something" else' -> False
    """
    query = request.query.strip()
    return len(query) > 0 and query.startswith('"') and query.endswith('"')


def should_disable_vector_search(request: BaseSearchRequest) -> bool:
    if has_user_vectors(request):
        return False

    if is_exact_match_only_query(request):
        return True

    if is_empty_query(request):
        return True

    return False


def min_score_from_query_params(
    min_score_bm25: float,
    min_score_semantic: Optional[float],
    deprecated_min_score: Optional[float],
) -> MinScore:
    # Keep backward compatibility with the deprecated min_score parameter
    semantic = deprecated_min_score if min_score_semantic is None else min_score_semantic
    return MinScore(bm25=min_score_bm25, semantic=semantic)


def min_score_from_payload(min_score: Optional[Union[float, MinScore]]) -> MinScore:
    # Keep backward compatibility with the deprecated
    # min_score payload parameter being a float
    if min_score is None:
        return MinScore(bm25=0, semantic=None)
    elif isinstance(min_score, float):
        return MinScore(bm25=0, semantic=min_score)
    return min_score


def maybe_log_request_payload(kbid: str, endpoint: str, item: BaseModel):
    if has_feature(const.Features.LOG_REQUEST_PAYLOADS, context={"kbid": kbid}, default=False):
        logger.info(
            "Request payload",
            extra={"kbid": kbid, "endpoint": endpoint, "payload": item.model_dump_json()},
        )
