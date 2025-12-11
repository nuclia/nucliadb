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

from nucliadb.models.internal.retrieval import RetrievalRequest, RetrievalResponse
from nucliadb.search.search.metrics import Metrics
from nucliadb_models.augment import AugmentRequest, AugmentResponse
from nucliadb_models.search import FindRequest, KnowledgeboxFindResults, NucliaDBClientType


# TODO: replace this for a sdk.find call when moving /ask to RAO
async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    # REVIEW: once in an SDK metrics, we'll lose track of metrics
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool]:
    from nucliadb.search.search.find import find

    results, incomplete, _ = await find(
        kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for, metrics
    )
    return results, incomplete


# TODO: replace this for a sdk.retrieve call when moving /ask to RAO
async def retrieve(
    kbid: str,
    item: RetrievalRequest,
    *,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> RetrievalResponse:
    from nucliadb.search.api.v1.retrieve import retrieve_endpoint

    return await retrieve_endpoint(
        kbid,
        item,
        x_ndb_client=x_ndb_client,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


# TODO: replace this for a sdk.augment call when moving /ask to RAO
async def augment(kbid: str, item: AugmentRequest) -> AugmentResponse:
    from nucliadb.search.api.v1.augment import augment_endpoint

    return await augment_endpoint(kbid, item)
