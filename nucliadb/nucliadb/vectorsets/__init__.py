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


from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.vectorsets import (
    create_vectorset_indexes,
    delete_vectorset_indexes,
    get_vectorsets_kv,
    set_vectorsets_kv,
)
from nucliadb.vectorsets.exceptions import VectorsetConflictError
from nucliadb.vectorsets.models import VectorSet, VectorSets
from nucliadb_models.vectors import VectorSet as CreateVectorSetPayload
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature


async def get_vectorsets(kbid: str) -> VectorSets:
    async with datamanagers.with_transaction(read_only=True) as txn:
        return await get_vectorsets_kv(txn, kbid=kbid) or VectorSets(vectorsets={})


async def create_vectorset(
    kbid: str, vectorset_id: str, payload: CreateVectorSetPayload
):
    # TODO: handle rollbacks in case of errors
    # TODO: can there be race conditions here when two simultaneous requests to create vectorsets?
    async with datamanagers.with_transaction() as txn:
        vectorsets = await get_vectorsets_kv(txn, kbid=kbid)
        if vectorsets is None:
            vectorsets = VectorSets(vectorsets={})

        if vectorset_id in vectorsets.vectorsets:
            raise VectorsetConflictError()

        if has_feature(const.Features.VECTORSETS_V2_INDEX_NODE_READY):
            await create_vectorset_indexes(
                kbid=kbid,
                semantic_vector_dimension=payload.semantic_vector_size,
                semantic_vector_similarity=str(payload.semantic_vector_similarity),
            )

        vectorset = VectorSet(
            id=vectorset_id,
            semantic_model=payload.semantic_model,
            semantic_vector_similarity=payload.semantic_vector_similarity,
            semantic_vector_size=payload.semantic_vector_size,
            semantic_threshold=payload.semantic_threshold,
            semantic_matryoshka_dimensions=payload.semantic_matryoshka_dimensions or [],
        )

        vectorsets.vectorsets[vectorset_id] = vectorset
        await set_vectorsets_kv(txn, kbid=kbid, vectorsets=vectorsets)
        await txn.commit()


async def delete_vectorset(kbid: str, vectorset_id: str):
    # TODO: can there be race conditions here when two simultaneous requests to create vectorsets?
    async with datamanagers.with_transaction() as txn:
        vectorsets = await get_vectorsets_kv(txn, kbid=kbid)
        if vectorsets is None:
            # No vectorsets in the knowledge box
            return

        if vectorset_id not in vectorsets.vectorsets:
            # Vectorset does not exist or has already been deleted
            return

        del vectorsets.vectorsets[vectorset_id]
        await set_vectorsets_kv(txn, kbid=kbid, vectorsets=vectorsets)
        await txn.commit()

    if has_feature(const.Features.VECTORSETS_V2_INDEX_NODE_READY):
        await delete_vectorset_indexes(kbid=kbid, vectorset_id=vectorset_id)
