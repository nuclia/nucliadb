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
from typing import Optional

from fastapi import HTTPException
from nucliadb_protos.resources_pb2 import UserVectorsWrapper
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    GetVectorSetsRequest,
    GetVectorSetsResponse,
    OpStatusWriter,
    SetVectorSetRequest,
    VectorSets,
)

from nucliadb_models.common import FIELD_TYPES_MAP_REVERSE
from nucliadb_models.vectors import UserVectorsWrapper as UserVectorsWrapperPy
from nucliadb_telemetry.utils import set_info_on_span
from nucliadb_utils.utilities import get_ingest


async def create_vectorset(kbid: str, vectorset: str, dimension: Optional[int] = None):
    ingest = get_ingest()
    pbrequest: SetVectorSetRequest = SetVectorSetRequest(id=vectorset)
    pbrequest.kb.uuid = kbid

    set_info_on_span({"nuclia.kbid": kbid})

    if dimension is not None:
        pbrequest.vectorset.dimension = dimension

    status: OpStatusWriter = await ingest.SetVectorSet(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on settings labels on a Knowledge box"
        )


async def get_vectorsets(kbid: str) -> Optional[VectorSets]:
    ingest = get_ingest()
    pbrequest: GetVectorSetsRequest = GetVectorSetsRequest()
    pbrequest.kb.uuid = kbid

    set_info_on_span({"nuclia.kbid": kbid})

    vectorsets: GetVectorSetsResponse = await ingest.GetVectorSets(pbrequest)  # type: ignore
    if vectorsets.status == GetVectorSetsResponse.Status.OK:
        return vectorsets.vectorsets
    else:
        return None


def parse_vectors(
    writer: BrokerMessage, vectors: UserVectorsWrapperPy, vectorsets: VectorSets
):
    for vector in vectors:
        evw = UserVectorsWrapper()
        evw.field.field_type = FIELD_TYPES_MAP_REVERSE[
            vector.field.field_type.value
        ]  # type: ignore
        evw.field.field = vector.field.field
        if vector.vectors is not None:
            for vectorset, user_vectors in vector.vectors.items():
                if vectorset not in vectorsets.vectorsets:
                    raise HTTPException(
                        status_code=412,
                        detail=str(f"Invalid vectorset"),
                    )
                else:
                    dimension = vectorsets.vectorsets[vectorset].dimension
                    for key, user_vector in user_vectors.items():
                        vo = evw.vectors.vectors[vectorset].vectors[key]
                        if len(user_vector.vector) == dimension:
                            if user_vector.vector.count(0) == dimension:
                                raise HTTPException(
                                    status_code=412,
                                    detail=str(f"Invalid vector should not be 0"),
                                )
                            vo.vector.extend(user_vector.vector)
                        else:
                            raise HTTPException(
                                status_code=412,
                                detail=str(
                                    f"Invalid dimension should be {dimension} was {len(user_vector.vector)}"
                                ),
                            )
                        if user_vector.positions is not None:
                            vo.start = user_vector.positions[0]
                            vo.end = user_vector.positions[1]

        if vector.vectors_to_delete is not None:
            for vectorset, user_vector_list in vector.vectors_to_delete.items():
                evw.vectors_to_delete[vectorset].vectors.extend(
                    user_vector_list.vectors
                )

        writer.user_vectors.append(evw)
