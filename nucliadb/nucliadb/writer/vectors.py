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
from nucliadb_protos.writer_pb2 import OpStatusWriter, SetVectorSetRequest

from nucliadb_models.vectors import VectorSimilarity
from nucliadb_utils.utilities import get_ingest


async def create_vectorset(
    kbid: str,
    vectorset: str,
    dimension: Optional[int] = None,
    similarity: Optional[VectorSimilarity] = None,
):
    ingest = get_ingest()
    pbrequest: SetVectorSetRequest = SetVectorSetRequest(id=vectorset)
    pbrequest.kb.uuid = kbid

    if dimension is not None:
        pbrequest.vectorset.dimension = dimension
    if similarity:
        pbrequest.vectorset.similarity = similarity.to_pb()

    status: OpStatusWriter = await ingest.SetVectorSet(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(status_code=500, detail="Error on creating vectorset")
