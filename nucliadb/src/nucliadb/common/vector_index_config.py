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

from nidx_protos import nodewriter_pb2 as Nidx

from nucliadb_protos import knowledgebox_pb2 as Nucliadb


def nucliadb_vector_type_to_nidx(nucliadb: Nucliadb.VectorType.ValueType) -> Nidx.VectorType.ValueType:
    if nucliadb == Nucliadb.DENSE_F32:
        return Nidx.DENSE_F32
    else:  # pragma: nocover
        raise Exception("Unknown vector type")


def nucliadb_index_config_to_nidx(nucliadb: Nucliadb.VectorIndexConfig) -> Nidx.VectorIndexConfig:
    return Nidx.VectorIndexConfig(
        normalize_vectors=nucliadb.normalize_vectors,
        similarity=nucliadb.similarity,
        vector_dimension=nucliadb.vector_dimension,
        vector_type=nucliadb_vector_type_to_nidx(nucliadb.vector_type),
    )
