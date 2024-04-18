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

import uuid

from nucliadb.ingest.orm.brain import FIELD_PARAGRAPH_ID, FIELD_VECTOR_ID, ResourceBrain
from nucliadb_protos import utils_pb2


def test_apply_field_vectors_for_matryoshka_embeddings():
    STORED_VECTOR_DIMENSION = 100
    MATRYOSHKA_DIMENSION = 10

    rid = uuid.uuid4().hex
    field_id = uuid.uuid4().hex
    vectors = utils_pb2.VectorObject(
        vectors=utils_pb2.Vectors(
            vectors=[
                utils_pb2.Vector(
                    start=0,
                    end=10,
                    start_paragraph=0,
                    end_paragraph=10,
                    vector=[1.0] * STORED_VECTOR_DIMENSION,
                )
            ]
        )
    )
    paragraph_key = FIELD_PARAGRAPH_ID.format(
        rid=rid,
        field_id=field_id,
        paragraph_start=0,
        paragraph_end=10,
    )
    vector_key = FIELD_VECTOR_ID.format(
        rid=rid,
        field_id=field_id,
        index=0,
        vector_start=0,
        vector_end=10,
    )

    brain = ResourceBrain(rid=rid)
    brain.apply_field_vectors(field_id, vectors, matryoshka_vector_dimension=None)
    vector = (
        brain.brain.paragraphs[field_id].paragraphs[paragraph_key].sentences[vector_key]
    )
    assert len(vector.vector) == STORED_VECTOR_DIMENSION

    brain = ResourceBrain(rid=rid)
    brain.apply_field_vectors(
        field_id, vectors, matryoshka_vector_dimension=MATRYOSHKA_DIMENSION
    )
    vector = (
        brain.brain.paragraphs[field_id].paragraphs[paragraph_key].sentences[vector_key]
    )
    assert len(vector.vector) == MATRYOSHKA_DIMENSION
