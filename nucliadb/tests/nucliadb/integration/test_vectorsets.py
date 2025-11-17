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

import functools
import random
import uuid
from typing import Any, Optional
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from nidx_protos import nodereader_pb2
from pytest_mock import MockerFixture

from nucliadb.common.cluster import manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.nidx import get_nidx_searcher_client
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.search.predict import DummyPredictEngine
from nucliadb.search.requesters import utils
from nucliadb_models.internal.predict import (
    QueryInfo,
)
from nucliadb_protos import (
    resources_pb2,
    utils_pb2,
    writer_pb2,
)
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    set_utility,
)
from tests.ndbfixtures.ingest import make_extracted_text
from tests.nucliadb.knowledgeboxes.vectorsets import KbSpecs
from tests.utils import inject_message
from tests.utils.dirty_index import wait_for_sync
from tests.utils.predict import predict_query_hook

DEFAULT_VECTOR_DIMENSION = 512
VECTORSET_DIMENSION = 12


@pytest.mark.deploy_modes("standalone")
async def test_vectorsets_work_on_a_kb_with_a_single_vectorset(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    kb_with_vectorset: KbSpecs,
):
    kbid = kb_with_vectorset.kbid
    vectorset_id = kb_with_vectorset.vectorset_id
    vectorset_dimension = kb_with_vectorset.vectorset_dimension

    shards = await manager.KBShardManager().get_shards_by_kbid(kbid)
    logic_shard = shards[0]
    shard_id = logic_shard.nidx_shard_id
    await wait_for_sync()

    test_cases = [
        (vectorset_dimension, vectorset_id),
    ]

    for dimension, vectorset in test_cases:
        query_pb = nodereader_pb2.SearchRequest(
            shard=shard_id,
            body="this is a query for my vectorset",
            vector=[1.23] * dimension,
            vectorset=vectorset,
            result_per_page=5,
        )
        results = await get_nidx_searcher_client().Search(query_pb)  # type: ignore
        assert len(results.vector.documents) == 5

    # Test that querying with the wrong dimension raises an exception
    test_cases = [
        (6000, vectorset_id),
        (6000, "multilingual"),
    ]
    for dimension, vectorset in test_cases:
        query_pb = nodereader_pb2.SearchRequest(
            shard=shard_id,
            body="this is a query for my vectorset",
            vector=[1.23] * dimension,
            vectorset=vectorset,
            result_per_page=5,
        )
        with pytest.raises(Exception) as exc:
            results = await get_nidx_searcher_client().Search(query_pb)  # type: ignore
        assert "inconsistent dimensions" in str(exc).lower()


@pytest.mark.parametrize(
    "vectorset,expected",
    [(None, "multilingual"), ("", "multilingual"), ("myvectorset", "myvectorset")],
)
@pytest.mark.deploy_modes("standalone")
async def test_vectorset_parameter_without_default_vectorset(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    vectorset: Optional[str],
    expected: str,
):
    kbid = standalone_knowledgebox

    calls: list[nodereader_pb2.SearchRequest] = []

    async def mock_nidx_query(kbid: str, method, pb_query: nodereader_pb2.SearchRequest, **kwargs):
        calls.append(pb_query)
        results = [nodereader_pb2.SearchResponse()]
        queried_nodes = []  # type: ignore
        return (results, queried_nodes)

    def set_predict_default_vectorset(query_info: QueryInfo) -> QueryInfo:
        assert query_info.sentence is not None
        query_info.sentence.vectors["multilingual"] = [1.0, 2.0, 3.0]
        return query_info

    with (
        predict_query_hook(set_predict_default_vectorset),
        patch(
            "nucliadb.search.api.v1.search.nidx_query",
            new=AsyncMock(side_effect=mock_nidx_query),
        ),
        patch(
            "nucliadb.search.search.retrieval.nidx_query",
            new=AsyncMock(side_effect=mock_nidx_query),
        ),
        patch(
            "nucliadb.common.datamanagers.vectorsets.exists",
            new=AsyncMock(return_value=True),
        ),
    ):
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/search",
            params={"query": "foo", "vectorset": vectorset},
        )
        assert resp.status_code == 200
        assert calls[-1].vectorset == expected

        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/find",
            params={
                "query": "foo",
                "vectorset": vectorset,
            },
        )
        assert resp.status_code == 200
        assert calls[-1].vectorset == expected


@pytest.mark.parametrize(
    "vectorset,expected",
    [(None, "multilingual"), ("", "multilingual"), ("myvectorset", "myvectorset")],
)
@pytest.mark.deploy_modes("standalone")
async def test_vectorset_parameter_with_default_vectorset(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    vectorset,
    expected,
):
    kbid = standalone_knowledgebox

    calls: list[nodereader_pb2.SearchRequest] = []

    async def mock_nidx_query(kbid: str, method, pb_query: nodereader_pb2.SearchRequest, **kwargs):
        calls.append(pb_query)
        results = [nodereader_pb2.SearchResponse()]
        queried_nodes = []  # type: ignore
        return (results, queried_nodes)

    with (
        patch(
            "nucliadb.search.api.v1.search.nidx_query",
            new=AsyncMock(side_effect=mock_nidx_query),
        ),
        patch(
            "nucliadb.search.search.retrieval.nidx_query",
            new=AsyncMock(side_effect=mock_nidx_query),
        ),
        patch(
            "nucliadb.common.datamanagers.vectorsets.exists",
            new=AsyncMock(return_value=True),
        ),
    ):
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/search",
            params={"query": "foo", "vectorset": vectorset},
        )
        assert resp.status_code == 200
        assert calls[-1].vectorset == expected

        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/find",
            params={
                "query": "foo",
                "vectorset": vectorset,
            },
        )
        assert resp.status_code == 200
        assert calls[-1].vectorset == expected


@pytest.mark.deploy_modes("standalone")
async def test_querying_kb_with_vectorsets(
    mocker: MockerFixture,
    storage: Storage,
    maindb_driver: Driver,
    shard_manager,
    learning_config,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    dummy_predict: DummyPredictEngine,
):
    """This tests validates a KB with 1 or 2 vectorsets have functional search
    using or not `vectorset` parameter in search. The point here is not the
    result, but checking the index response.

    """
    query: tuple[Any, Optional[nodereader_pb2.SearchResponse], Optional[Exception]] = (None, None, None)

    async def query_shard_wrapper(shard: str, pb_query: nodereader_pb2.SearchRequest):
        nonlocal query

        from nucliadb.search.search.shards import query_shard

        # this avoids problems with spying an object twice
        nidx = get_nidx_searcher_client()
        if not hasattr(nidx.Search, "spy_return"):
            spy = mocker.spy(nidx, "Search")
        else:
            spy = nidx.Search

        try:
            result = await query_shard(shard, pb_query)
        except Exception as exc:
            query = (spy, None, exc)
            raise
        else:
            query = (spy, result, None)
            return result

    def predict_query_wrapper(original, dimension: int, vectorset_dimensions: dict[str, int]):
        @functools.wraps(original)
        async def inner(*args, **kwargs):
            query_info = await original(*args, **kwargs)
            for vectorset_id, vectorset_dimension in vectorset_dimensions.items():
                query_info.sentence.vectors[vectorset_id] = [1.0] * vectorset_dimension
            return query_info

        return inner

    # KB with one vectorset

    kbid = KnowledgeBox.new_unique_kbid()
    kbslug = "kb-with-one-vectorset"
    kbid, _ = await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=kbslug,
        semantic_models={
            "model": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE, vector_dimension=768
            ),
        },
    )
    rid = uuid.uuid4().hex
    field_id = "my-field"
    bm = create_broker_message_with_vectorsets(kbid, rid, field_id, [("model", 768)])
    await inject_message(nucliadb_ingest_grpc, bm)

    with (
        patch.dict(utils.METHODS, {utils.Method.SEARCH: query_shard_wrapper}, clear=True),
    ):
        with (
            patch.object(
                dummy_predict,
                "query",
                side_effect=predict_query_wrapper(dummy_predict.query, 768, {"model": 768}),
            ),
        ):
            resp = await nucliadb_reader.post(
                f"/kb/{kbid}/find",
                json={
                    "query": "foo",
                },
            )
            assert resp.status_code == 200

            node_search_spy, result, error = query
            assert result is not None
            assert error is None

            request = node_search_spy.call_args[0][0]
            # there's only one model and we get it as the default
            assert request.vectorset == "model"
            assert len(request.vector) == 768

            resp = await nucliadb_reader.post(
                f"/kb/{kbid}/find",
                json={
                    "query": "foo",
                    "vectorset": "model",
                },
            )
            assert resp.status_code == 200

            node_search_spy, result, error = query
            assert result is not None
            assert error is None

            request = node_search_spy.call_args[0][0]
            assert request.vectorset == "model"
            assert len(request.vector) == 768

    # KB with 2 vectorsets

    kbid = KnowledgeBox.new_unique_kbid()
    kbslug = "kb-with-vectorsets"
    kbid, _ = await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=kbslug,
        semantic_models={
            "model-A": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE, vector_dimension=768
            ),
            "model-B": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.DOT, vector_dimension=1024
            ),
        },
    )
    rid = uuid.uuid4().hex
    field_id = "my-field"
    bm = create_broker_message_with_vectorsets(
        kbid, rid, field_id, [("model-A", 768), ("model-B", 1024)]
    )
    await inject_message(nucliadb_ingest_grpc, bm)

    with (
        patch.dict(utils.METHODS, {utils.Method.SEARCH: query_shard_wrapper}, clear=True),
    ):
        with (
            patch.object(
                dummy_predict,
                "query",
                side_effect=predict_query_wrapper(dummy_predict.query, 500, {"model-A": 768}),
            ),
        ):
            resp = await nucliadb_reader.post(
                f"/kb/{kbid}/find",
                json={
                    "query": "foo",
                    "vectorset": "model-A",
                },
            )
            assert resp.status_code == 200

            node_search_spy, result, error = query
            assert result is not None
            assert error is None

            request = node_search_spy.call_args[0][0]
            assert request.vectorset == "model-A"
            assert len(request.vector) == 768

        with (
            patch.object(
                dummy_predict,
                "query",
                side_effect=predict_query_wrapper(dummy_predict.query, 500, {"model-B": 1024}),
            ),
        ):
            resp = await nucliadb_reader.post(
                f"/kb/{kbid}/find",
                json={
                    "query": "foo",
                    "vectorset": "model-B",
                },
            )
            assert resp.status_code == 200

            node_search_spy, result, error = query
            assert result is not None
            assert error is None

            request = node_search_spy.call_args[0][0]
            assert request.vectorset == "model-B"
            assert len(request.vector) == 1024

        with (
            patch.object(
                dummy_predict,
                "query",
                side_effect=predict_query_wrapper(
                    dummy_predict.query, 500, {"model-A": 768, "model-B": 1024}
                ),
            ),
        ):
            resp = await nucliadb_reader.get(
                f"/kb/{kbid}/find",
                params={
                    "query": "foo",
                },
            )
            assert resp.status_code == 200
            node_search_spy, result, error = query
            request = node_search_spy.call_args[0][0]
            assert result is not None
            assert error is None
            # with more than one vectorset, we get the first one
            assert request.vectorset == "model-A"


@pytest.fixture(scope="function")
def dummy_predict():
    predict = DummyPredictEngine()
    set_utility(Utility.PREDICT, predict)
    yield predict
    clean_utility(Utility.PREDICT)


def create_broker_message_with_vectorsets(
    kbid: str,
    rid: str,
    field_id: str,
    vectorsets: list[tuple[str, int]],
):
    bm = writer_pb2.BrokerMessage(kbid=kbid, uuid=rid, type=writer_pb2.BrokerMessage.AUTOCOMMIT)

    body = "Lorem ipsum dolor sit amet..."
    bm.texts[field_id].body = body

    bm.extracted_text.append(make_extracted_text(field_id, body))

    for vectorset_id, vectorset_dimension in vectorsets:
        # custom vectorset
        field_vectors = resources_pb2.ExtractedVectorsWrapper()
        field_vectors.field.field = field_id
        field_vectors.field.field_type = resources_pb2.FieldType.TEXT
        field_vectors.vectorset_id = vectorset_id
        for i in range(0, 100, 10):
            field_vectors.vectors.vectors.vectors.append(
                utils_pb2.Vector(
                    start=i,
                    end=i + 10,
                    vector=[random.random()] * vectorset_dimension,
                )
            )
        bm.field_vectors.append(field_vectors)
    return bm
