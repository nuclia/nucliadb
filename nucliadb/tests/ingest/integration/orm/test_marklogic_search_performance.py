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
"""Search performance over synthetic data ingested through the MarkLogic processor.

Sizes are configurable with MARKLOGIC_PERF_RESOURCES, MARKLOGIC_PERF_FIELDS,
MARKLOGIC_PERF_PARAGRAPHS and MARKLOGIC_PERF_REPEAT. Run with `-s` to see timings.
"""

import asyncio
import json
import os
import random
import statistics
import time
import uuid
from dataclasses import dataclass

import pytest
from nidx_protos import nodereader_pb2

from nucliadb.common.ids import ParagraphId
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.marklogic import documents, vector_views
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor.processor_marklogic import ProcessorMarkLogic
from nucliadb.search.search import backend
from nucliadb.search.search.query_parser.models import Filters as RetrievalFilters
from nucliadb.search.search.query_parser.models import (
    KeywordQuery,
    Query,
    ReciprocalRankFusion,
    SemanticQuery,
    UnitRetrieval,
)
from nucliadb.search.search.retrieval import text_block_search
from nucliadb_models.security import RequestSecurity
from nucliadb_protos import utils_pb2, writer_pb2
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_utils.storages.storage import Storage
from tests.utils.broker_messages import BrokerMessageBuilder

N_RESOURCES = int(os.environ.get("MARKLOGIC_PERF_RESOURCES", "30"))
N_FIELDS = int(os.environ.get("MARKLOGIC_PERF_FIELDS", "3"))
N_PARAGRAPHS = int(os.environ.get("MARKLOGIC_PERF_PARAGRAPHS", "4"))
REPEAT = int(os.environ.get("MARKLOGIC_PERF_REPEAT", "5"))
DIMENSION = 16
VECTORSET = "perf-model"
TOP_K = 20


@dataclass
class SyntheticField:
    rid: str
    field_id: str
    tier: str
    topic: str
    group: str | None
    has_keyword: bool
    hot_paragraphs: set[str]

    @property
    def key(self) -> tuple[str, str]:
        return self.rid, self.field_id


def facet_filter(facet: str) -> nodereader_pb2.FilterExpression:
    expression = nodereader_pb2.FilterExpression()
    expression.facet.facet = facet
    return expression


def keyword_filter(keyword: str) -> nodereader_pb2.FilterExpression:
    expression = nodereader_pb2.FilterExpression()
    expression.keyword.keyword = keyword
    return expression


def field_filter(field_id: str) -> nodereader_pb2.FilterExpression:
    expression = nodereader_pb2.FilterExpression()
    expression.field.field_type = "t"
    expression.field.field_id = field_id
    return expression


def not_filter(operand: nodereader_pb2.FilterExpression) -> nodereader_pb2.FilterExpression:
    expression = nodereader_pb2.FilterExpression()
    expression.bool_not.CopyFrom(operand)
    return expression


async def ingest(processor: ProcessorMarkLogic, kbid: str, rng: random.Random) -> list[SyntheticField]:
    fields = []
    seqid = 0
    for index in range(N_RESOURCES):
        rid = uuid.uuid4().hex
        tier = "gold" if index % 2 == 0 else "silver"
        group = "secret" if index % 4 == 0 else None
        writer = BrokerMessageBuilder(
            kbid=kbid, rid=rid, source=writer_pb2.BrokerMessage.MessageSource.WRITER
        )
        writer.with_resource_labels("tier", [tier])
        if group is not None:
            writer.bm.security.access_groups.append(group)
        for field_index in range(N_FIELDS):
            writer.bm.texts[f"field{field_index}"].body = "synthetic"
        seqid += 1
        await processor.process(writer.build(), seqid=seqid, transaction_check=False)

        processor_message = BrokerMessageBuilder(
            kbid=kbid, rid=rid, source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR
        )
        for field_index in range(N_FIELDS):
            field_id = f"field{field_index}"
            topic = "even" if (index + field_index) % 2 == 0 else "odd"
            has_keyword = index % 5 == 0 and field_index == 0
            builder = processor_message.field_builder(field_id, writer_pb2.FieldType.TEXT)
            builder.with_extracted_labels("topic", [topic])
            hot = set()
            for paragraph_index in range(N_PARAGRAPHS):
                text = f"needle paragraph {paragraph_index} of {field_id} in resource {index}. "
                if has_keyword and paragraph_index == 0:
                    text = "Friedrich Nietzsche " + text
                paragraph_id, _ = builder.add_paragraph(
                    text,
                    labels=[("heat", ["hot"])] if paragraph_index == 0 else None,
                    vectors={VECTORSET: [rng.uniform(-1, 1) for _ in range(DIMENSION)]},
                )
                if paragraph_index == 0:
                    hot.add(paragraph_id.full())
            fields.append(SyntheticField(rid, field_id, tier, topic, group, has_keyword, hot))
        seqid += 1
        await processor.process(processor_message.build(), seqid=seqid, transaction_check=False)
    return fields


async def wait_for_vector_view(driver: MarkLogicDriver, kbid: str, expected_rows: int) -> None:
    """New view templates are unavailable while the databases are reindexed."""
    javascript = (
        "const op = require('/MarkLogic/optic'); "
        f"op.fromView({json.dumps(vector_views.SCHEMA_NAME)}, {json.dumps(vector_views.view_name(VECTORSET, DIMENSION))})"
        ".groupBy(null, op.count('n')).result().toArray()[0].n"
    )
    rows = None
    for _ in range(120):
        try:
            async with driver.ro_transaction(kbid=kbid) as txn:
                rows = (await documents.evaluate(txn, javascript))[0]
            if rows == expected_rows:
                return
        except Exception:
            pass
        await asyncio.sleep(1)
    raise TimeoutError(f"Vector view has {rows} rows, expected {expected_rows}")


@pytest.mark.deploy_modes("standalone")
async def test_marklogic_search_performance(
    maindb_driver: MarkLogicDriver, storage: Storage, monkeypatch: pytest.MonkeyPatch
):
    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"marklogic-perf-{uuid.uuid4().hex}",
        semantic_models={
            VECTORSET: SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=DIMENSION,
            )
        },
    )
    rng = random.Random(42)
    query_vector = [rng.uniform(-1, 1) for _ in range(DIMENSION)]
    processor = ProcessorMarkLogic(maindb_driver, storage, partition="1")
    timings: dict[str, float] = {}

    async def search(
        name: str,
        filters: RetrievalFilters,
        *,
        keyword: bool = True,
        semantic: bool = True,
    ) -> set[str]:
        retrieval = UnitRetrieval(
            query=Query(
                keyword=KeywordQuery(query="needle", is_synonyms_query=False, min_score=0)
                if keyword
                else None,
                semantic=SemanticQuery(query=query_vector, vectorset=VECTORSET, min_score=-1)
                if semantic
                else None,
            ),
            top_k=TOP_K,
            filters=filters,
            rank_fusion=ReciprocalRankFusion(window=TOP_K),
        )
        durations = []
        for _ in range(REPEAT):
            start = time.perf_counter()
            blocks, _, _, _ = await text_block_search(kbid, retrieval)
            durations.append(time.perf_counter() - start)
        timings[name] = statistics.median(durations)
        return {str(block.paragraph_id) for block in blocks}

    def fields_of(paragraph_ids: set[str]) -> set[tuple[str, str]]:
        keys = set()
        for paragraph_id in paragraph_ids:
            parsed = ParagraphId.from_string(paragraph_id)
            keys.add((parsed.rid, parsed.field_id.key))
        return keys

    try:
        start = time.perf_counter()
        fields = await ingest(processor, kbid, rng)
        ingest_time = time.perf_counter() - start
        all_keys = {field.key for field in fields}

        for name, keyword, semantic in (
            ("keyword", True, False),
            ("semantic", False, True),
            ("hybrid", True, True),
        ):
            matches = await search(name, RetrievalFilters(), keyword=keyword, semantic=semantic)
            assert len(matches) == min(TOP_K, len(fields) * N_PARAGRAPHS)
            assert fields_of(matches) <= all_keys

        # Resource labels apply to all the resource fields
        matches = await search(
            "resource facet", RetrievalFilters(field_expression=facet_filter("/l/tier/gold"))
        )
        assert matches
        assert fields_of(matches) <= {field.key for field in fields if field.tier == "gold"}

        matches = await search(
            "field facet", RetrievalFilters(field_expression=facet_filter("/l/topic/odd"))
        )
        assert matches
        assert fields_of(matches) <= {field.key for field in fields if field.topic == "odd"}

        matches = await search("field id", RetrievalFilters(field_expression=field_filter("field1")))
        assert matches
        assert {field_id for _, field_id in fields_of(matches)} == {"field1"}

        keyword_fields = {field.key for field in fields if field.has_keyword}
        matches = await search(
            "keyword filter", RetrievalFilters(field_expression=keyword_filter("Friedrich Nietzsche"))
        )
        assert matches
        assert fields_of(matches) <= keyword_fields
        matches = await search(
            "keyword filter wrong order",
            RetrievalFilters(field_expression=keyword_filter("Nietzsche Friedrich")),
        )
        assert matches == set()

        matches = await search(
            "match nothing", RetrievalFilters(field_expression=facet_filter("/l/tier/missing"))
        )
        assert matches == set()

        matches = await search(
            "match everything",
            RetrievalFilters(field_expression=not_filter(facet_filter("/l/tier/missing"))),
        )
        assert len(matches) == min(TOP_K, len(fields) * N_PARAGRAPHS)

        public_keys = {field.key for field in fields if field.group is None}
        matches = await search(
            "security other group", RetrievalFilters(security=RequestSecurity(groups=["other"]))
        )
        assert matches
        assert fields_of(matches) <= public_keys
        matches = await search(
            "security secret group", RetrievalFilters(security=RequestSecurity(groups=["secret"]))
        )
        assert len(matches) == min(TOP_K, len(fields) * N_PARAGRAPHS)

        hot_paragraphs = set().union(*(field.hot_paragraphs for field in fields))
        matches = await search(
            "paragraph facet", RetrievalFilters(paragraph_expression=facet_filter("/l/heat/hot"))
        )
        assert matches
        assert matches <= hot_paragraphs

        gold_hot = set().union(*(field.hot_paragraphs for field in fields if field.tier == "gold"))
        matches = await search(
            "field AND paragraph",
            RetrievalFilters(
                field_expression=facet_filter("/l/tier/gold"),
                paragraph_expression=facet_filter("/l/heat/hot"),
                filter_expression_operator=nodereader_pb2.AND,
            ),
        )
        assert matches
        assert matches <= gold_hot

        gold_keys = {field.key for field in fields if field.tier == "gold"}
        matches = await search(
            "field OR paragraph",
            RetrievalFilters(
                field_expression=facet_filter("/l/tier/gold"),
                paragraph_expression=facet_filter("/l/heat/hot"),
                filter_expression_operator=nodereader_pb2.OR,
            ),
        )
        assert matches
        assert all(
            paragraph in hot_paragraphs or key in gold_keys
            for paragraph in matches
            for key in fields_of({paragraph})
        )

        # Force ANN over the vector view and compare it with the exact scan
        await wait_for_vector_view(maindb_driver, kbid, len(fields) * N_PARAGRAPHS)
        semantic_cases = {
            "semantic": RetrievalFilters(),
            "semantic field facet": RetrievalFilters(field_expression=facet_filter("/l/topic/odd")),
            "semantic security": RetrievalFilters(security=RequestSecurity(groups=["other"])),
        }
        monkeypatch.setattr(backend, "EXACT_VECTOR_SEARCH_MAX_CANDIDATES", 10**9)
        exact = {
            name: await search(f"{name} (exact)", filters, keyword=False)
            for name, filters in semantic_cases.items()
        }
        monkeypatch.setattr(backend, "EXACT_VECTOR_SEARCH_MAX_CANDIDATES", 0)
        modes: list[str] = []
        monkeypatch.setattr(
            backend.semantic_search_mode, "inc", lambda labels: modes.append(labels["mode"])
        )
        for name, filters in semantic_cases.items():
            modes.clear()
            matches = await search(f"{name} (ANN)", filters, keyword=False)
            assert set(modes) == {"ann"}, f"{name}: semantic modes {modes}"
            assert len(matches) == len(exact[name])
            recall = len(matches & exact[name]) / len(exact[name])
            assert recall >= 0.9, f"{name}: ANN recall {recall:.2f}"

        print(
            f"\nMarkLogic search performance: {N_RESOURCES} resources, {len(fields)} fields, "
            f"{len(fields) * N_PARAGRAPHS} chunks/embeddings (ingested in {ingest_time:.1f}s)"
        )
        for name, seconds in timings.items():
            print(f"  {name:<28} {seconds * 1000:8.1f} ms (median of {REPEAT})")
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)
