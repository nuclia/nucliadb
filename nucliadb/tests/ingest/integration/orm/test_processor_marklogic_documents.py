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
import asyncio
import json
import math
import uuid

import pytest
from nidx_protos import nodereader_pb2, noderesources_pb2

from nucliadb.common.datamanagers import fields as field_datamanager
from nucliadb.common.ids import VectorId
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic import documents, vector_views
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor.processor_marklogic import ProcessorMarkLogic
from nucliadb.ingest.orm.rollover import rollover_resource
from nucliadb.search.search import backend
from nucliadb.search.search.query_parser.models import (
    Filters as RetrievalFilters,
)
from nucliadb.search.search.query_parser.models import (
    KeywordQuery,
    Query,
    ReciprocalRankFusion,
    SemanticQuery,
    UnitRetrieval,
)
from nucliadb.search.search.retrieval import text_block_search
from nucliadb_models.search import SCORE_TYPE
from nucliadb_models.security import RequestSecurity
from nucliadb_protos import knowledgebox_pb2, utils_pb2, writer_pb2
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_utils.storages.storage import Storage
from tests.utils.broker_messages import BrokerMessageBuilder

VECTOR = [0.1, 0.2, 0.3]


@pytest.mark.deploy_modes("standalone")
async def test_document_update_replaces_or_adds_top_level_properties(
    maindb_driver: MarkLogicDriver,
):
    kbid = KnowledgeBox.new_unique_kbid()
    await maindb_driver.ensure_kb_database(kbid)
    uri = f"/document-updates/{uuid.uuid4().hex}.json"
    original = {
        "slug": "before",
        "metadata": {"source": "old"},
        "facets": ["old"],
    }

    try:
        async with maindb_driver.transaction(read_only=False, kbid=kbid) as txn:
            await documents.write(txn, uri, MarkLogicCollections.RESOURCES, original)
            await documents.update_document(
                txn,
                DocumentUpdate(uri=uri, collection=MarkLogicCollections.RESOURCES).set(
                    slug="after",
                    metadata={"replacement": True},
                    facets=["new", "second"],
                    added="value",
                    added_object={"nested": {"value": 1}},
                    added_array=["first", "second"],
                ),
            )
            await txn.commit()

        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            updated = await documents.read(txn, uri)
            assert updated == {
                "slug": "after",
                "metadata": {"replacement": True},
                "facets": ["new", "second"],
                "added": "value",
                "added_object": {"nested": {"value": 1}},
                "added_array": ["first", "second"],
            }
    finally:
        await maindb_driver.delete_kb_database(kbid)


@pytest.mark.deploy_modes("standalone")
async def test_text_block_search_reads_marklogic_chunks(
    maindb_driver: MarkLogicDriver, storage: Storage, monkeypatch
):
    kbid = KnowledgeBox.new_unique_kbid()
    slug = f"marklogic-search-{uuid.uuid4().hex}"
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=slug,
        semantic_models={
            "integration-model": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=len(VECTOR),
            )
        },
    )
    resource_id = str(uuid.uuid4())
    text = "A uniquely searchable MarkLogic chunk."
    paragraph_id = f"{resource_id}/t/body/0-{len(text)}"
    vector_id = str(VectorId.from_string(f"{resource_id}/t/body/0/0-{len(text)}"))

    try:
        async with maindb_driver.transaction(read_only=False, kbid=kbid) as txn:
            await field_datamanager.create_processed_documents(
                txn,
                field=field_datamanager.FieldProcessedContent(
                    resource_id=resource_id,
                    field_id="body",
                    field_type="t",
                    extracted_text=text,
                    facets=[],
                ),
                chunks=[
                    field_datamanager.ChunkContent(
                        resource_id=resource_id,
                        field_id="body",
                        field_type="t",
                        chunk_id=paragraph_id,
                        text=text,
                        facets=[],
                        data=documents.to_json(
                            noderesources_pb2.ParagraphMetadata(
                                position=noderesources_pb2.Position(
                                    start=0,
                                    end=len(text),
                                )
                            )
                        ),
                    )
                ],
                embeddings=[
                    field_datamanager.ChunkEmbeddingContent(
                        resource_id=resource_id,
                        field_id="body",
                        field_type="t",
                        chunk_id=vector_id,
                        model_id="integration-model",
                        chunk_embedding_index=0,
                        vector=VECTOR,
                        data=documents.to_json(
                            noderesources_pb2.SentenceMetadata(
                                position=noderesources_pb2.Position(
                                    start=0,
                                    end=len(text),
                                )
                            )
                        ),
                    )
                ],
            )
            await txn.commit()

        retrieval = UnitRetrieval(
            query=Query(
                keyword=KeywordQuery(
                    query="uniquely searchable",
                    is_synonyms_query=False,
                    min_score=0,
                )
            ),
            top_k=10,
            rank_fusion=ReciprocalRankFusion(window=10),
        )
        text_blocks, pb_query, search_response, _ = await text_block_search(kbid, retrieval)

        assert pb_query.body == "uniquely searchable"
        assert search_response.paragraph.total == 1
        assert [str(block.paragraph_id) for block in text_blocks] == [paragraph_id]

        semantic_retrieval = UnitRetrieval(
            query=Query(
                semantic=SemanticQuery(
                    query=VECTOR,
                    vectorset="integration-model",
                    min_score=0.5,
                )
            ),
            top_k=10,
            rank_fusion=ReciprocalRankFusion(window=10),
        )
        semantic_blocks, _, semantic_response, _ = await text_block_search(kbid, semantic_retrieval)
        assert [str(block.paragraph_id) for block in semantic_blocks] == [paragraph_id]
        assert semantic_response.vector.documents[0].doc_id.id == vector_id

        hybrid_retrieval = UnitRetrieval(
            query=Query(
                keyword=KeywordQuery(
                    query="uniquely searchable",
                    is_synonyms_query=False,
                    min_score=0,
                ),
                semantic=SemanticQuery(
                    query=VECTOR,
                    vectorset="integration-model",
                    min_score=0.5,
                ),
            ),
            top_k=10,
            rank_fusion=ReciprocalRankFusion(window=10),
        )
        eval_queries = []
        evaluate = documents.evaluate

        async def capture_eval(txn, javascript):
            eval_queries.append(javascript)
            return await evaluate(txn, javascript)

        monkeypatch.setattr(documents, "evaluate", capture_eval)
        hybrid_blocks, _, hybrid_response, _ = await text_block_search(kbid, hybrid_retrieval)
        assert len(eval_queries) == 1
        assert MarkLogicCollections.CHUNKS in eval_queries[0]
        assert MarkLogicCollections.EMBEDDINGS in eval_queries[0]
        assert [str(block.paragraph_id) for block in hybrid_blocks] == [paragraph_id]
        assert hybrid_response.paragraph.total == 1
        assert len(hybrid_response.vector.documents) == 1
        assert hybrid_blocks[0].score_type == SCORE_TYPE.BOTH
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)


@pytest.mark.deploy_modes("standalone")
async def test_text_block_search_filters_field_and_paragraph_results(
    maindb_driver: MarkLogicDriver, storage: Storage
):
    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"marklogic-filter-search-{uuid.uuid4().hex}",
        semantic_models={
            "integration-model": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=len(VECTOR),
            )
        },
    )
    resource_a = str(uuid.uuid4())
    resource_b = str(uuid.uuid4())
    specs = [
        (resource_a, "alpha", 0, 10, ["/l/state/keep"]),
        (resource_a, "alpha", 11, 21, ["/l/state/drop"]),
        (resource_a, "beta", 0, 10, ["/l/state/keep"]),
        (resource_b, "gamma", 0, 10, ["/l/state/drop"]),
    ]

    def chunk_id(spec):
        rid, field_id, start, end, _ = spec
        return f"{rid}/t/{field_id}/{start}-{end}"

    def resource_filter(rid: str):
        expression = nodereader_pb2.FilterExpression()
        expression.resource.resource_id = rid
        return expression

    def field_filter(field_id: str):
        expression = nodereader_pb2.FilterExpression()
        expression.field.field_type = "t"
        expression.field.field_id = field_id
        return expression

    def facet_filter(facet: str):
        expression = nodereader_pb2.FilterExpression()
        expression.facet.facet = facet
        return expression

    def boolean_filter(kind: str, *operands):
        expression = nodereader_pb2.FilterExpression()
        getattr(expression, kind).operands.extend(operands)
        return expression

    async def search(filters: RetrievalFilters, *, semantic: bool = False, hybrid: bool = False):
        keyword = (
            KeywordQuery(query="needle", is_synonyms_query=False, min_score=0)
            if not semantic or hybrid
            else None
        )
        semantic_query = (
            SemanticQuery(query=VECTOR, vectorset="integration-model", min_score=0.5)
            if semantic or hybrid
            else None
        )
        retrieval = UnitRetrieval(
            query=Query(keyword=keyword, semantic=semantic_query),
            top_k=20,
            filters=filters,
            rank_fusion=ReciprocalRankFusion(window=20),
        )
        blocks, _, response, _ = await text_block_search(kbid, retrieval)
        return {str(block.paragraph_id) for block in blocks}, response

    try:
        async with maindb_driver.transaction(read_only=False, kbid=kbid) as txn:
            for rid in (resource_a, resource_b):
                await documents.write(
                    txn,
                    MarkLogicURIs.resource(rid),
                    MarkLogicCollections.RESOURCES,
                    {"facets": [f"/l/resource/{rid}"]},
                )
            for rid, field_id in ((resource_a, "alpha"), (resource_a, "beta"), (resource_b, "gamma")):
                await field_datamanager.create_processed_documents(
                    txn,
                    field=field_datamanager.FieldProcessedContent(
                        resource_id=rid,
                        field_id=field_id,
                        field_type="t",
                        extracted_text="needle text",
                        facets=[f"/l/field/{field_id}"],
                    ),
                    chunks=[],
                    embeddings=[],
                )
            for rid, field_id, start, end, facets in specs:
                cid = chunk_id((rid, field_id, start, end, facets))
                vector_id = f"{rid}/t/{field_id}/0/{start}-{end}"
                await field_datamanager.create_processed_documents(
                    txn,
                    field=field_datamanager.FieldProcessedContent(
                        resource_id=rid,
                        field_id=field_id,
                        field_type="t",
                        extracted_text="needle text",
                        facets=[f"/l/field/{field_id}"],
                    ),
                    chunks=[
                        field_datamanager.ChunkContent(
                            resource_id=rid,
                            field_id=field_id,
                            field_type="t",
                            chunk_id=cid,
                            text="needle text",
                            facets=facets,
                            data=documents.to_json(
                                noderesources_pb2.ParagraphMetadata(
                                    position=noderesources_pb2.Position(start=start, end=end)
                                )
                            ),
                        )
                    ],
                    embeddings=[
                        field_datamanager.ChunkEmbeddingContent(
                            resource_id=rid,
                            field_id=field_id,
                            field_type="t",
                            chunk_id=vector_id,
                            model_id="integration-model",
                            chunk_embedding_index=0,
                            vector=VECTOR,
                            data=documents.to_json(
                                noderesources_pb2.SentenceMetadata(
                                    position=noderesources_pb2.Position(start=start, end=end)
                                )
                            ),
                        )
                    ],
                )
            await txn.commit()

        expected_a_alpha = {chunk_id(specs[0]), chunk_id(specs[1])}
        expected_a_fields = {chunk_id(spec) for spec in specs[:3]}
        expected_across = {chunk_id(specs[0]), chunk_id(specs[1]), chunk_id(specs[3])}

        # Field expression AND and OR, first restricted to one resource.
        expression = boolean_filter("bool_and", resource_filter(resource_a), field_filter("alpha"))
        matches, response = await search(RetrievalFilters(field_expression=expression))
        assert matches == expected_a_alpha
        assert response.paragraph.total == 2

        expression = boolean_filter(
            "bool_and",
            resource_filter(resource_a),
            boolean_filter("bool_or", field_filter("alpha"), field_filter("beta")),
        )
        matches, _ = await search(RetrievalFilters(field_expression=expression))
        assert matches == expected_a_fields

        # Field OR must also select matching fields from different resources.
        expression = boolean_filter("bool_or", field_filter("alpha"), field_filter("gamma"))
        matches, _ = await search(RetrievalFilters(field_expression=expression))
        assert matches == expected_across

        # Combine field and paragraph predicates with both operators.
        keep = facet_filter("/l/state/keep")
        matches, _ = await search(
            RetrievalFilters(
                field_expression=resource_filter(resource_a),
                paragraph_expression=keep,
                filter_expression_operator=nodereader_pb2.AND,
            )
        )
        assert matches == {chunk_id(specs[0]), chunk_id(specs[2])}

        matches, _ = await search(
            RetrievalFilters(
                field_expression=resource_filter(resource_b),
                paragraph_expression=keep,
                filter_expression_operator=nodereader_pb2.OR,
            )
        )
        assert matches == {chunk_id(specs[0]), chunk_id(specs[2]), chunk_id(specs[3])}

        # Facets are hierarchical: a parent facet matches its children, a partial segment doesn't
        all_chunks = {chunk_id(spec) for spec in specs}
        matches, _ = await search(RetrievalFilters(paragraph_expression=facet_filter("/l/state")))
        assert matches == all_chunks
        matches, _ = await search(RetrievalFilters(paragraph_expression=facet_filter("/l/sta")))
        assert matches == set()
        matches, _ = await search(RetrievalFilters(field_expression=facet_filter("/l/field")))
        assert matches == all_chunks
        matches, _ = await search(RetrievalFilters(field_expression=facet_filter("/l/field/alph")))
        assert matches == set()

        # Semantic-only and hybrid retrieval apply the same combined predicates.
        combined_filters = RetrievalFilters(
            field_expression=boolean_filter(
                "bool_and", resource_filter(resource_a), field_filter("alpha")
            ),
            paragraph_expression=keep,
            filter_expression_operator=nodereader_pb2.AND,
        )
        matches, semantic_response = await search(combined_filters, semantic=True)
        assert matches == {chunk_id(specs[0])}
        assert len(semantic_response.vector.documents) == 1

        matches, hybrid_response = await search(combined_filters, hybrid=True)
        assert matches == {chunk_id(specs[0])}
        assert hybrid_response.paragraph.total == 1
        assert len(hybrid_response.vector.documents) == 1
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)


@pytest.mark.deploy_modes("standalone")
async def test_resource_relabeling_filters_existing_field_chunks(
    maindb_driver: MarkLogicDriver, storage: Storage
):
    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"relabel-{uuid.uuid4().hex}",
        semantic_models={
            "integration-model": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=len(VECTOR),
            )
        },
    )
    resource_id = str(uuid.uuid4())
    field_id = "body"
    text = "foo"
    paragraph_id = f"{resource_id}/t/{field_id}/0-{len(text)}"
    processor = ProcessorMarkLogic(maindb_driver, storage, partition="1")

    def facet_filter(facet: str):
        expression = nodereader_pb2.FilterExpression()
        expression.facet.facet = facet
        return expression

    async def search(facet: str):
        retrieval = UnitRetrieval(
            query=Query(keyword=KeywordQuery(query="foo", is_synonyms_query=False, min_score=0)),
            top_k=10,
            filters=RetrievalFilters(field_expression=facet_filter(facet)),
            rank_fusion=ReciprocalRankFusion(window=10),
        )
        blocks, _, _, _ = await text_block_search(kbid, retrieval)
        return {str(block.paragraph_id) for block in blocks}

    try:
        writer_message = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            source=writer_pb2.BrokerMessage.MessageSource.WRITER,
        )
        writer_message.bm.basic.usermetadata.classifications.add(labelset="category", label="X")
        writer_message.bm.texts[field_id].body = "Stored field value"
        await processor.process(writer_message.build(), seqid=1, transaction_check=False)

        processor_message = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR,
        )
        field_builder = processor_message.field_builder(field_id, writer_pb2.FieldType.TEXT)
        field_builder.with_extracted_labels("field-category", ["field-label"])
        field_builder.add_paragraph(text)
        await processor.process(processor_message.build(), seqid=2, transaction_check=False)

        assert await search("/l/category/X") == {paragraph_id}
        assert await search("/l/field-category/field-label") == {paragraph_id}

        relabel_message = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            source=writer_pb2.BrokerMessage.MessageSource.WRITER,
        )
        relabel_message.bm.basic.usermetadata.classifications.add(labelset="category", label="Y")
        await processor.process(relabel_message.build(), seqid=3, transaction_check=False)

        assert await search("/l/category/X") == set()
        assert await search("/l/category/Y") == {paragraph_id}
        assert await search("/l/field-category/field-label") == {paragraph_id}

        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            processed_fields = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.FIELDS_PROCESSED,
            )
            chunks = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.CHUNKS,
            )
            assert processed_fields[MarkLogicURIs.field_processed(resource_id, field_id, "t")][
                "facets"
            ] == ["/l/field-category/field-label"]
            assert processed_fields[MarkLogicURIs.field_processed(resource_id, field_id, "t")][
                "resource_facets"
            ] == ["/l/category/Y"]
            assert chunks[MarkLogicURIs.chunk(resource_id, "t", field_id, paragraph_id)]["facets"] == [
                "/k/text"
            ]
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)


@pytest.mark.deploy_modes("standalone")
async def test_rollover_restores_resource_copies_in_processed_fields(
    maindb_driver: MarkLogicDriver, storage: Storage
):
    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"rollover-{uuid.uuid4().hex}",
        semantic_models={
            "integration-model": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=len(VECTOR),
            )
        },
    )
    resource_id = str(uuid.uuid4())
    field_id = "body"
    text = "foo"
    paragraph_id = f"{resource_id}/t/{field_id}/0-{len(text)}"
    processed_uri = MarkLogicURIs.field_processed(resource_id, field_id, "t")
    processor = ProcessorMarkLogic(maindb_driver, storage, partition="1")

    async def search(filters: RetrievalFilters) -> set[str]:
        retrieval = UnitRetrieval(
            query=Query(keyword=KeywordQuery(query="foo", is_synonyms_query=False, min_score=0)),
            top_k=10,
            filters=filters,
            rank_fusion=ReciprocalRankFusion(window=10),
        )
        blocks, _, _, _ = await text_block_search(kbid, retrieval)
        return {str(block.paragraph_id) for block in blocks}

    label_filter = nodereader_pb2.FilterExpression()
    label_filter.facet.facet = "/l/category/X"
    by_label = RetrievalFilters(field_expression=label_filter)
    by_other_group = RetrievalFilters(
        field_expression=label_filter, security=RequestSecurity(groups=["other"])
    )

    try:
        writer_message = BrokerMessageBuilder(
            kbid=kbid, rid=resource_id, source=writer_pb2.BrokerMessage.MessageSource.WRITER
        )
        writer_message.bm.basic.usermetadata.classifications.add(labelset="category", label="X")
        writer_message.bm.security.access_groups.append("secret")
        writer_message.bm.texts[field_id].body = "Stored field value"
        await processor.process(writer_message.build(), seqid=1, transaction_check=False)
        processor_message = BrokerMessageBuilder(
            kbid=kbid, rid=resource_id, source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR
        )
        processor_message.field_builder(field_id, writer_pb2.FieldType.TEXT).add_paragraph(text)
        await processor.process(processor_message.build(), seqid=2, transaction_check=False)

        # Processed fields written before the resource copies existed
        async with maindb_driver.rw_transaction(kbid=kbid) as txn:
            await documents.update_document(
                txn,
                DocumentUpdate(
                    uri=processed_uri, collection=MarkLogicCollections.FIELDS_PROCESSED, upsert=False
                ).remove("resource_facets", "security"),
            )
            await txn.commit()
        assert await search(by_label) == set()

        assert await rollover_resource(maindb_driver, storage, kbid, resource_id)

        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            processed_field = await documents.read(txn, processed_uri)
        assert processed_field is not None
        assert processed_field["resource_facets"] == ["/l/category/X"]
        assert processed_field["security"] == {"access_groups": ["secret"]}
        assert await search(by_label) == {paragraph_id}
        assert await search(by_other_group) == set()
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)


async def read_resource_documents(
    txn: MarkLogicTransaction, *, resource_id: str, collection: MarkLogicCollections
) -> dict[str, dict]:
    directory = MarkLogicURIs.resource_directory(resource_id)
    query = (
        "cts.uris('', ['document', 'item-order'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(collection)}), "
        f"cts.directoryQuery({json.dumps(directory)}, 'infinity')]))"
    )
    uris = await documents.evaluate(txn, query)
    result = {}
    for uri in uris:
        content = await documents.read(txn, str(uri))
        assert content is not None
        result[str(uri)] = content
    return result


@pytest.mark.deploy_modes("standalone")
async def test_writer_persists_resource_and_fields(maindb_driver: MarkLogicDriver, storage: Storage):
    kbid = KnowledgeBox.new_unique_kbid()
    slug = f"marklogic-{uuid.uuid4().hex}"
    vectorset_id = "integration-model"
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=slug,
        semantic_models={
            vectorset_id: SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=len(VECTOR),
            )
        },
    )

    try:
        resource_id = str(uuid.uuid4())
        resource_slug = f"slug-{resource_id}"
        text = "MarkLogic stores this resource and its indexed paragraph."
        builder = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            slug=resource_slug,
            source=writer_pb2.BrokerMessage.MessageSource.WRITER,
        )
        builder.with_title("MarkLogic integration resource")
        builder.with_summary("Resource and field metadata persisted through the processor")
        builder.bm.origin.metadata["source"] = "integration-test"
        builder.bm.basic.metadata.metadata["tenant"] = "test-tenant"
        builder.bm.basic.usermetadata.classifications.add(labelset="topic", label="first")
        builder.bm.basic.usermetadata.classifications.add(
            labelset="topic", label="cancelled", cancelled_by_user=True
        )
        builder.bm.security.access_groups.append("team")
        builder.bm.extra.metadata["source"] = "writer"
        message = builder.build()
        message.texts["body"].body = text

        processor = ProcessorMarkLogic(maindb_driver, storage, partition="1")
        await processor.process(message, seqid=1)

        async with maindb_driver.transaction(read_only=True, kbid=kbid) as txn:
            resource = await documents.read(txn, MarkLogicURIs.resource(resource_id))
            assert resource is not None
            assert resource["slug"] == resource_slug
            assert resource["basic"]["title"] == "MarkLogic integration resource"
            assert resource["origin"]["metadata"]["source"] == "integration-test"
            assert resource["facets"] == ["/l/topic/first"]
            assert resource["security"]["access_groups"] == ["team"]
            assert resource["extra"]["metadata"]["source"] == "writer"

            field = await documents.read(
                txn,
                MarkLogicURIs.field(resource_id, "t", "body"),
            )
            assert field is not None
            assert field["value"]["body"] == text
            assert field["status"]

            processed_fields = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.FIELDS_PROCESSED,
            )
            assert processed_fields == {
                MarkLogicURIs.field_processed(resource_id, "summary", "a"): {
                    "resource_id": resource_id,
                    "field_id": "summary",
                    "field_type": "a",
                    "extracted_text": "Resource and field metadata persisted through the processor",
                    "facets": [],
                    "resource_facets": ["/l/topic/first"],
                    "security": {"access_groups": ["team"]},
                },
                MarkLogicURIs.field_processed(resource_id, "title", "a"): {
                    "resource_id": resource_id,
                    "field_id": "title",
                    "field_type": "a",
                    "extracted_text": "MarkLogic integration resource",
                    "facets": [],
                    "resource_facets": ["/l/topic/first"],
                    "security": {"access_groups": ["team"]},
                },
            }

            chunks = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.CHUNKS,
            )
            assert len(chunks) == 2
            assert {(chunk["field_type"], chunk["field_id"]) for chunk in chunks.values()} == {
                ("a", "summary"),
                ("a", "title"),
            }

            embeddings = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.EMBEDDINGS,
            )
            assert embeddings == {}

        builder = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            source=writer_pb2.BrokerMessage.MessageSource.WRITER,
        )
        builder.bm.basic.title = "Updated title"
        builder.bm.basic.usermetadata.classifications.add(labelset="topic", label="second")
        builder.bm.texts["body"].body = "Updated body"
        message2 = builder.bm
        await processor.process(message2, seqid=2)
        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            resource = await documents.read(txn, MarkLogicURIs.resource(resource_id))
            assert resource is not None
            assert resource["title"] == "Updated title"
            assert resource["facets"] == ["/l/topic/second"]
            assert resource["security"]["access_groups"] == ["team"]
            assert resource["extra"]["metadata"]["source"] == "writer"
            field = await documents.read(txn, MarkLogicURIs.field(resource_id, "t", "body"))
            assert field is not None
            assert field["value"]["body"] == "Updated body"
            processed_fields = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.FIELDS_PROCESSED,
            )
            assert (
                processed_fields[MarkLogicURIs.field_processed(resource_id, "title", "a")][
                    "extracted_text"
                ]
                == "MarkLogic integration resource"
            )
            assert (
                processed_fields[MarkLogicURIs.field_processed(resource_id, "summary", "a")][
                    "extracted_text"
                ]
                == "Resource and field metadata persisted through the processor"
            )
            for processed_field in processed_fields.values():
                assert processed_field["resource_facets"] == ["/l/topic/second"]
                assert processed_field["security"] == {"access_groups": ["team"]}

            chunks = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.CHUNKS,
            )
            assert len(chunks) == 2

            embeddings = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.EMBEDDINGS,
            )
            assert embeddings == {}
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)


@pytest.mark.deploy_modes("standalone")
async def test_processor_replaces_processed_field_documents(
    maindb_driver: MarkLogicDriver, storage: Storage
):
    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"marklogic-reprocess-{uuid.uuid4().hex}",
        semantic_models={
            "integration-model": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=len(VECTOR),
            )
        },
    )
    resource_id = str(uuid.uuid4())
    processor = ProcessorMarkLogic(maindb_driver, storage, partition="1")

    try:
        writer_message = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            source=writer_pb2.BrokerMessage.MessageSource.WRITER,
        )
        writer_message.bm.texts["body"].body = "Stored field value"
        await processor.process(writer_message.build(), seqid=1, transaction_check=False)

        old_text = "Previous processor output"
        old_message = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR,
        )
        old_message.field_builder("body", writer_pb2.FieldType.TEXT).add_paragraph(
            old_text, vectors={"integration-model": VECTOR}
        )
        await processor.process(old_message.build(), seqid=2, transaction_check=False)

        new_text = "Replacement processor output with a new range"
        new_vector = [0.4, 0.5, 0.6]
        new_message = BrokerMessageBuilder(
            kbid=kbid,
            rid=resource_id,
            source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR,
        )
        new_message.field_builder("body", writer_pb2.FieldType.TEXT).add_paragraph(
            new_text, vectors={"integration-model": new_vector}
        )
        await processor.process(new_message.build(), seqid=3, transaction_check=False)

        expected_chunk_uri = MarkLogicURIs.chunk(
            resource_id, "t", "body", f"{resource_id}/t/body/0-{len(new_text)}"
        )
        expected_embedding_uri = MarkLogicURIs.embedding(
            resource_id,
            "t",
            "body",
            "integration-model",
            f"{resource_id}/t/body/0/0-{len(new_text)}",
        )
        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            processed_fields = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.FIELDS_PROCESSED,
            )
            assert set(processed_fields) == {MarkLogicURIs.field_processed(resource_id, "body", "t")}
            assert (
                processed_fields[MarkLogicURIs.field_processed(resource_id, "body", "t")][
                    "extracted_text"
                ]
                == new_text
            )

            chunks = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.CHUNKS,
            )
            assert set(chunks) == {expected_chunk_uri}
            assert chunks[expected_chunk_uri]["text"] == new_text

            embeddings = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.EMBEDDINGS,
            )
            assert set(embeddings) == {expected_embedding_uri}
            assert [round(value, 6) for value in embeddings[expected_embedding_uri]["vector"]] == (
                new_vector
            )
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)


async def wait_for_vector_views(
    driver: MarkLogicDriver, kbid: str, rows: dict[tuple[str, int], int]
) -> None:
    """New view templates are unavailable while the databases are reindexed."""
    counts: dict[tuple[str, int], object] = {}
    for _ in range(120):
        for (vectorset_id, dimension), expected in rows.items():
            javascript = (
                "const op = require('/MarkLogic/optic'); "
                f"op.fromView({json.dumps(vector_views.SCHEMA_NAME)}, "
                f"{json.dumps(vector_views.view_name(vectorset_id, dimension))})"
                ".groupBy(null, op.count('n')).result().toArray()[0].n"
            )
            try:
                async with driver.ro_transaction(kbid=kbid) as txn:
                    counts[(vectorset_id, dimension)] = (await documents.evaluate(txn, javascript))[0]
            except Exception as exc:
                counts[(vectorset_id, dimension)] = exc
        if all(counts[key] == expected for key, expected in rows.items()):
            return
        await asyncio.sleep(1)
    raise TimeoutError(f"Vector views not ready: {counts}, expected {rows}")


def cosine(left: list[float], right: list[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    return dot / (math.sqrt(sum(a * a for a in left)) * math.sqrt(sum(b * b for b in right)))


@pytest.mark.deploy_modes("standalone")
async def test_ann_semantic_search_with_multiple_vectorsets(
    maindb_driver: MarkLogicDriver, storage: Storage, monkeypatch: pytest.MonkeyPatch
):
    # Each vectorset gets its own view, also when two of them share the dimension
    models = {"model-a": 3, "model-b": 3, "model-c": 4}
    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"multi-vectorset-{uuid.uuid4().hex}",
        semantic_models={
            model: SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE, vector_dimension=dimension
            )
            for model, dimension in models.items()
        },
    )
    resource_id = str(uuid.uuid4())
    n_paragraphs = 10
    vectors = {
        model: [
            [math.cos(index + offset), math.sin(index + offset), *([0.5] * (dimension - 2))]
            for index in range(n_paragraphs)
        ]
        for offset, (model, dimension) in enumerate(models.items())
    }
    # model-b vectors are the closest to model-a queries, so they must not leak into its results
    query_vectors = {
        "model-a": vectors["model-b"][0],
        "model-b": vectors["model-b"][5],
        "model-c": vectors["model-c"][3],
    }
    processor = ProcessorMarkLogic(maindb_driver, storage, partition="1")

    modes: list[str] = []
    monkeypatch.setattr(backend.semantic_search_mode, "inc", lambda labels: modes.append(labels["mode"]))

    async def search(model: str) -> dict[int, float]:
        retrieval = UnitRetrieval(
            query=Query(
                semantic=SemanticQuery(query=query_vectors[model], vectorset=model, min_score=-1)
            ),
            top_k=20,
            rank_fusion=ReciprocalRankFusion(window=20),
        )
        _, _, response, _ = await text_block_search(kbid, retrieval)
        return {
            paragraph_starts.index(document.metadata.position.start): document.score
            for document in response.vector.documents
        }

    try:
        writer_message = BrokerMessageBuilder(
            kbid=kbid, rid=resource_id, source=writer_pb2.BrokerMessage.MessageSource.WRITER
        )
        writer_message.bm.texts["body"].body = "Stored field value"
        await processor.process(writer_message.build(), seqid=1, transaction_check=False)
        processor_message = BrokerMessageBuilder(
            kbid=kbid, rid=resource_id, source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR
        )
        field_builder = processor_message.field_builder("body", writer_pb2.FieldType.TEXT)
        paragraph_starts = []
        for index in range(n_paragraphs):
            _, paragraph = field_builder.add_paragraph(
                f"paragraph {index}. ",
                vectors={model: vectors[model][index] for model in models},
            )
            paragraph_starts.append(paragraph.start)
        await processor.process(processor_message.build(), seqid=2, transaction_check=False)
        await wait_for_vector_views(
            maindb_driver,
            kbid,
            {(model, dimension): n_paragraphs for model, dimension in models.items()},
        )

        exact = {model: await search(model) for model in models}
        monkeypatch.setattr(backend, "EXACT_VECTOR_SEARCH_MAX_CANDIDATES", 0)
        for model in models:
            modes.clear()
            scores = await search(model)
            assert modes == ["ann"], model
            assert scores.keys() == set(range(n_paragraphs)), model
            for index, score in scores.items():
                expected = cosine(query_vectors[model], vectors[model][index])
                assert score == pytest.approx(expected, abs=1e-4), (model, index)
            assert scores == pytest.approx(exact[model], abs=1e-6), model
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)


@pytest.mark.deploy_modes("standalone")
async def test_vectorset_created_after_kb_ingests_and_searches(
    maindb_driver: MarkLogicDriver, storage: Storage, monkeypatch: pytest.MonkeyPatch
):
    """Existing resources keep no embeddings for a new vectorset, new data is ingested and searchable."""
    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"new-vectorset-{uuid.uuid4().hex}",
        semantic_models={
            "model-a": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE, vector_dimension=3
            )
        },
    )
    new_model, new_dimension = "model-new", 5
    processor = ProcessorMarkLogic(maindb_driver, storage, partition="1")
    query_vector = [1.0, 0.5, 0.0, 0.25, 0.1]
    seqid = 0

    modes: list[str] = []
    monkeypatch.setattr(backend.semantic_search_mode, "inc", lambda labels: modes.append(labels["mode"]))

    async def ingest(rid: str, vectors: dict[str, list[list[float]]]) -> list[str]:
        nonlocal seqid
        writer_message = BrokerMessageBuilder(
            kbid=kbid, rid=rid, source=writer_pb2.BrokerMessage.MessageSource.WRITER
        )
        writer_message.bm.texts["body"].body = "Stored field value"
        seqid += 1
        await processor.process(writer_message.build(), seqid=seqid, transaction_check=False)
        processor_message = BrokerMessageBuilder(
            kbid=kbid, rid=rid, source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR
        )
        field_builder = processor_message.field_builder("body", writer_pb2.FieldType.TEXT)
        paragraph_ids = []
        for index in range(3):
            paragraph_id, _ = field_builder.add_paragraph(
                f"paragraph {index}. ",
                vectors={model: model_vectors[index] for model, model_vectors in vectors.items()},
            )
            paragraph_ids.append(paragraph_id.full())
        seqid += 1
        await processor.process(processor_message.build(), seqid=seqid, transaction_check=False)
        return paragraph_ids

    async def search(model: str, query: list[float]) -> tuple[set[str], list[float]]:
        retrieval = UnitRetrieval(
            query=Query(semantic=SemanticQuery(query=query, vectorset=model, min_score=-1)),
            top_k=20,
            rank_fusion=ReciprocalRankFusion(window=20),
        )
        blocks, _, response, _ = await text_block_search(kbid, retrieval)
        scores = sorted((document.score for document in response.vector.documents), reverse=True)
        return {str(block.paragraph_id) for block in blocks}, scores

    try:
        old_rid, new_rid = str(uuid.uuid4()), str(uuid.uuid4())
        old_paragraphs = await ingest(old_rid, {"model-a": [[1.0, 0.0, 0.0]] * 3})

        async with maindb_driver.rw_transaction(kbid=kbid) as txn:
            await KnowledgeBox(txn, storage, kbid).create_vectorset(
                knowledgebox_pb2.VectorSetConfig(
                    vectorset_id=new_model,
                    vectorset_index_config=knowledgebox_pb2.VectorIndexConfig(
                        vector_dimension=new_dimension,
                        similarity=utils_pb2.VectorSimilarity.COSINE,
                    ),
                )
            )
            await txn.commit()

        new_vectors = [[1.0, 0.5, 0.0, 0.25, 0.1], [0.0, 1.0, 0.0, 0.0, 0.0], [0.5, 0.5, 0.5, 0.5, 0.5]]
        new_paragraphs = await ingest(
            new_rid, {"model-a": [[0.0, 1.0, 0.0]] * 3, new_model: new_vectors}
        )
        await wait_for_vector_views(
            maindb_driver, kbid, {("model-a", 3): 6, (new_model, new_dimension): 3}
        )
        expected_scores = sorted((cosine(query_vector, vector) for vector in new_vectors), reverse=True)

        # Only the data pushed after the vectorset creation has its embeddings
        paragraphs, scores = await search(new_model, query_vector)
        assert paragraphs == set(new_paragraphs)
        assert scores == pytest.approx(expected_scores, abs=1e-4)
        paragraphs, _ = await search("model-a", [1.0, 0.0, 0.0])
        assert paragraphs == {*old_paragraphs, *new_paragraphs}

        monkeypatch.setattr(backend, "EXACT_VECTOR_SEARCH_MAX_CANDIDATES", 0)
        modes.clear()
        paragraphs, scores = await search(new_model, query_vector)
        assert modes == ["ann"]
        assert paragraphs == set(new_paragraphs)
        assert scores == pytest.approx(expected_scores, abs=1e-4)
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)
