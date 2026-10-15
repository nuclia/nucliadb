import json
import uuid
from unittest.mock import AsyncMock

import pytest
from nidx_protos import nodereader_pb2, noderesources_pb2

from nucliadb.common.datamanagers import fields as field_datamanager
from nucliadb.common.ids import VectorId
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor.processor_marklogic import ProcessorMarkLogic
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
from nucliadb_protos import utils_pb2, writer_pb2
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
async def test_writer_persists_resource_and_fields_without_index_message(
    maindb_driver: MarkLogicDriver, storage: Storage, monkeypatch
):
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
        generate_index_message = AsyncMock(side_effect=AssertionError("Writer must not index"))
        monkeypatch.setattr(processor, "generate_index_message", generate_index_message)
        await processor.process(message, seqid=1)
        generate_index_message.assert_not_awaited()

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
            assert processed_fields == {}

            chunks = await read_resource_documents(
                txn,
                resource_id=resource_id,
                collection=MarkLogicCollections.CHUNKS,
            )
            assert chunks == {}

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
        generate_index_message.assert_not_awaited()
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
            for collection in (
                MarkLogicCollections.FIELDS_PROCESSED,
                MarkLogicCollections.CHUNKS,
                MarkLogicCollections.EMBEDDINGS,
            ):
                assert (
                    await read_resource_documents(txn, resource_id=resource_id, collection=collection)
                    == {}
                )
    finally:
        await KnowledgeBox.delete(maindb_driver, kbid)
