from uuid import uuid4

import pytest
from nidx_protos.nodereader_pb2 import GraphSearchRequest, GraphSearchResponse, SearchRequest
from nidx_protos.noderesources_pb2 import IndexRelation

from nucliadb.common.datamanagers.fields import FieldProcessedContent, create_processed_documents
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.marklogic import documents, graph
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.search.search import backend
from nucliadb.search.search.graph_backend import append_graph_results, graph_expression
from nucliadb_protos.utils_pb2 import Relation, RelationNode


@pytest.mark.deploy_modes("standalone")
async def test_graph_fixture_keyword_chunks(maindb_driver, graph_resource, standalone_knowledgebox):
    async with maindb_driver.ro_transaction(kbid=standalone_knowledgebox) as txn:
        chunks = await documents.evaluate(
            txn,
            "Array.from(cts.search(cts.andQuery([cts.collectionQuery('chunks'), "
            f"cts.directoryQuery('/resources/{graph_resource}/', 'infinity')]), ['filtered', 'score-zero']))"
            ".map(doc => doc.root.toObject())",
        )
        fields = await documents.evaluate(
            txn,
            f"Array.from(cts.search(cts.directoryQuery('/resources/{graph_resource}/', 'infinity'), ['filtered', 'score-zero']))"
            ".map(doc => ({uri: xdmp.nodeUri(doc), value: doc.root.toObject()}))",
        )
        assert {entry["field_id"] for entry in backend._flatten_eval_array(chunks)} >= {
            "title",
            "summary",
        }, fields
    result = await backend.hybrid_search(
        standalone_knowledgebox,
        SearchRequest(paragraph=True, body="Searching in my knowledge graph", result_per_page=100),
    )
    assert {entry.field for entry in result.paragraph.results} >= {"/a/title", "/a/summary"}, str(result)


async def test_embedded_graph_occurrence_queries_and_replacement(marklogic_maindb_driver):
    driver = marklogic_maindb_driver
    kbid = uuid4().hex
    rid = uuid4().hex
    await driver.ensure_kb_database(kbid)
    relations = []
    for index, (source, label, destination) in enumerate(
        [
            ("Anna", "LIVES_IN", "Paris"),
            ("Peter", "WORKS_IN", "London"),
            ("Anna", "LIVES_IN", "Paris"),
        ]
    ):
        relation = IndexRelation(
            relation=Relation(
                source=RelationNode(value=source),
                to=RelationNode(value=destination),
                relation_label=label,
            )
        )
        relation.relation.metadata.paragraph_id = f"{rid}/t/body/{index}-{index + 1}"
        relations.append(graph.relation_content(relation, f"{rid}/t/body", index))
    field = FieldProcessedContent(
        resource_id=rid,
        field_type="t",
        field_id="body",
        extracted_text="",
        facets=[],
        relations=relations,
    )

    async def search(query):
        javascript = " ".join(
            [
                "(() => {",
                backend._CONSTANTS_JS,
                backend._HELPERS_JS,
                backend._prefilter_javascript(None, None),
                f"return {graph_expression(query)};",
                "})()",
            ]
        )
        async with driver.ro_transaction(kbid=kbid) as txn:
            rows = backend._flatten_eval_array(await documents.evaluate(txn, javascript))
        response = GraphSearchResponse()
        append_graph_results(response, rows)
        return response

    try:
        async with driver.rw_transaction(kbid=kbid) as txn:
            await create_processed_documents(txn, field=field, chunks=[], embeddings=[])
            await txn.commit()
        query = GraphSearchRequest(top_k=10)
        query.query.path.path.source.value = "Anna"
        query.query.path.path.destination.value = "London"
        assert not (await search(query)).graph
        query.query.path.Clear()
        query.query.path.bool_not.path.relation.value = "WORKS_IN"
        found = await search(query)
        assert len(found.graph) == 2
        assert len({path.metadata.paragraph_id for path in found.graph}) == 2
        query.query.path.Clear()
        query.query.path.bool_not.path.source.value = "Anna"
        query.kind = GraphSearchRequest.NODES
        nodes = (await search(query)).nodes
        assert {node.value for node in nodes} == {"Peter", "Paris", "London"}
        field.relations = []
        async with driver.rw_transaction(kbid=kbid) as txn:
            await create_processed_documents(txn, field=field, chunks=[], embeddings=[])
            await txn.commit()
        query.kind = GraphSearchRequest.PATH
        query.query.path.Clear()
        query.query.path.path.SetInParent()
        assert not (await search(query)).graph
    finally:
        await driver.delete_kb_database(kbid)


async def test_resource_relations_without_processed_fields(marklogic_maindb_driver, monkeypatch):
    driver = marklogic_maindb_driver
    monkeypatch.setattr(backend, "get_driver", lambda: driver)
    kbid = uuid4().hex
    rid = uuid4().hex
    await driver.ensure_kb_database(kbid)
    relation = IndexRelation(
        relation=Relation(
            source=RelationNode(value="Anna"), to=RelationNode(value="Paris"), relation_label="LIVES_IN"
        )
    )
    content = {
        "facets": ["/l/topic/cities"],
        "security": {"access_groups": ["team"]},
        "relations": [graph.relation_content(relation, "", 0)],
    }
    try:
        async with driver.rw_transaction(kbid=kbid) as txn:
            await documents.write(
                txn, MarkLogicURIs.resource(rid), MarkLogicCollections.RESOURCES, content
            )
            await txn.commit()
        query = GraphSearchRequest(top_k=10)
        query.query.path.path.source.value = "Anna"
        query.field_filter.resource.resource_id = rid
        query.security.access_groups.append("team")
        result = await backend.graph_search(kbid, query)
        assert len(result.graph) == 1
        assert not result.graph[0].resource_field_id
        query.field_filter.resource.resource_id = uuid4().hex
        assert not (await backend.graph_search(kbid, query)).graph
        query.field_filter.Clear()
        query.field_filter.facet.facet = "/l/topic"
        assert len((await backend.graph_search(kbid, query)).graph) == 1
        query.security.access_groups[:] = ["other-team"]
        assert not (await backend.graph_search(kbid, query)).graph
        query.security.access_groups[:] = ["team"]
        query.field_filter.Clear()
        query.field_filter.field.field_type = "t"
        assert not (await backend.graph_search(kbid, query)).graph
        async with driver.ro_transaction(kbid=kbid) as txn:
            assert not await documents.read(txn, MarkLogicURIs.field_processed(rid, "metadata", "a"))
    finally:
        await driver.delete_kb_database(kbid)
