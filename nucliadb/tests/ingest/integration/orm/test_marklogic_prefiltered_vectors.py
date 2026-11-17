import asyncio
import json
import math
import uuid
from xml.etree import ElementTree

import pytest
from nidx_protos.nodereader_pb2 import FilterOperator, SearchRequest

from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.marklogic_schema import SCHEMAS_DATABASE
from nucliadb.common.marklogic import documents, vector_views
from nucliadb.common.marklogic.client import Document
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.search.search import backend
from nucliadb_protos import utils_pb2
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata


@pytest.mark.deploy_modes("standalone")
async def test_persisted_prefiltered_vector_scoring(marklogic_maindb_settings, storage, monkeypatch):
    model = f"prefilter-probe-{uuid.uuid4().hex}"
    dimension = 1536
    maindb_driver = MarkLogicDriver(
        **marklogic_maindb_settings,
        system_database=f"{model}-system",
        kb_database_prefix=f"{model}-kb-",
    )
    kbid = KnowledgeBox.new_unique_kbid()
    rid = uuid.uuid4().hex
    field = f"/resources/{rid}/fields/t/allowed/"
    denied = f"/resources/{rid}/fields/t/denied/"

    async def evaluate(javascript):
        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            return await documents.evaluate(txn, javascript)

    entries = []
    collections = backend.MarkLogicCollections
    for name in ("allowed", "denied"):
        entries.append(
            {
                "uri": f"/resources/{rid}/fields/t/{name}/processed.json",
                "root": {"resource_id": rid, "field_type": "t", "field_id": name},
                "collections": [collections.FIELDS_PROCESSED],
            }
        )
    for directory, offsets, hot in (
        (field, "split/0-10", True),
        (field, "split/10-20", False),
        (denied, "0-10", True),
    ):
        entries.append(
            {
                "uri": directory + "chunks/" + offsets + "/data.json",
                "root": {"facets": ["/hot"] if hot else []},
                "collections": [collections.CHUNKS],
            }
        )
    vectors: list[tuple[str, int, list[float], int, int]] = [
        ("denied", index, [1.0, 0.0, 0.0], 0, 10) for index in range(32)
    ]
    vectors += [("allowed", 32, [0.99, 0.1, 0], 10, 20), ("allowed", 33, [0.6, 0.8, 0], 0, 10)]
    for name, index, vector, start, end in vectors:
        split = "split/" if name == "allowed" else ""
        entries.append(
            {
                "uri": f"/resources/{rid}/fields/t/{name}/embeddings/{index}.json",
                "root": {
                    "resource_id": rid,
                    "field_type": "t",
                    "field_id": name,
                    "model_id": model,
                    "vector": vector + [0.0] * (dimension - len(vector)),
                    "chunk_id": f"{rid}/t/{name}/{split}{index}/999-1000",
                    "facets": ["/hot", "/hot/child"] if index == 33 else [],
                    "data": {
                        "position": {"start": start, "end": end},
                        "nested": [{"labels": ["a", "b"]}, None],
                        "empty": [],
                    },
                },
                "collections": [collections.EMBEDDINGS, vector_views.collection(model, dimension)],
            }
        )
    view = json.dumps(vector_views.view_name(model, dimension))
    query_vector = [1.0] + [0.0] * (dimension - 1)
    prefix = f"const op = require('/MarkLogic/optic'); op.fromView('nucliadb', {view}, '', op.fragmentIdCol('fid'))"
    await maindb_driver.initialize()
    try:
        await KnowledgeBox.create(
            maindb_driver,
            kbid=kbid,
            slug=model,
            semantic_models={
                model: SemanticModelMetadata(
                    similarity_function=utils_pb2.VectorSimilarity.COSINE, vector_dimension=dimension
                )
            },
        )
        await evaluate(
            "declareUpdate(); "
            f"for (const entry of {json.dumps(entries)}) "
            "xdmp.documentInsert(entry.uri, entry.root, {collections: entry.collections});"
        )
        for attempt in range(120):
            try:
                rows = (
                    await evaluate(prefix + ".select(['field_key', 'paragraph_key']).result().toArray()")
                )[0]
                if len(rows) == len(vectors):
                    break
            except Exception as error:
                if "SQL-TABLEREINDEXING" not in str(error):
                    raise
            await asyncio.sleep(0.5)
        else:
            pytest.fail("Persisted vector view did not become ready")
        assert any(row["paragraph_key"] == field + "split/0-10" for row in rows)
        assert any(row["paragraph_key"] == denied + "0-10" for row in rows)

        predicate = f".where(op.eq(op.col('paragraph_key'), {json.dumps(field + 'split/0-10')}))"
        scoring = (
            f".annTopK(1, op.col('vector'), vec.vector({json.dumps(query_vector)}), "
            "op.col('distance'), {noIndex:null})"
        )
        plan = await evaluate(prefix + predicate + scoring + ".explain()")
        assert "ann-top-k-index" not in json.dumps(plan).lower()
        winner = (
            await evaluate(
                prefix
                + predicate
                + scoring
                + ".select(['paragraph_key', 'distance']).result().toArray()"
            )
        )[0]
        assert winner[0]["paragraph_key"] == field + "split/0-10"
        assert 1 - winner[0]["distance"] == pytest.approx(0.6, abs=1e-6)
        print(
            f"persisted view: {len(rows)} vectors; farther eligible score={1 - winner[0]['distance']:.6f}; no ANN index"
        )

        helpers = backend._HELPERS_JS.replace(
            "cts.doc(result.uri)", "(hydrated.push(result.uri), cts.doc(result.uri))"
        )
        monkeypatch.setattr(backend, "_HELPERS_JS", helpers)
        monkeypatch.setattr(backend, "EXACT_VECTOR_SEARCH_MAX_CANDIDATES", 1000)
        query = SearchRequest(
            vector=query_vector, vectorset=model, result_per_page=1, min_score_semantic=-1
        )
        query.field_filter.field.field_type = "t"
        query.field_filter.field.field_id = "allowed"
        query.paragraph_filter.facet.facet = "/hot"

        async def search():
            parameters = backend._QueryParameters()
            source = backend._search_javascript(
                query, lexical=False, semantic=True, parameters=parameters
            )
            source = "const hydrated = []; const result = " + source + "; ({result, hydrated})"
            async with maindb_driver.ro_transaction(kbid=kbid) as txn:
                result = await documents.evaluate(
                    txn, parameters.bind(source), variables=parameters.variables
                )
            return backend._eval_object(result)

        result = await search()
        assert result["result"]["semantic_mode"] == "exact"
        assert result["result"]["embeddings"][0]["score"] == pytest.approx(0.6, abs=1e-6)
        expected = entries[-1]["root"]
        assert result["result"]["embeddings"][0]["content"] == {
            name: expected[name] for name in ("chunk_id", "facets", "data")
        }
        assert result["hydrated"] == [field + "embeddings/33.json"]
        print(
            f"AND: 34 {dimension}-dimensional vectors -> {len(result['hydrated'])} hydrated winner; "
            "nested metadata preserved, no vector in response"
        )
        query.min_score_semantic = 0.7
        result = await search()
        assert result["result"]["embeddings"] == []
        assert result["hydrated"] == []
        query.min_score_semantic = -1
        query.filter_operator = FilterOperator.OR
        result = await search()
        assert result["result"]["embeddings"][0]["score"] == pytest.approx(1)
        content = result["result"]["embeddings"][0]["content"]
        assert content["facets"] == []
        assert set(content) == {"chunk_id", "facets", "data"}
        assert len(result["hydrated"]) == 1
        print(
            "OR: hot paragraph outside allowed field wins at score=1.000000; threshold .7 excludes AND winner"
        )
        query.field_filter.field.field_id = "missing"
        query.filter_operator = FilterOperator.AND
        assert (await search())["result"]["embeddings"] == []
        query.filter_operator = FilterOperator.OR
        assert (await search())["result"]["embeddings"][0]["score"] == pytest.approx(1)
        query.result_per_page = 0
        assert (await search())["result"]["embeddings"] == []
    finally:
        try:
            try:
                await maindb_driver.delete_kb_database(kbid)
            finally:
                async with maindb_driver.admin_client() as admin:
                    await admin.delete_database(maindb_driver.system_database)
                    assert await admin.get_database(maindb_driver.kb_database(kbid)) is None
                    assert await admin.get_database(maindb_driver.system_database) is None
        finally:
            try:
                template = json.dumps(vector_views.template_uri(model, dimension))
                await maindb_driver.client.eval(
                    javascript=f"declareUpdate(); xdmp.documentDelete({template});",
                    params={"database": SCHEMAS_DATABASE},
                )
                assert await maindb_driver.client.eval(
                    javascript=f"fn.docAvailable({template})",
                    params={"database": SCHEMAS_DATABASE},
                ) == [False]
                print("Cleanup verified: disposable databases and unique vector template absent")
            finally:
                vector_views._ensured.discard((model, dimension))
                await maindb_driver.finalize()


@pytest.mark.deploy_modes("standalone")
async def test_large_prefiltered_vectors_use_ann_index(
    maindb_driver: MarkLogicDriver, storage, monkeypatch
):
    """Large AND scopes must retain indexed ANN, eligibility, and >=90% recall@20."""
    model = f"prefilter-ann-{uuid.uuid4().hex}"
    kbid = KnowledgeBox.new_unique_kbid()
    rid = uuid.uuid4().hex
    eligible_count = 1200
    denied_per_filter = 2048
    limit = 20
    total = eligible_count + 2 * denied_per_filter
    collections = backend.MarkLogicCollections
    pending = []
    eligible_ids = set()
    denied_ids = set()
    assert backend.EXACT_VECTOR_SEARCH_MAX_CANDIDATES == 1000

    async def evaluate(javascript):
        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            return await documents.evaluate(txn, javascript)

    async def flush():
        async with maindb_driver.transaction(kbid=kbid, read_only=False) as txn:
            await documents.write_many(txn, pending)
            await txn.commit()
        pending.clear()

    def add_document(uri, root, document_collections):
        pending.append(
            Document(
                uri=uri,
                content=root,
                collections=document_collections,
                content_type="application/json",
            )
        )

    original_helpers = backend._HELPERS_JS
    scoring_start = "const rows = plan.annTopK("
    scoring_end = ".select(['uri', 'distance']).result().toArray();"
    assert original_helpers.count(scoring_start) == 1
    assert original_helpers.count(scoring_end) == 1
    instrumented_helpers = original_helpers.replace(
        scoring_start, "const rows = recordScoringPlan(plan.annTopK("
    ).replace(scoring_end, ".select(['uri', 'distance']));")
    monkeypatch.setattr(backend, "_HELPERS_JS", instrumented_helpers)

    async def search(query):
        parameters = backend._QueryParameters()
        source = backend._search_javascript(query, lexical=False, semantic=True, parameters=parameters)
        source = (
            "const scoringPlans = []; "
            "function recordScoringPlan(plan) { "
            "scoringPlans.push(plan.explain()); return plan.result().toArray(); } "
            "const result = " + source + "; ({result, scoringPlans})"
        )
        async with maindb_driver.ro_transaction(kbid=kbid) as txn:
            result = await documents.evaluate(
                txn, parameters.bind(source), variables=parameters.variables
            )
        return backend._eval_object(result)

    try:
        await KnowledgeBox.create(
            maindb_driver,
            kbid=kbid,
            slug=model,
            semantic_models={
                model: SemanticModelMetadata(
                    similarity_function=utils_pb2.VectorSimilarity.COSINE, vector_dimension=3
                )
            },
        )
        version = (await evaluate("xdmp.version()"))[0]
        for name in ("allowed", "denied"):
            add_document(
                f"/resources/{rid}/fields/t/{name}/processed.json",
                {"resource_id": rid, "field_type": "t", "field_id": name},
                [collections.FIELDS_PROCESSED],
            )
        for index in range(total):
            eligible = index >= 2 * denied_per_filter
            name = "denied" if index < denied_per_filter else "allowed"
            hot = index < denied_per_filter or eligible
            start = index * 10
            end = start + 10
            chunk_id = f"{rid}/t/{name}/{start}-{end}"
            (eligible_ids if eligible else denied_ids).add(chunk_id)
            angle = 0.7 + (index - 2 * denied_per_filter) * 0.0003 if eligible else 0.01 + index * 0.0001
            directory = f"/resources/{rid}/fields/t/{name}/"
            add_document(
                f"{directory}chunks/{start}-{end}/data.json",
                {"facets": ["/hot"] if hot else []},
                [collections.CHUNKS],
            )
            add_document(
                f"{directory}embeddings/{index}.json",
                {
                    "resource_id": rid,
                    "field_type": "t",
                    "field_id": name,
                    "model_id": model,
                    "vector": [math.cos(angle), math.sin(angle), 0.0],
                    "chunk_id": chunk_id,
                    "facets": [],
                    "data": {"position": {"start": start, "end": end}},
                },
                [collections.EMBEDDINGS, vector_views.collection(model, 3)],
            )
            if len(pending) >= 256:
                await flush()
        if pending:
            await flush()

        view = json.dumps(vector_views.view_name(model, 3))
        for attempt in range(120):
            try:
                rows = (
                    await evaluate(
                        "const op = require('/MarkLogic/optic'); "
                        f"op.fromView('nucliadb', {view}).select(['field_key', 'paragraph_key'])"
                        ".result().toArray()"
                    )
                )[0]
                if len(rows) == total:
                    break
            except Exception as error:
                if "SQL-TABLEREINDEXING" not in str(error):
                    raise
            await asyncio.sleep(0.5)
        else:
            pytest.fail(f"Persisted vector view did not become ready for {total} vectors")
        print(
            f"MarkLogic {version}; vectors={len(rows)} eligible={eligible_count} "
            f"field-denied={denied_per_filter} paragraph-denied={denied_per_filter}; "
            f"default exactMax=1000; KB={kbid}; template={vector_views.template_uri(model, 3)}"
        )

        filtered = SearchRequest(
            vector=[1, 0, 0], vectorset=model, result_per_page=limit, min_score_semantic=-1
        )
        filtered.field_filter.field.field_type = "t"
        filtered.field_filter.field.field_id = "allowed"
        filtered.paragraph_filter.facet.facet = "/hot"
        filtered.filter_operator = FilterOperator.AND
        unrestricted = SearchRequest(
            vector=[1, 0, 0], vectorset=model, result_per_page=limit, min_score_semantic=-1
        )
        results = {}
        errors = {}
        for label, query, exact in (
            ("filtered-exact", filtered, True),
            ("unrestricted-exact", unrestricted, True),
            ("unrestricted-ann", unrestricted, False),
            ("filtered-ann", filtered, False),
        ):
            try:
                with monkeypatch.context() as scoped:
                    if exact:
                        scoped.setattr(backend, "EXACT_VECTOR_SEARCH_MAX_CANDIDATES", total + 1)
                    result = await search(query)
                embeddings = result["result"]["embeddings"]
                ids = {embedding["content"]["chunk_id"] for embedding in embeddings}
                operators = [
                    {
                        "operator": node.tag.rsplit("}", 1)[-1],
                        **{
                            key: value
                            for key, value in node.attrib.items()
                            if key in ("query-mode", "part-index")
                        },
                    }
                    for explanation in result["scoringPlans"]
                    for node in ElementTree.fromstring(explanation).iter()
                    if node.tag.rsplit("}", 1)[-1] in ("ann-top-k", "ann-top-k-index")
                ]
                indexed = any(node["operator"] == "ann-top-k-index" for node in operators)
                baseline = results.get(label.replace("-ann", "-exact"))
                recall = len(ids & baseline["ids"]) / limit if baseline else None
                results[label] = {
                    "ids": ids,
                    "count": len(embeddings),
                    "mode": result["result"]["semantic_mode"],
                    "indexed": indexed,
                    "recall": recall,
                }
                print(
                    f"{label}: count={len(embeddings)} mode={results[label]['mode']} "
                    f"plan-index={indexed} eligible-hits={len(ids & eligible_ids)} "
                    f"recall@{limit}={recall}; physical operators={json.dumps(operators)}"
                )
            except Exception as error:
                errors[label] = str(error)
                print(f"{label}: query failed: {error}")

        assert not errors, f"Production query execution failed: {errors}"
        expected = {
            f"{rid}/t/allowed/{index * 10}-{index * 10 + 10}"
            for index in range(2 * denied_per_filter, 2 * denied_per_filter + limit)
        }
        assert results["filtered-exact"]["ids"] == expected
        assert results["unrestricted-exact"]["ids"] == {
            f"{rid}/t/denied/{index * 10}-{index * 10 + 10}" for index in range(limit)
        }
        assert len(denied_ids) == 2 * denied_per_filter
        failures = []
        for label, result in results.items():
            exact = label.endswith("-exact")
            if result["mode"] != ("exact" if exact else "ann"):
                failures.append(f"{label}: wrong semantic mode {result['mode']}")
            if result["indexed"] != (not exact):
                failures.append(f"{label}: ann-top-k-index={result['indexed']}")
            if result["count"] != limit or len(result["ids"]) != limit:
                failures.append(f"{label}: expected {limit} distinct hits, got {result['count']}")
            if label.startswith("filtered-") and not result["ids"] <= eligible_ids:
                failures.append(f"{label}: returned ineligible embeddings")
            if not exact and result["recall"] < 0.9:
                failures.append(f"{label}: recall@{limit}={result['recall']:.3f}, expected >=0.9")
        assert not failures, "\n".join(failures)
    finally:
        try:
            try:
                await KnowledgeBox.delete(maindb_driver, kbid)
            finally:
                await maindb_driver.delete_kb_database(kbid)
        finally:
            template = json.dumps(vector_views.template_uri(model, 3))
            await maindb_driver.client.eval(
                javascript=f"declareUpdate(); xdmp.documentDelete({template});",
                params={"database": SCHEMAS_DATABASE},
            )
            vector_views._ensured.discard((model, 3))
            assert await maindb_driver.client.eval(
                javascript=f"fn.docAvailable({template})",
                params={"database": SCHEMAS_DATABASE},
            ) == [False]
        async with maindb_driver.admin_client() as admin:
            assert await admin.get_database(maindb_driver.kb_database(kbid)) is None
        print(f"Cleanup verified: KB {kbid} database and unique vector template absent")
