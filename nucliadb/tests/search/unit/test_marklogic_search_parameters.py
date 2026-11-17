import json
import shutil
import subprocess
from unittest.mock import AsyncMock, Mock

import pytest
from nidx_protos.nodereader_pb2 import FilterExpression, SearchRequest

from nucliadb.common.marklogic import documents
from nucliadb.search.search import backend


async def test_evaluate_forwards_external_variables(monkeypatch):
    driver = Mock()
    driver.client.eval = AsyncMock(return_value=[{"ok": True}])
    transaction = Mock()
    transaction.sdk_transaction = AsyncMock(return_value=None)
    transaction.params = AsyncMock(return_value={"database": "kb-test"})
    monkeypatch.setattr(documents, "driver_txn", lambda txn: (driver, transaction))
    variables = {"searchParameters": json.dumps([[1.0, 0.0], "model"])}

    result = await documents.evaluate(transaction, "var searchParameters;", variables=variables)

    assert result == [{"ok": True}]
    driver.client.eval.assert_awaited_once_with(
        javascript="var searchParameters;", vars=variables, tx=None, params={"database": "kb-test"}
    )


def _bound_search(query):
    parameters = backend._QueryParameters()
    javascript = parameters.bind(
        backend._search_javascript(query, lexical=True, semantic=True, parameters=parameters)
    )
    return javascript, json.loads(parameters.variables["searchParameters"])


def test_vector_strategy_threshold_is_bound(monkeypatch):
    assert backend.EXACT_VECTOR_SEARCH_MAX_CANDIDATES == 1000
    query = SearchRequest(vector=[1.0], vectorset="model", result_per_page=7)
    sources = []
    for threshold in (1000, 0):
        monkeypatch.setattr(backend, "EXACT_VECTOR_SEARCH_MAX_CANDIDATES", threshold)
        parameters = backend._QueryParameters()
        source = backend._vector_results_expression(query, False, literal=parameters.literal)
        sources.append(source)
        assert "exactMax: searchParams[" in source
        assert parameters.values[-1] == threshold
        assert f"exactMax: {threshold}" in backend._vector_results_expression(query, False)
    assert sources[0] == sources[1]


def test_search_source_is_stable_when_request_values_change():
    sources = []
    bindings = []
    for index in (1, 2):
        query = SearchRequest(
            body=f"term{index}",
            paragraph=True,
            result_per_page=index,
            min_score_bm25=index,
            vector=[float(index), 0.0],
            vectorset=f"model{index}",
            min_score_semantic=index / 10,
        )
        query.field_filter.resource.resource_id = f"resource{index}"
        query.paragraph_filter.facet.facet = f"/label{index}"
        query.graph_search.query.path.path.source.value = f"node{index}"
        query.graph_node_vectorset = f"node-model{index}"
        query.graph_edge_vectorset = f"edge-model{index}"
        javascript, values = _bound_search(query)
        sources.append(javascript)
        bindings.append(values)
        assert f"term{index}" not in javascript
        assert f"model{index}" not in javascript
        assert f"resource{index}" not in javascript
        assert f"/label{index}" not in javascript
        assert [float(index), 0.0] in values
        assert f"model{index}" in values
        assert [f"term{index}"] in values
    assert sources[0] == sources[1]
    assert bindings[0] != bindings[1]


@pytest.mark.parametrize("kind", ["resource", "field", "resource_field_prefix", "facet", "keyword"])
def test_nested_filter_values_are_bound(kind):
    expression = FilterExpression()
    operand = expression.bool_and.operands.add().bool_not
    if kind == "resource":
        operand.resource.resource_id = "private-value"
    elif kind == "field":
        operand.field.field_type = "private-value"
        operand.field.field_id = "private-value"
    elif kind == "resource_field_prefix":
        operand.resource_field_prefix.resource_id = "private-value"
        operand.resource_field_prefix.field_type = "t"
        operand.resource_field_prefix.field_id_prefix = "private-value"
    elif kind == "facet":
        operand.facet.facet = "private-value"
    else:
        operand.keyword.keyword = "private-value"
    parameters = backend._QueryParameters()

    result = backend._expression_query(expression, field_level=True, literal=parameters.literal)

    assert "private-value" not in result.js
    assert "private-value" in parameters.variables["searchParameters"]
    assert result.filtered == (kind == "keyword")


def test_disabled_hybrid_security_is_preserved():
    query = SearchRequest(body="term", paragraph=True, result_per_page=1, vector=[1.0])
    query.security.access_groups.append("restricted-group")
    javascript, values = _bound_search(query)
    assert "restricted-group" not in javascript
    assert "restricted-group" not in json.dumps(values)


def test_vector_only_templates_do_not_enumerate_prefilter_keys():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for server-side JavaScript API mocks")
    query = SearchRequest(vector=[1.0], vectorset="model", result_per_page=1)
    query.field_filter.resource.resource_id = "resource"
    query.paragraph_filter.facet.facet = "/label"
    source = backend._search_javascript(query, lexical=False, semantic=True)
    assert "const materializeFieldDirs = false;" in source
    query.graph_search.query.path.path.source.value = "node"
    assert "const materializeFieldDirs = true;" in backend._search_javascript(
        query, lexical=False, semantic=True
    )
    assert "const materializeFieldDirs = true;" in backend._search_javascript(
        query, lexical=True, semantic=True
    )
    prefilter = backend._prefilter_javascript(query.field_filter, None, materialize_fields=False)
    vector = backend._vector_results_expression(query, False)
    harness = r"""
const assert = require('node:assert/strict');
const [helpers, prefilter, vector] = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const sources = [];
const cts = new Proxy({}, {get(target, name) {
    if (['uris', 'search', 'estimate'].includes(name)) {
        return () => { throw new Error('vector-only index filters must not enumerate keys'); };
    }
    return (...args) => ({name, args});
}});
const op = {
    col: name => name, viewCol: (qualifier, name) => {assert.ok(qualifier); return name;},
    as: (name, value) => ({name, value}),
    fn: {replace: (...args) => args, matches: (...args) => args},
    fromDocUris(query) {
        assert.ok(!Array.isArray(query));
        sources.push(query);
        return {where() {return this;}, select() {return this;}, whereDistinct() {return this;}};
    }
};
const run = new Function('cts', 'require', 'COLLECTIONS', 'vec', helpers + prefilter + `
    const paragraphOr = false;
    const paragraphQuery = cts.trueQuery();
    const embeddingScope = cts.trueQuery();
    semanticSearch = search => search;
    return (${vector});`);
const search = run(cts, () => op, {fieldsProcessed: 'fields', chunks: 'chunks', embeddings: 'embeddings'},
    {vector: value => value});
assert.equal(sources.length, 2);
assert.ok(search.fieldPlan);
assert.ok(search.paragraphPlan);
"""
    subprocess.run(
        [node, "-e", harness],
        input=json.dumps([backend._HELPERS_JS, prefilter, vector]),
        text=True,
        check=True,
    )


async def test_hybrid_search_does_not_reconcile_views(monkeypatch):
    driver = Mock(spec=backend.MarkLogicDriver)
    transaction = Mock()
    transaction.database = "existing-kb"
    driver.ro_transaction.return_value.__aenter__ = AsyncMock(return_value=transaction)
    driver.ro_transaction.return_value.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(backend, "get_driver", lambda: driver)
    monkeypatch.setattr(documents, "driver_txn", lambda txn: (driver, transaction))
    ensure = AsyncMock()
    monkeypatch.setattr(backend.vector_views, "ensure_view", ensure)

    async def evaluate(*args, **kwargs):
        ensure.assert_not_called()
        return [{"embeddings": []}]

    monkeypatch.setattr(documents, "evaluate", evaluate)
    await backend.hybrid_search("kb", SearchRequest(vector=[1, 0], vectorset="model", result_per_page=1))
    ensure.assert_not_called()


def test_semantic_helper_native_scoring_and_view_errors():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to exercise the server-side JavaScript with API mocks")
    harness = r"""
const assert = require('node:assert/strict');
const helpers = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
let attempts = [];
let candidateRows = [];
let eligibleSize = 1001;
let probeError = null;
let annError = null;
let probes = [];
let hydrated = 0;
let metadata = {chunk_id: 'chunk', facets: [], data: {position: {start: 0, end: 10},
    nested: [{labels: ['a', 'b']}, null], empty: []}};
const root = {
    get chunk_id() { return metadata.chunk_id; },
    get facets() { return metadata.facets; },
    get data() { return metadata.data; },
    get vector() { throw new Error('vector must not be materialized'); },
    toObject() { throw new Error('whole root must not be materialized'); }
};
const plan = {
    annTopK(count, column, vector, distance, options) {
        assert.equal(Object.hasOwn(options, 'onlyIndex'), false);
        if (Object.hasOwn(options, 'noIndex')) assert.equal(options.noIndex, null);
        attempts.push([count, Object.hasOwn(options, 'noIndex')]);
    if (annError) throw annError;
    return this;
  },
  where(scope) { assert.equal(scope, 'indexed-scope'); return this; },
  joinDocUri() { return this; },
    select(columns) {
        if (columns.length === 1) {
                assert.deepEqual(columns, ['fid']);
                return {limit(count) {
                        probes.push(count);
                        return {result() {
                                if (probeError) throw probeError;
                                return {toArray: () => Array.from({length: Math.min(count, eligibleSize)},
                                        (_, fid) => ({fid}))};
                        }};
                }};
        }
        return this;
    },
  result() { return {toArray: () => candidateRows}; }
};
const op = {fromView: () => plan, col: name => name, fragmentIdCol: name => name,
    viewCol: (qualifier, name) => {assert.equal(qualifier, 'vectors'); return name;}};
const cts = {
        estimate() { throw new Error('broad scope estimates must not choose scoring strategy'); },
    doc: () => {
        hydrated++;
        return {root, toObject() { throw new Error('whole document must not be materialized'); }};
    },
  search(scope) {
        throw new Error('document scanning must not be used');
  }
};
const vec = {vector: value => value, cosineDistance: () => 0};
const execute = new Function('cts', 'vec', 'require', 'VECTOR_SCHEMA', helpers +
  '; return search => ({results: semanticSearch(search), mode: semanticMode});')(
    cts, vec, () => op, 'schema');
const search = {limit: 1, scope: 'indexed-scope', queryVector: [1],
    minScore: 0, view: 'view', exactMax: 1000,
    fieldPlan: null, paragraphPlan: null, paragraphOr: false};
candidateRows = [{uri: 'uri', distance: 0}];
const initial = execute(search);
assert.equal(initial.mode, 'ann');
assert.deepEqual(initial.results, [{score: 1, content: metadata}]);
assert.deepEqual(attempts, [[1, false]]);
assert.deepEqual(probes, [1001]);
assert.equal(hydrated, 1);
for (const content of [
    {...metadata, facets: ['/hot', '/hot/child']},
    {chunk_id: 'chunk'},
    {},
    {chunk_id: null, facets: null, data: null}
]) {
    metadata = content;
    assert.deepEqual(JSON.parse(JSON.stringify(execute(search).results)), [{score: 1, content}]);
}
attempts = [];
candidateRows = [];
const shortPage = execute(search);
assert.equal(shortPage.mode, 'ann');
assert.equal(shortPage.results.length, 0);
assert.deepEqual(attempts, [[1, false]]);
attempts = [];
candidateRows = [{uri: 'uri', distance: 0}];
for (const size of [1, 999, 1000, 1001, 1000000]) {
    eligibleSize = size;
    attempts = [];
    const result = execute({...search, limit: 7});
    assert.equal(result.mode, size <= 1000 ? 'exact' : 'ann');
    assert.equal(result.results.length, 1);
    assert.deepEqual(attempts, [[7, size <= 1000]]);
}
eligibleSize = 0;
attempts = [];
hydrated = 0;
assert.deepEqual(execute(search), {results: [], mode: 'exact'});
assert.deepEqual(attempts, []);
assert.equal(hydrated, 0);
probes = [];
assert.equal(execute({...search, limit: 0}).results.length, 0);
assert.deepEqual(probes, []);
eligibleSize = 1001;
annError = {name: 'SQL-TABLENOTFOUND'};
assert.throws(() => execute(search), error => error === annError);
annError = {name: 'SQL-TABLEREINDEXING'};
assert.throws(() => execute(search), error => error === annError);
eligibleSize = 1;
assert.throws(() => execute(search), error => error === annError);
annError = new Error('unexpected');
assert.throws(() => execute(search), /unexpected/);
probeError = new Error('eligibility probe failed');
assert.throws(() => execute(search), error => error === probeError);
"""
    subprocess.run([node, "-e", harness], input=json.dumps(backend._HELPERS_JS), text=True, check=True)


def test_prefiltered_scoring_predicates_threshold_limit_and_winner_hydration():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for server-side JavaScript API mocks")
    harness = r"""
const assert = require('node:assert/strict');
const helpers = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const field = '/resources/r/fields/t/a/';
const other = '/resources/r/fields/t/b/';
const rows = [
    {uri: 'denied', field_key: other, paragraph_key: other + '0-10', distance: 0},
    {uri: 'wrong-paragraph', field_key: field, paragraph_key: field + 'split/10-20', distance: .1},
    {uri: 'eligible', field_key: field, paragraph_key: field + 'split/0-10', distance: .4},
    {uri: 'farther', field_key: field, paragraph_key: field + 'split/0-10', distance: .6}
];
rows.forEach((row, index) => { row.fid = index; });
let hydrated = [];
let calls = [];
let probes = [];
let lastSource = null;
let viewAliases = [];
class Plan {
    constructor(selected) { this.rows = selected; }
    where(predicate) { return new Plan(this.rows.filter(predicate)); }
    select(columns) {
        return new Plan(this.rows.map(row => Object.fromEntries(columns.map(column =>
            typeof column === 'string' ? [column, row[column]] : [column.name, column.value(row)]))));
    }
    whereDistinct() {
        return new Plan([...new Map(this.rows.map(row => [JSON.stringify(row), row])).values()]);
    }
    union(other) { return new Plan([...this.rows, ...other.rows]); }
    limit(count) {
        assert.ok(this.rows.every(row => Object.keys(row).length === 1 && 'fid' in row));
        probes.push([count, this.rows.length]);
        return new Plan(this.rows.slice(0, count));
    }
    joinInner(other, keys) {
        return new Plan(this.rows.flatMap(left => other.rows.filter(right =>
            keys.left(left) === keys.right(right)).map(right => ({...left, ...right}))));
    }
    annTopK(count, column, vector, distance, options) {
        const exact = this.rows.length <= search.exactMax;
        assert.deepEqual(options, exact ? {maxDistance: 1 - search.minScore, noIndex: null} :
            {maxDistance: 1 - search.minScore});
        assert.equal(new Set(this.rows.map(row => row.fid)).size, this.rows.length);
        calls.push(count);
        return new Plan(this.rows.filter(row => row.distance <= options.maxDistance)
            .sort((left, right) => left.distance - right.distance).slice(0, count));
    }
    joinDocUri() { return this; }
    result() { return {toArray: () => this.rows}; }
}
const op = {
    col: name => row => row[name], fragmentIdCol: name => name,
    viewCol: (qualifier, name) => {
        assert.ok(['vectors', 'field_vectors', 'paragraph_vectors', 'fields', 'paragraphs'].includes(qualifier));
        return row => row[name];
    },
    on: (left, right) => ({left, right}),
    as: (name, value) => ({name, value}),
    fn: {
        replace: (value, pattern, replacement) => row => value(row).replace(new RegExp(pattern), replacement),
        matches: (value, pattern) => row => new RegExp(pattern).test(value(row))
    },
    fromDocUris: (query, qualifier) => {
        assert.ok(['fields', 'paragraphs'].includes(qualifier));
        lastSource = query;
        return new Plan((Array.isArray(query) ? query : query.uris).map(uri => ({uri})));
    },
    fromView: (schema, view, qualifier, fragmentId) => {
        assert.ok(['vectors', 'field_vectors', 'paragraph_vectors'].includes(qualifier));
        viewAliases.push(qualifier);
        return new Plan(rows.map(row => ({...row, [fragmentId]: row.fid})));
    }
};
const cts = {
    estimate() { throw new Error('filtered scopes must not choose ANN by estimate'); },
    uris() { throw new Error('index filters must remain query-backed'); },
    search(query, options) {
        assert.deepEqual(options, ['filtered', 'score-zero']);
        return query.verified.map(uri => ({uri}));
    },
    doc(uri) {
        hydrated.push(uri);
        return {root: {chunk_id: uri, facets: [], data: {}},
            toObject() { throw new Error('whole document must not be materialized'); }};
    }
};
const execute = new Function('cts', 'vec', 'require', 'VECTOR_SCHEMA', 'xdmp', helpers +
    '; return {run: search => ({results: semanticSearch(search), mode: semanticMode}), matchingKeyPlan};')(
        cts, {}, () => op, 'schema', {nodeUri: doc => doc.uri});
const fieldPlan = keys => execute.matchingKeyPlan({uris: keys.map(key => key + 'processed.json')}, false, 'field_key');
const paragraphPlan = keys => execute.matchingKeyPlan({uris: keys.map(key =>
    key.replace(/^(\/resources\/[^/]+\/fields\/[^/]+\/[^/]+\/)/, '$1chunks/') + '/data.json')}, false, 'paragraph_key');
const largeQuery = {uris: Array.from({length: 10000}, (_, index) => field + index + '/processed.json')};
assert.equal(execute.matchingKeyPlan(largeQuery, false, 'field_key').rows.length, 10000);
assert.equal(lastSource, largeQuery);
const verified = [field + 'chunks/split/0-10/data.json', 'invalid-uri'];
assert.deepEqual(execute.matchingKeyPlan({verified}, true, 'paragraph_key').rows,
    [{paragraph_key: field + 'split/0-10'}]);
assert.deepEqual(lastSource, verified);
assert.deepEqual(execute.matchingKeyPlan({verified: []}, true, 'field_key').rows, []);
let search = {limit: 1, scope: () => true, queryVector: [1], minScore: .5,
    view: 'view', exactMax: 2,
    fieldPlan: fieldPlan([field, field]), paragraphPlan: paragraphPlan([field + 'split/0-10']), paragraphOr: false};
let result = execute.run(search);
assert.equal(result.mode, 'exact');
assert.deepEqual(probes, [[3, 2]]);
assert.equal(result.results[0].content.chunk_id, 'eligible');
assert.equal(result.results[0].score, .6);
assert.deepEqual(hydrated, ['eligible']);
assert.deepEqual(viewAliases, ['vectors']);
viewAliases = [];
search = {...search, paragraphOr: true};
result = execute.run(search);
assert.deepEqual(viewAliases, ['vectors', 'field_vectors', 'paragraph_vectors']);
assert.equal(result.mode, 'ann');
assert.equal(result.results[0].content.chunk_id, 'wrong-paragraph');
assert.deepEqual(probes.at(-1), [3, 3]);
search = {...search, fieldPlan: fieldPlan([]), paragraphPlan: paragraphPlan([other + '0-10'])};
assert.equal(execute.run(search).results[0].content.chunk_id, 'denied');
search = {...search, fieldPlan: null, paragraphPlan: paragraphPlan([])};
assert.equal(execute.run(search).results[0].content.chunk_id, 'denied');
search = {...search, paragraphOr: false};
calls = [];
assert.deepEqual(execute.run(search).results, []);
assert.deepEqual(calls, []);
search = {...search, fieldPlan: fieldPlan([field]), paragraphPlan: paragraphPlan([field + 'split/0-10']), minScore: .7};
assert.deepEqual(execute.run(search).results, []);
search = {...search, minScore: 0, limit: 2};
assert.equal(execute.run(search).results.length, 2);
search = {...search, paragraphOr: true, limit: 10};
assert.equal(execute.run(search).results.length, 3);
search = {...search, fieldPlan: fieldPlan([]), paragraphPlan: null};
assert.deepEqual(execute.run(search).results, []);
assert.deepEqual(execute.run({...search, limit: 0}).results, []);
"""
    subprocess.run([node, "-e", harness], input=json.dumps(backend._HELPERS_JS), text=True, check=True)
