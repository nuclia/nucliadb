import json

from nidx_protos.noderesources_pb2 import IndexRelation

from nucliadb.common.marklogic import graph
from nucliadb_protos.utils_pb2 import Relation, RelationNode


def test_relation_projection_preserves_occurrence_and_metadata():
    indexed = IndexRelation(
        relation=Relation(
            source=RelationNode(value="Rosá", subtype="PERSON"),
            to=RelationNode(value="Paris", subtype="PLACE"),
            relation_label="LIVES_IN",
        ),
        facets=["/g/u"],
    )
    indexed.relation.metadata.paragraph_id = "rid/t/body/0-10"
    content = graph.relation_content(indexed, "rid/t/body", 3)
    assert content["source_normalized"] == "rosa"
    assert content["source_value"] == "Rosá"
    assert content["occurrence"] == 3
    assert json.loads(content["facets_json"]) == ["/g/u"]
    assert json.loads(content["metadata_json"])["paragraph_id"] == "rid/t/body/0-10"
    columns = graph.template()["template"]["rows"][0]["columns"]
    assert {column["name"] for column in columns} == set(content)


def test_fuzzy_patterns_include_prefix_and_all_edit_operations():
    from nucliadb.search.search.graph_backend import fuzzy_patterns

    patterns = fuzzy_patterns("anna", prefix=True)
    assert {"anna*", "?anna*", "a?na*", "ana*", "anan*", "anna?*"} <= set(patterns)
    assert r"a\*b*" in fuzzy_patterns("a*b", prefix=True)
