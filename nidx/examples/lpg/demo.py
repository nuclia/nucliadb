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
import argparse
import json
from uuid import uuid4
import time
from nidx_protos import nidx_pb2

from .cypher import Cypher
from .cypher import Path as CypherPath
from .storage import NidxClient, PropertyFilter, SearchHit
from .storage import Graph as LPGraph



def cypher_value(value: object) -> object:
    if isinstance(value, nidx_pb2.LpgRecord):
        if value.key.kind == nidx_pb2.LPG_NODE:
            return {"id": value.key.id, "name": value.properties["name"].text}
        return {
            "id": value.key.id,
            "type": value.edge_type,
            "source": value.source,
            "target": value.target,
        }
    if isinstance(value, CypherPath):
        return {
            "nodes": [cypher_value(node) for node in value.nodes],
            "relationships": [cypher_value(edge) for edge in value.relationships],
        }
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Demonstrate the LPG graph APIs with illustrative data")
    parser.add_argument("--address", required=True, help="Trusted API gRPC address (host:port)")
    args = parser.parse_args()
    nidx = NidxClient(args.address)
    try:
        graph = LPGraph.create(nidx, str(uuid4()))
        time.sleep(5)
        graph.upsert_node("progress", ["Company"], {"name": "Progress"})
        people = [
            ("matthieu", "Matthieu Jonglez", "leader", "leadership"),
            ("ramon", "Ramon", "manager", "leadership"),
            ("ferran", "Ferran", "software engineer", "nucliadb"),
            ("joan", "Joan Antoni", "software engineer", "nucliadb"),
            ("carmen", "Carmen", "AI engineer", "AI"),
            ("oni", "Oni", "AI engineer", "AI"),
            ("kento", "Kento", "AI engineer", "AI"),
            ("arnau", "Arnau", "AI engineer", "AI"),
        ]
        for id, name, role, team in people:
            if team == "nucliadb":
                focus, vector = "nucliadb graph search and indexing", [1.0, 0.0, 0.0]
            elif team == "AI":
                focus, vector = "AI engineering and language models", [0.0, 1.0, 0.0]
            else:
                focus, vector = "engineering leadership and management", [0.0, 0.0, 1.0]
            graph.upsert_node(
                id,
                ["Person"],
                {"name": name, "role": role, "team": team, "active": True},
                text_chunks={"focus": [focus]},
                embeddings={"focus": [vector]},
            )
            graph.upsert_edge(f"{id}-works-for", id, "WORKS_FOR", "progress", {})

        graph.upsert_edge("ramon-reports-to", "ramon", "REPORTS_TO", "matthieu", {"since": 2024})
        for id in ("ferran", "joan", "carmen", "oni", "kento", "arnau"):
            graph.upsert_edge(f"{id}-reports-to", id, "REPORTS_TO", "ramon", {"since": 2024})

        updated = graph.update_text_properties(
            "node",
            "ferran",
            text_chunks={"focus": ["nucliadb graph search, indexing, and retrieval"]},
            embeddings={"focus": [[1.0, 0.0, 0.0]]},
        )
        engineers = graph.find("node", PropertyFilter("role", "eq", value="AI engineer"))
        team_filter = [
            PropertyFilter("team", "eq", value="nucliadb"),
            PropertyFilter("active", "eq", value=True),
        ]
        text_hits = graph.search(text="graph search", text_property="focus", filters=team_filter)
        vector_hits = graph.search(vector=[1.0, 0.0, 0.0], text_property="focus", filters=team_filter)
        hybrid_hits = graph.search(
            text="graph search",
            vector=[1.0, 0.0, 0.0],
            text_property="focus",
            filters=team_filter,
        )
        expansion = graph.expand(["ramon"], direction="incoming", edge_types=["REPORTS_TO"])
        path = graph.shortest_path("ferran", "matthieu", direction="outgoing", edge_types=["REPORTS_TO"])
        if path is None:
            raise RuntimeError("Expected a reporting path from Ferran to Matthieu Jonglez")

        cypher = Cypher(nidx.lpg_client, graph.shard_id, timeout=30)
        queries: dict[str, tuple[str, dict[str, object]]] = {
            "lookup": (
                "MATCH (p:Person {name: $name}) RETURN p",
                {"name": "Ferran"},
            ),
            "neighborhood": (
                "MATCH (p:Person {name: $name})-[r]-(neighbor) RETURN p, r, neighbor LIMIT 20",
                {"name": "Ramon"},
            ),
            "bounded_traversal": (
                "MATCH path = (p:Person {name: $name})-[:REPORTS_TO*1..3]->(other) RETURN path LIMIT 20",
                {"name": "Ferran"},
            ),
            "coworkers": (
                "MATCH (p:Person)-[:WORKS_FOR]->(c:Company)<-[:WORKS_FOR]-(other:Person) "
                "WHERE p.name = $name RETURN p, c, other",
                {"name": "Ferran"},
            ),
            "relationship_filter": (
                "MATCH (a:Person)-[r:REPORTS_TO]->(b:Person) WHERE r.since >= $since RETURN a, r, b",
                {"since": 2024},
            ),
            "employee_count": (
                "MATCH (c:Company)<-[:WORKS_FOR]-(p:Person) "
                "RETURN c.name, count(p) AS employees ORDER BY employees DESC LIMIT 10",
                {},
            ),
            "shortest_path": (
                "MATCH path = shortestPath((a:Person {name: $start})-[:REPORTS_TO*..6]-"
                "(b:Person {name: $end})) RETURN path",
                {"start": "Ferran", "end": "Matthieu Jonglez"},
            ),
        }
        cypher_results = {}
        for name, (query, parameters) in queries.items():
            rows = cypher.execute(query, parameters)
            cypher_results[name] = {
                "query": query,
                "parameters": parameters,
                "rows": [{key: cypher_value(value) for key, value in row.items()} for row in rows],
            }

        try:
            graph.delete_node("arnau")
        except ValueError as error:
            if "incident edges" not in str(error):
                raise
            deletion_error = str(error)
        else:
            raise RuntimeError("Node deletion should fail while incident edges remain")
        incident = graph.expand(["arnau"], direction="both")
        removed_edges = sorted({match.edge.id for match in incident.matches})
        for edge_id in removed_edges:
            graph.delete_edge(edge_id)
        graph.delete_node("arnau")

        def hits(results: list[SearchHit]) -> list[dict[str, object]]:
            return [
                {
                    "name": hit.record.properties["name"],
                    "property": hit.property,
                    "text": hit.text,
                    "score": hit.score,
                    "text_rank": hit.text_rank,
                    "vector_rank": hit.vector_rank,
                }
                for hit in results
            ]

        print(
            json.dumps(
                {
                    "note": (
                        "Illustrative organization and reporting relationships; toy vectors "
                        "demonstrate search mechanics, not semantic relevance. "
                        "People and query results describe the graph before deletion."
                    ),
                    "graph": {"tenant": graph.tenant, "shard_id": graph.shard_id},
                    "people": [
                        dict(id=id, name=name, role=role, team=team) for id, name, role, team in people
                    ],
                    "updated_text_properties": {
                        "person": updated.id,
                        "text_chunks": updated.text_chunks,
                    },
                    "discovery": {
                        "filter": "role = AI engineer",
                        "people": [r.properties for r in engineers],
                    },
                    "search": {
                        "filter": "team = nucliadb AND active = true",
                        "text_query": "graph search",
                        "vector_query": [1.0, 0.0, 0.0],
                        "text": hits(text_hits),
                        "vector": hits(vector_hits),
                        "hybrid": hits(hybrid_hits),
                    },
                    "expand": {
                        "start": "Ramon",
                        "direction": "incoming",
                        "edge_type": "REPORTS_TO",
                        "matches": [
                            {
                                "person": match.neighbor.properties["name"],
                                "relationship": match.edge.edge_type,
                            }
                            for match in expansion.matches
                        ],
                    },
                    "shortest_path": {
                        "people": [node.properties["name"] for node in path.nodes],
                        "relationships": [edge.edge_type for edge in path.edges],
                    },
                    "cypher": cypher_results,
                    "deletion": {
                        "node": "Arnau",
                        "initial_error": deletion_error,
                        "removed_edges": removed_edges,
                        "node_exists": graph.get("node", "arnau") is not None,
                    },
                },
                indent=2,
                ensure_ascii=False,
            )
        )
    finally:
        nidx.close()


if __name__ == "__main__":
    main()
