import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from .storage import EmbeddedNidx, Graph, PropertyFilter


def main() -> None:
    parser = argparse.ArgumentParser(description="Milestone 1: LPG storage on real embedded nidx")
    parser.add_argument("--directory", type=Path, required=True, help="Persistent prototype storage")
    args = parser.parse_args()
    database_url = os.environ.get("LPG_DATABASE_URL")
    if not database_url:
        parser.error("Set LPG_DATABASE_URL to a dedicated PostgreSQL database")
    nidx = EmbeddedNidx(database_url, args.directory)
    try:
        graph = Graph.create(nidx, str(uuid4()))
        graph.upsert_node("P1", ["Product"], {"name": "Laptop", "price": 1200, "available": True})
        graph.upsert_node("W1", ["Warehouse"], {"country": "Spain"})
        graph.upsert_node("C1", ["Client"], {"name": "Acme"})
        graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10})
        graph.upsert_edge("E2", "P1", "STOCKED_AT", "W1", {"quantity": 25})
        graph.upsert_edge("E3", "C1", "BUYS", "P1", {"quantity": 2})
        print(json.dumps({"tenant": graph.tenant, "shard_id": graph.shard_id}))
        print("Nodes with price >= 1000:")
        print(
            json.dumps(
                [asdict(r) for r in graph.find("node", PropertyFilter("price", "range", lower=1000))],
                indent=2,
            )
        )
        print("Parallel edge with quantity >= 20, recovered through the relation index:")
        print(
            json.dumps(
                [asdict(r) for r in graph.topology(PropertyFilter("quantity", "range", lower=20))],
                indent=2,
            )
        )
        graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 5})
        reopened = Graph(nidx, graph.tenant, graph.shard_id)
        assert [r.id for r in reopened.topology()] == ["E1", "E2", "E3"]
        reopened.delete_node("P1", detach=True)
        assert reopened.find("edge") == []
        assert [r.id for r in reopened.find("node")] == ["C1", "W1"]
        print("Reopen, replacement, and cascading deletion verified.")
    finally:
        nidx.close()


if __name__ == "__main__":
    main()
