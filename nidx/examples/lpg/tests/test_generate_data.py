# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import json
import math
import subprocess
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from nidx.examples.lpg.generate_data import Config, generate_records, write_dataset


def test_jsonl_graph_integrity(tmp_path: Path) -> None:
    config = Config(products=8, warehouses=4, clients=5)
    output = tmp_path / "graph.jsonl"
    counts = write_dataset(output, config)
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    nodes = {row["id"]: row for row in rows if row["kind"] == "node"}
    edges = [row for row in rows if row["kind"] == "edge"]
    assert counts == {"nodes": 17, "edges": 7 * 3 + 2 + 4 * 5 + 3 * 10}
    assert len({row["id"] for row in rows}) == len(rows)
    assert all(row["kind"] == "node" for row in rows[:17])
    assert len(edges) == counts["edges"]
    expected_labels = {
        "STOCKED_AT": ("Product", "Warehouse"),
        "BUYS": ("Client", "Product"),
        "SUPPLIES": ("Warehouse", "Client"),
    }
    incident = set()
    triples = Counter()
    for row in edges:
        source_label, target_label = expected_labels[row["edge_type"]]
        assert nodes[row["source"]]["labels"] == [source_label]
        assert nodes[row["target"]]["labels"] == [target_label]
        incident.update((row["source"], row["target"]))
        triples[row["source"], row["edge_type"], row["target"]] += 1
    assert triples["P00000001", "STOCKED_AT", "W00000001"] >= 2
    assert {"P00000008", "W00000004", "C00000005"}.isdisjoint(incident)
    assert {row["kind"] for row in rows if row["text_chunks"]} == {"node", "edge"}
    assert all("embeddings" not in row for row in rows)
    before = output.read_bytes()
    with pytest.raises(FileExistsError):
        write_dataset(output, config)
    assert output.read_bytes() == before


def test_reproducibility_and_vectors() -> None:
    config = Config(products=3, warehouses=2, clients=2, vector_dimension=7)
    rows = list(generate_records(config))
    assert rows == list(generate_records(config))
    assert rows != list(generate_records(replace(config, seed=43)))
    for row in rows:
        assert row["embeddings"].keys() == row["text_chunks"].keys()
        for name, vectors in row["embeddings"].items():
            assert len(vectors) == len(row["text_chunks"][name])
            for vector in vectors:
                assert len(vector) == 7
                assert all(math.isfinite(value) for value in vector)
                assert sum(value * value for value in vector) == pytest.approx(1)
    without_vectors = [{key: value for key, value in row.items() if key != "embeddings"} for row in rows]
    assert without_vectors == list(generate_records(replace(config, vector_dimension=0)))


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"products": -1}, "products must be nonnegative"),
        ({"vector_dimension": -1}, "vector_dimension must be nonnegative"),
        ({"isolated_per_label": 201}, "cannot exceed"),
        ({"warehouses": 1}, "STOCKED_AT requires"),
        ({"products": 1, "stocked_per_product": 0}, "BUYS requires"),
        ({"clients": 1, "buys_per_client": 0}, "SUPPLIES requires"),
    ],
)
def test_invalid_config(overrides: dict[str, int], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        replace(Config(), **overrides)


def test_no_edges_and_empty_dataset() -> None:
    config = Config(products=1, warehouses=1, clients=1)
    assert [row["kind"] for row in generate_records(config)] == ["node"] * 3
    assert (
        list(generate_records(Config(products=0, warehouses=0, clients=0, isolated_per_label=0))) == []
    )


def test_cli(tmp_path: Path) -> None:
    output = tmp_path / "cli.jsonl"
    command = [
        sys.executable,
        "-m",
        "nidx.examples.lpg.generate_data",
        "--output",
        str(output),
        "--products",
        "3",
        "--warehouses",
        "2",
        "--clients",
        "2",
        "--vector-dimension",
        "3",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    assert json.loads(result.stdout) == {
        "output": str(output),
        "seed": 42,
        "vector_dimension": 3,
        "nodes": 7,
        "edges": 23,
    }
    assert len(output.read_text().splitlines()) == 30
    invalid = subprocess.run([*command, "--products", "-1"], capture_output=True, text=True)
    assert invalid.returncode == 2
    assert "products must be nonnegative" in invalid.stderr
