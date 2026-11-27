# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Generate offline enterprise LPG JSONL without importing nidx or connecting to a database."""

import argparse
import json
import math
import random
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal, NotRequired, TypedDict

from faker import Faker


class GraphRow(TypedDict):
    kind: Literal["node", "edge"]
    id: str
    properties: dict[str, str | int | float | bool | list[str]]
    text_chunks: dict[str, list[str]]
    labels: NotRequired[list[str]]
    source: NotRequired[str]
    edge_type: NotRequired[str]
    target: NotRequired[str]
    embeddings: NotRequired[dict[str, list[list[float]]]]


@dataclass(frozen=True)
class Config:
    products: int = 1_000
    warehouses: int = 20
    clients: int = 200
    stocked_per_product: int = 3
    buys_per_client: int = 5
    supplies_per_warehouse: int = 10
    isolated_per_label: int = 1
    seed: int = 42
    vector_dimension: int = 0
    max_node_properties: int = 0
    max_edge_properties: int = 0

    def __post_init__(self) -> None:
        for name in (
            "products",
            "warehouses",
            "clients",
            "stocked_per_product",
            "buys_per_client",
            "supplies_per_warehouse",
            "isolated_per_label",
            "vector_dimension",
            "max_node_properties",
            "max_edge_properties",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be nonnegative")
        for name, minimum in (("max_node_properties", 10), ("max_edge_properties", 5)):
            if 0 < getattr(self, name) < minimum:
                raise ValueError(f"{name} must be 0 or at least {minimum}, including text properties")
        if self.isolated_per_label > min(self.products, self.warehouses, self.clients):
            raise ValueError("isolated_per_label cannot exceed any node count")
        products = self.products - self.isolated_per_label
        warehouses = self.warehouses - self.isolated_per_label
        clients = self.clients - self.isolated_per_label
        if products and self.stocked_per_product and not warehouses:
            raise ValueError("STOCKED_AT requires a non-isolated warehouse")
        if clients and self.buys_per_client and not products:
            raise ValueError("BUYS requires a non-isolated product")
        if warehouses and self.supplies_per_warehouse and not clients:
            raise ValueError("SUPPLIES requires a non-isolated client")


CATEGORIES = (
    ("electronics", ("laptop", "monitor", "router"), "computing connectivity office equipment"),
    ("food", ("apple", "coffee", "rice"), "fresh groceries food pantry ingredients"),
    ("industrial", ("pump", "drill", "valve"), "industrial maintenance factory machinery"),
    ("medical", ("bandage", "thermometer", "gloves"), "clinical healthcare protective supplies"),
    ("furniture", ("desk", "chair", "shelf"), "workspace furniture storage seating"),
)
COUNTRIES = ("Spain", "France", "Germany", "Italy", "United Kingdom", "United States")


def generate_records(config: Config) -> Iterator[GraphRow]:
    fake = Faker("en_US")
    fake.seed_instance(config.seed)
    rng = random.Random(config.seed)
    vector_rng = random.Random(config.seed + 1)
    property_rng = random.Random(config.seed + 2)

    def enrich(row: GraphRow) -> GraphRow:
        maximum = config.max_node_properties if row["kind"] == "node" else config.max_edge_properties
        if maximum:
            count = len(row["properties"]) + len(row["text_chunks"])
            for index in range(property_rng.randint(count, maximum) - count):
                name = f"synthetic_{index:03d}"
                match index % 6:
                    case 0:
                        row["properties"][name] = (
                            f"{property_rng.choice(COUNTRIES)} {property_rng.choice(CATEGORIES)[2]}"
                        )
                    case 1:
                        row["properties"][name] = property_rng.randint(-1_000_000, 1_000_000)
                    case 2:
                        row["properties"][name] = round(property_rng.uniform(-10_000, 10_000), 4)
                    case 3:
                        row["properties"][name] = property_rng.choice((True, False))
                    case 4:
                        row["properties"][name] = property_rng.sample(
                            COUNTRIES, property_rng.randint(1, 3)
                        )
                    case 5:
                        row["properties"][name] = date.fromordinal(
                            property_rng.randint(
                                date(2020, 1, 1).toordinal(), date(2025, 12, 31).toordinal()
                            )
                        ).isoformat()
        if config.vector_dimension:
            embeddings: dict[str, list[list[float]]] = {}
            for name, chunks in row["text_chunks"].items():
                vectors = []
                for _ in chunks:
                    values = [vector_rng.gauss(0, 1) for _ in range(config.vector_dimension)]
                    norm = math.sqrt(sum(value * value for value in values))
                    if norm == 0:
                        raise ValueError("Generated a zero vector; choose another seed")
                    vectors.append([value / norm for value in values])
                embeddings[name] = vectors
            row["embeddings"] = embeddings
        return row

    for index in range(1, config.products + 1):
        category, items, keywords = rng.choice(CATEGORIES)
        item = rng.choice(items)
        brand = fake.company()
        country = rng.choice(COUNTRIES)
        yield enrich(
            {
                "kind": "node",
                "id": f"P{index:08d}",
                "labels": ["Product"],
                "properties": {
                    "name": f"{brand} {item}",
                    "category": category,
                    "brand": brand,
                    "price": rng.randint(100, 200_000) / 100,
                    "currency": "EUR",
                    "available": rng.random() < 0.9,
                    "country": country,
                    "tags": [category, item],
                },
                "text_chunks": {
                    "description": [
                        f"{brand} supplies this {item} for {category} customers in {country}.",
                        f"Designed for {keywords}. {fake.sentence(nb_words=12)}",
                    ],
                    "features": [f"{item}: {keywords}. {fake.sentence(nb_words=8)}"],
                },
            }
        )
    for index in range(1, config.warehouses + 1):
        country = rng.choice(COUNTRIES)
        city = fake.city()
        refrigerated = rng.choice((True, False))
        yield enrich(
            {
                "kind": "node",
                "id": f"W{index:08d}",
                "labels": ["Warehouse"],
                "properties": {
                    "name": f"{city} distribution centre",
                    "country": country,
                    "city": city,
                    "capacity": rng.randint(1_000, 100_000),
                    "refrigerated": refrigerated,
                },
                "text_chunks": {
                    "description": [
                        f"Distribution warehouse in {city}, {country}, for regional delivery."
                    ],
                    "handling": [
                        "Refrigerated storage for food and medical supplies."
                        if refrigerated
                        else "Ambient storage for electronics, furniture and industrial supplies."
                    ],
                },
            }
        )
    for index in range(1, config.clients + 1):
        name = fake.company()
        sector = rng.choice(CATEGORIES)[0]
        country = rng.choice(COUNTRIES)
        yield enrich(
            {
                "kind": "node",
                "id": f"C{index:08d}",
                "labels": ["Client"],
                "properties": {
                    "name": name,
                    "sector": sector,
                    "country": country,
                    "tier": rng.choice(("standard", "premium", "enterprise")),
                    "active": rng.random() < 0.95,
                    "registered_on": fake.date_between(
                        start_date=date(2020, 1, 1), end_date=date(2025, 12, 31)
                    ).isoformat(),
                },
                "text_chunks": {
                    "profile": [
                        f"{name} is a {sector} business in {country} purchasing regional supplies."
                    ],
                },
            }
        )

    products = config.products - config.isolated_per_label
    warehouses = config.warehouses - config.isolated_per_label
    clients = config.clients - config.isolated_per_label
    edge_index = 0

    def edge(source: str, edge_type: str, target: str) -> GraphRow:
        nonlocal edge_index
        edge_index += 1
        return enrich(
            {
                "kind": "edge",
                "id": f"E{edge_index:010d}",
                "source": source,
                "edge_type": edge_type,
                "target": target,
                "properties": {
                    "quantity": rng.randint(1, 1_000),
                    "priority": rng.choice(("normal", "urgent")),
                    "confirmed": rng.choice((True, False)),
                    "date": fake.date_between(
                        start_date=date(2025, 1, 1), end_date=date(2025, 12, 31)
                    ).isoformat(),
                },
                "text_chunks": {
                    "note": [
                        f"{source} {edge_type} {target}. Shipment and procurement: {fake.sentence(nb_words=10)}"
                    ]
                },
            }
        )

    for index in range(1, products + 1):
        for _ in range(config.stocked_per_product):
            yield edge(f"P{index:08d}", "STOCKED_AT", f"W{rng.randint(1, warehouses):08d}")
    # Separate identities for the same triple exercise parallel-edge storage and deletion.
    if products and warehouses and config.stocked_per_product:
        yield edge("P00000001", "STOCKED_AT", "W00000001")
        yield edge("P00000001", "STOCKED_AT", "W00000001")
    for index in range(1, clients + 1):
        for _ in range(config.buys_per_client):
            yield edge(f"C{index:08d}", "BUYS", f"P{rng.randint(1, products):08d}")
    for index in range(1, warehouses + 1):
        for _ in range(config.supplies_per_warehouse):
            yield edge(f"W{index:08d}", "SUPPLIES", f"C{rng.randint(1, clients):08d}")


def write_dataset(path: Path, config: Config) -> dict[str, int]:
    counts: Counter[str] = Counter()
    with path.open("x", encoding="utf-8") as output:
        for row in generate_records(config):
            output.write(
                json.dumps(row, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n"
            )
            counts[row["kind"]] += 1
    return {"nodes": counts["node"], "edges": counts["edge"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New JSONL file (never overwritten)")
    defaults = Config()
    for name in (
        "products",
        "warehouses",
        "clients",
        "stocked_per_product",
        "buys_per_client",
        "supplies_per_warehouse",
        "isolated_per_label",
        "seed",
        "vector_dimension",
        "max_node_properties",
        "max_edge_properties",
    ):
        parser.add_argument(f"--{name.replace('_', '-')}", type=int, default=getattr(defaults, name))
    args = parser.parse_args()
    try:
        config = Config(
            products=args.products,
            warehouses=args.warehouses,
            clients=args.clients,
            stocked_per_product=args.stocked_per_product,
            buys_per_client=args.buys_per_client,
            supplies_per_warehouse=args.supplies_per_warehouse,
            isolated_per_label=args.isolated_per_label,
            seed=args.seed,
            vector_dimension=args.vector_dimension,
            max_node_properties=args.max_node_properties,
            max_edge_properties=args.max_edge_properties,
        )
    except ValueError as error:
        parser.error(str(error))
    counts = write_dataset(args.output, config)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "seed": config.seed,
                "vector_dimension": config.vector_dimension,
                **counts,
            }
        )
    )


if __name__ == "__main__":
    main()
