# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""A bounded interpreter for the seven Cypher shapes in queries.md."""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

import grpc
from google.protobuf.json_format import MessageToDict
from nidx_protos import nidx_pb2, nidx_pb2_grpc


class UnsupportedQuery(ValueError):
    """The query is outside the supported Cypher subset."""


class QueryLimitExceeded(RuntimeError):
    """Executing the query would exceed a safety limit."""


class LpgReader(Protocol):
    def FindRecords(
        self, request: nidx_pb2.FindRecordsRequest, *, timeout: float
    ) -> nidx_pb2.FindRecordsResponse: ...

    def GetRecords(
        self, request: nidx_pb2.GetRecordsRequest, *, timeout: float
    ) -> nidx_pb2.GetRecordsResponse: ...


@dataclass(frozen=True)
class Path:
    nodes: tuple[nidx_pb2.LpgRecord, ...]
    relationships: tuple[nidx_pb2.LpgRecord, ...]


@dataclass(frozen=True)
class _Predicate:
    variable: str
    property: str
    operator: Literal["=", ">="]
    parameter: str


# Keywords are case-insensitive, while identifiers and backreferences are not.
IDENT = r"[A-Za-z_][A-Za-z_0-9]*"
W = r"\s*"
S = r"\s+"
ENTRY = rf"{IDENT}{W}:{W}\${IDENT}"
PROPERTIES = rf"{ENTRY}(?:{W},{W}{ENTRY})*"
CONDITION = rf"{IDENT}\.{IDENT}{W}(?:>=|=){W}\${IDENT}"
CONDITIONS = rf"{CONDITION}(?:{S}(?i:AND){S}{CONDITION})*"
NODE = rf"\({W}(?P<node>{IDENT}):(?P<label>{IDENT}){W}\{{{W}(?P<properties>{PROPERTIES}){W}\}}{W}\)"
PATTERNS = [
    ("lookup", rf"(?i:MATCH){S}{NODE}{S}(?i:RETURN){S}(?P=node)"),
    (
        "neighborhood",
        rf"(?i:MATCH){S}{NODE}{W}-\[{W}(?P<rel>{IDENT}){W}\]-{W}"
        rf"\({W}(?P<other>{IDENT}){W}\){S}(?i:RETURN){S}(?P=node){W},{W}"
        rf"(?P=rel){W},{W}(?P=other){S}(?i:LIMIT){S}(?P<limit>[0-9]+)",
    ),
    (
        "traverse",
        rf"(?i:MATCH){S}(?P<path>{IDENT}){W}={W}{NODE}{W}-\[:(?P<types>{IDENT}(?:\|{IDENT})*)"
        rf"\*(?P<min>[0-9]+)\.\.(?P<max>[0-9]+)\]->{W}"
        rf"\({W}(?P<other>{IDENT}){W}\){S}(?i:RETURN){S}(?P=path){S}"
        rf"(?i:LIMIT){S}(?P<limit>[0-9]+)",
    ),
    (
        "join",
        rf"(?i:MATCH){S}\({W}(?P<node>{IDENT}):(?P<label>{IDENT}){W}\){W}"
        rf"-\[:(?P<type1>{IDENT})\]->{W}\({W}(?P<middle>{IDENT}):(?P<label2>{IDENT}){W}\){W}"
        rf"<-\[:(?P<type2>{IDENT})\]-{W}\({W}(?P<other>{IDENT}):(?P<label3>{IDENT}){W}\){S}"
        rf"(?i:WHERE){S}(?P<where>{CONDITIONS}){S}"
        rf"(?i:RETURN){S}(?P=node){W},{W}(?P=middle){W},{W}(?P=other)",
    ),
    (
        "edge_filter",
        rf"(?i:MATCH){S}\({W}(?P<node>{IDENT}):(?P<label>{IDENT}){W}\){W}"
        rf"-\[{W}(?P<rel>{IDENT}):(?P<type1>{IDENT}){W}\]->{W}"
        rf"\({W}(?P<other>{IDENT}):(?P<label2>{IDENT}){W}\){S}"
        rf"(?i:WHERE){S}(?P<where>{CONDITIONS}){S}"
        rf"(?i:RETURN){S}(?P=node){W},{W}(?P=rel){W},{W}(?P=other)",
    ),
    (
        "aggregate",
        rf"(?i:MATCH){S}\({W}(?P<node>{IDENT}):(?P<label>{IDENT}){W}\){W}"
        rf"<-\[:(?P<type1>{IDENT})\]-{W}\({W}(?P<other>{IDENT}):(?P<label2>{IDENT}){W}\){S}"
        rf"(?i:RETURN){S}(?P=node)\.(?P<prop>{IDENT}){W},{W}(?i:count){W}"
        rf"\({W}(?P=other){W}\){S}(?i:AS){S}(?P<alias>{IDENT}){S}"
        rf"(?i:ORDER BY){S}(?P=alias){S}(?i:DESC){S}(?i:LIMIT){S}(?P<limit>[0-9]+)",
    ),
    (
        "shortest",
        rf"(?i:MATCH){S}(?P<path>{IDENT}){W}={W}(?i:shortestPath){W}\({W}{NODE}{W}"
        rf"-\[:(?P<type1>{IDENT})\*\.\.(?P<max>[0-9]+)\]-{W}"
        rf"\({W}(?P<other>{IDENT}):(?P<label2>{IDENT}){W}\{{{W}"
        rf"(?P<properties2>{PROPERTIES}){W}\}}{W}\){W}\){S}(?i:RETURN){S}(?P=path)",
    ),
]
COMPILED = [(name, re.compile(pattern)) for name, pattern in PATTERNS]


def parse(query: str) -> tuple[str, dict[str, str]]:
    query = query.strip().removesuffix(";").strip()
    for kind, pattern in COMPILED:
        match = pattern.fullmatch(query)
        if match:
            parts = {key: value for key, value in match.groupdict().items() if value is not None}
            variables = [
                parts[key] for key in ("node", "other", "middle", "rel", "path") if key in parts
            ]
            if len(variables) != len(set(variables)):
                raise UnsupportedQuery("Pattern variables must be distinct")
            return kind, parts
    raise UnsupportedQuery("Query does not match a supported Cypher shape")


def _predicates(parts: dict[str, str]) -> list[_Predicate]:
    predicates = []
    for key, variable in (("properties", "node"), ("properties2", "other")):
        seen: set[str] = set()
        for entry in parts.get(key, "").split(","):
            if not entry:
                continue
            prop, param = entry.split(":")
            prop = prop.strip()
            if prop in seen:
                raise UnsupportedQuery(f"Duplicate property {prop!r} in a property map")
            seen.add(prop)
            predicates.append(_Predicate(parts[variable], prop, "=", param.strip()[1:]))
    variables = {parts[key] for key in ("node", "other", "middle", "rel") if key in parts}
    for match in re.finditer(rf"({IDENT})\.({IDENT}){W}(>=|=){W}\$({IDENT})", parts.get("where", "")):
        variable, prop, operator, parameter = match.groups()
        if variable not in variables:
            raise UnsupportedQuery(f"Unknown predicate variable {variable!r}")
        predicates.append(_Predicate(variable, prop, "=" if operator == "=" else ">=", parameter))
    return predicates


def _value(record: nidx_pb2.LpgRecord, key: str) -> object:
    prop = record.properties.get(key)
    if prop is None:
        return None
    kind = prop.WhichOneof("value")
    if kind == "null_value":
        return None
    if kind in ("text", "integer", "float", "boolean"):
        return getattr(prop, kind)
    raise UnsupportedQuery(f"Property {key!r} has unsupported type {kind!r}")


def _equal(left: object, right: object) -> bool:
    if left is None or right is None:
        return False
    if type(left) is bool or type(right) is bool:
        return type(left) is bool and type(right) is bool and left == right
    return left == right


class Cypher:
    def __init__(
        self,
        client: LpgReader,
        graph_id: str,
        *,
        max_records: int = 10000,
        max_rpcs: int = 10000,
        max_results: int = 1000,
        max_hops: int = 6,
        timeout: float = 5,
    ) -> None:
        if not graph_id or min(max_records, max_rpcs, max_results, max_hops) < 1 or timeout <= 0:
            raise ValueError("graph_id and positive query limits are required")
        self.client, self.graph_id = client, graph_id
        self.max_records, self.max_rpcs = max_records, max_rpcs
        self.max_results, self.max_hops, self.timeout = max_results, max_hops, timeout

    def execute(self, query: str, parameters: Mapping[str, object]) -> list[dict[str, object]]:
        kind, p = parse(query)
        predicates = _predicates(p)
        for predicate in predicates:
            name = predicate.parameter
            if name not in parameters:
                raise ValueError(f"Missing parameter ${name}")
            value = parameters[name]
            if value is not None and type(value) not in (str, bool, int, float):
                raise ValueError(f"Parameter ${name} must be a scalar")
            if type(value) is float and not math.isfinite(value):
                raise ValueError(f"Parameter ${name} must be finite")
            if predicate.operator == ">=" and value is not None and type(value) not in (int, float):
                raise ValueError(f"Parameter ${name} for >= must be numeric")
        if "max" in p and not 1 <= int(p["max"]) <= self.max_hops:
            raise QueryLimitExceeded(f"Maximum path length must be between 1 and {self.max_hops}")
        if "min" in p and not 1 <= int(p["min"]) <= int(p["max"]):
            raise UnsupportedQuery("Traversal must have a positive, ordered hop range")
        limit = int(p["limit"]) if "limit" in p else None
        if limit is not None and limit > self.max_results:
            raise QueryLimitExceeded(f"LIMIT exceeds {self.max_results}")
        if limit == 0:
            return []
        execution = _Execution(self, parameters)
        rows = execution.run(kind, p, predicates)
        result: list[dict[str, object]] = []
        for row in rows:
            if len(result) == self.max_results:
                raise QueryLimitExceeded("Result limit exceeded; result would be incomplete")
            result.append(row)
            if limit is not None and len(result) == limit:
                break
        return result


class _Execution:
    def __init__(self, query: Cypher, parameters: Mapping[str, object]) -> None:
        self.query, self.parameters = query, parameters
        self.records = 0
        self.rpcs = 0
        self.nodes: dict[str, nidx_pb2.LpgRecord] = {}

    def _rpc(self) -> None:
        self.rpcs += 1
        if self.rpcs > self.query.max_rpcs:
            raise QueryLimitExceeded("RPC limit exceeded")

    def _count(self, amount: int) -> None:
        self.records += amount
        if self.records > self.query.max_records:
            raise QueryLimitExceeded("Record limit exceeded; result would be incomplete")

    def find(
        self,
        kind: nidx_pb2.LpgRecordKind.ValueType,
        *,
        label: str | None = None,
        source: str | None = None,
        target: str | None = None,
        incident_node: str | None = None,
        edge_type: str | None = None,
        filters: Sequence[nidx_pb2.LpgPropertyFilter] = (),
    ) -> Iterator[nidx_pb2.LpgRecord]:
        token = ""
        seen: set[str] = set()
        while True:
            self._rpc()
            request = nidx_pb2.FindRecordsRequest(
                graph_id=self.query.graph_id,
                kind=kind,
                page_size=min(1000, self.query.max_records),
                page_token=token,
                label=label,
                source=source,
                target=target,
                incident_node=incident_node,
                edge_type=edge_type,
                filters=filters,
            )
            response = self.query.client.FindRecords(request, timeout=self.query.timeout)
            self._count(len(response.records))
            yield from response.records
            token = response.next_page_token
            if not token:
                return
            if token in seen:
                raise RuntimeError("NidxLPG repeated a pagination token")
            seen.add(token)

    def node(self, id: str) -> nidx_pb2.LpgRecord:
        if id not in self.nodes:
            self._rpc()
            response = self.query.client.GetRecords(
                nidx_pb2.GetRecordsRequest(
                    graph_id=self.query.graph_id,
                    keys=[nidx_pb2.LpgRecordKey(kind=nidx_pb2.LPG_NODE, id=id)],
                ),
                timeout=self.query.timeout,
            )
            self._count(len(response.records))
            if len(response.records) != 1 or response.records[0].key.id != id:
                raise RuntimeError(f"Endpoint {id!r} disappeared or returned an invalid record")
            self.nodes[id] = response.records[0]
        return self.nodes[id]

    def filters(self, predicates: Sequence[_Predicate]) -> list[nidx_pb2.LpgPropertyFilter]:
        filters = []
        for predicate in predicates:
            value = self.parameters[predicate.parameter]
            if predicate.operator != "=":
                continue
            if type(value) is str:
                filters.append(nidx_pb2.LpgPropertyFilter(property=predicate.property, text=value))
            elif type(value) is bool:
                filters.append(nidx_pb2.LpgPropertyFilter(property=predicate.property, boolean=value))
            # Numeric comparisons cross integer/float types; typed index filters do not.
        return filters

    def matches(self, record: nidx_pb2.LpgRecord, predicates: Sequence[_Predicate]) -> bool:
        for predicate in predicates:
            left = _value(record, predicate.property)
            right = self.parameters[predicate.parameter]
            if predicate.operator == "=":
                if not _equal(left, right):
                    return False
            elif (
                not isinstance(left, (int, float))
                or isinstance(left, bool)
                or not isinstance(right, (int, float))
                or isinstance(right, bool)
                or left < right
            ):
                return False
        return True

    def matching_nodes(
        self, label: str, predicates: Sequence[_Predicate]
    ) -> Iterator[nidx_pb2.LpgRecord]:
        if any(self.parameters[predicate.parameter] is None for predicate in predicates):
            return
        for node in self.find(nidx_pb2.LPG_NODE, label=label, filters=self.filters(predicates)):
            if self.matches(node, predicates):
                yield node

    def edges(
        self, node: str, direction: str, edge_type: str | None = None
    ) -> Iterator[nidx_pb2.LpgRecord]:
        if direction == "source":
            yield from self.find(nidx_pb2.LPG_EDGE, source=node, edge_type=edge_type)
        elif direction == "target":
            yield from self.find(nidx_pb2.LPG_EDGE, target=node, edge_type=edge_type)
        elif direction == "incident_node":
            yield from self.find(nidx_pb2.LPG_EDGE, incident_node=node, edge_type=edge_type)
        else:
            raise ValueError(f"Unknown direction: {direction}")

    def run(
        self, kind: str, p: dict[str, str], predicates: Sequence[_Predicate]
    ) -> Iterator[dict[str, object]]:
        node_predicates = [predicate for predicate in predicates if predicate.variable == p["node"]]
        other_predicates = [
            predicate for predicate in predicates if predicate.variable == p.get("other")
        ]
        if kind in ("lookup", "neighborhood", "traverse", "join", "shortest"):
            starts = self.matching_nodes(p["label"], node_predicates)
        if kind == "lookup":
            for node in starts:
                yield {p["node"]: node}
        elif kind == "neighborhood":
            for node in starts:
                for edge in self.edges(node.key.id, "incident_node"):
                    other_id = edge.target if edge.source == node.key.id else edge.source
                    yield {p["node"]: node, p["rel"]: edge, p["other"]: self.node(other_id)}
        elif kind == "traverse":
            for node in starts:
                yield from self.paths(node, p)
        elif kind == "join":
            middle_predicates = [
                predicate for predicate in predicates if predicate.variable == p["middle"]
            ]
            for node in starts:
                for first in self.edges(node.key.id, "source", p["type1"]):
                    middle = self.node(first.target)
                    if p["label2"] not in middle.labels or not self.matches(middle, middle_predicates):
                        continue
                    for second in self.edges(middle.key.id, "target", p["type2"]):
                        if second.key.id == first.key.id:
                            continue
                        other = self.node(second.source)
                        if p["label3"] in other.labels and self.matches(other, other_predicates):
                            yield {p["node"]: node, p["middle"]: middle, p["other"]: other}
        elif kind == "edge_filter":
            edge_predicates = [predicate for predicate in predicates if predicate.variable == p["rel"]]
            for edge in self.find(
                nidx_pb2.LPG_EDGE, edge_type=p["type1"], filters=self.filters(edge_predicates)
            ):
                if not self.matches(edge, edge_predicates):
                    continue
                source, target = self.node(edge.source), self.node(edge.target)
                if (
                    p["label"] in source.labels
                    and p["label2"] in target.labels
                    and self.matches(source, node_predicates)
                    and self.matches(target, other_predicates)
                ):
                    yield {p["node"]: source, p["rel"]: edge, p["other"]: target}
        elif kind == "aggregate":
            counts: dict[str | None, int] = {}
            for edge in self.find(nidx_pb2.LPG_EDGE, edge_type=p["type1"]):
                source, target = self.node(edge.source), self.node(edge.target)
                if p["label2"] not in source.labels or p["label"] not in target.labels:
                    continue
                value = _value(target, p["prop"])
                if value is not None and not isinstance(value, str):
                    raise UnsupportedQuery("Grouping is supported only for string or null properties")
                counts[value] = counts.get(value, 0) + 1
            for value, count in sorted(
                counts.items(), key=lambda item: (-item[1], item[0] is None, item[0] or "")
            ):
                yield {f"{p['node']}.{p['prop']}": value, p["alias"]: count}
        elif kind == "shortest":
            ends = list(self.matching_nodes(p["label2"], other_predicates))
            for start in starts:
                for end in ends:
                    if start.key.id == end.key.id:
                        raise UnsupportedQuery("shortestPath with identical endpoints is not supported")
                    path = self.shortest(start, end, p["type1"], int(p["max"]))
                    if path is not None:
                        yield {p["path"]: path}

    def paths(self, start: nidx_pb2.LpgRecord, p: dict[str, str]) -> Iterator[dict[str, object]]:
        types = set(p["types"].split("|"))
        stack = [Path((start,), ())]
        while stack:
            path = stack.pop()
            length = len(path.relationships)
            if length >= int(p["min"]):
                yield {p["path"]: path}
            if length == int(p["max"]):
                continue
            used = {edge.key.id for edge in path.relationships}
            for edge in self.edges(path.nodes[-1].key.id, "source"):
                if edge.edge_type in types and edge.key.id not in used:
                    stack.append(
                        Path((*path.nodes, self.node(edge.target)), (*path.relationships, edge))
                    )

    def shortest(
        self, start: nidx_pb2.LpgRecord, end: nidx_pb2.LpgRecord, edge_type: str, max_hops: int
    ) -> Path | None:
        queue = deque([Path((start,), ())])
        seen = {start.key.id}
        while queue:
            path = queue.popleft()
            if len(path.relationships) == max_hops:
                continue
            node = path.nodes[-1]
            for edge in self.edges(node.key.id, "incident_node", edge_type):
                next_id = edge.target if edge.source == node.key.id else edge.source
                if next_id in seen:
                    continue
                next_node = self.node(next_id)
                next_path = Path((*path.nodes, next_node), (*path.relationships, edge))
                if next_id == end.key.id:
                    return next_path
                seen.add(next_id)
                queue.append(next_path)
        return None


def _json_value(value: object) -> object:
    if isinstance(value, nidx_pb2.LpgRecord):
        return MessageToDict(value, preserving_proto_field_name=True)
    if isinstance(value, Path):
        return {
            "nodes": [_json_value(node) for node in value.nodes],
            "relationships": [_json_value(edge) for edge in value.relationships],
        }
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", required=True, help="Nidx API gRPC address")
    parser.add_argument("--graph-id", required=True, help="LPG graph/shard ID")
    parser.add_argument("--query", required=True, help="Supported Cypher query")
    parser.add_argument("--parameters", default="{}", help="JSON object of Cypher parameters")
    args = parser.parse_args()
    parameters = json.loads(args.parameters)
    if not isinstance(parameters, dict):
        parser.error("--parameters must be a JSON object")
    with grpc.insecure_channel(args.address) as channel:
        rows = Cypher(nidx_pb2_grpc.NidxLPGStub(channel), args.graph_id).execute(args.query, parameters)
    print(
        json.dumps([{key: _json_value(value) for key, value in row.items()} for row in rows], indent=2)
    )


if __name__ == "__main__":
    main()
