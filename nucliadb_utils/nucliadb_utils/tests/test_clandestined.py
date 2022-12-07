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

from typing import Dict, List

import pytest

from nucliadb_utils.clandestined import Cluster


def test_init_no_options():
    cluster = Cluster()
    assert cluster.seed == 0
    assert cluster.replicas == 2
    assert cluster.nodes == {}
    assert cluster.zones == []
    assert dict(cluster.zone_members) == {}
    assert {} == dict(cluster.rings)


def test_seed():
    cluster = Cluster(seed=10)
    assert cluster.seed == 10


def test_init_single_zone():
    cluster_config = {
        "1": {},
        "2": {},
        "3": {},
    }
    cluster = Cluster(cluster_config, replicas=1)
    assert 1 == cluster.replicas
    assert 3 == len(cluster.nodes)
    assert 1 == len(cluster.zones)
    assert 3 == len(cluster.zone_members[None])
    assert 1 == len(cluster.rings)
    assert 3 == len(cluster.rings[None].nodes)


def test_init_zones():
    cluster_config = {
        "1": {"zone": "a"},
        "2": {"zone": "b"},
        "3": {"zone": "a"},
        "4": {"zone": "b"},
        "5": {"zone": "a"},
        "6": {"zone": "b"},
    }
    cluster = Cluster(cluster_config)
    assert 2 == cluster.replicas
    assert 6 == len(cluster.nodes)
    assert ["a", "b"] == cluster.zones
    assert ["1", "3", "5"] == sorted(cluster.zone_members["a"])
    assert ["2", "4", "6"] == sorted(cluster.zone_members["b"])
    assert 2 == len(cluster.rings)
    assert 3 == len(cluster.rings["a"].nodes)
    assert 3 == len(cluster.rings["b"].nodes)


def test_add_zone_1():
    cluster = Cluster()
    assert 0 == len(cluster.nodes)
    assert [] == cluster.zones
    assert 0 == len(cluster.zone_members)
    assert 0 == len(cluster.rings)

    cluster.add_zone("b")
    assert 0 == len(cluster.nodes)
    assert ["b"] == cluster.zones
    assert 0 == len(cluster.zone_members["b"])
    assert 0 == len(cluster.rings)

    cluster.add_zone("b")
    assert 0 == len(cluster.nodes)
    assert ["b"] == cluster.zones
    assert 0 == len(cluster.zone_members["b"])
    assert 0 == len(cluster.rings)

    cluster.add_zone("a")
    assert 0 == len(cluster.nodes)
    assert ["a", "b"] == cluster.zones
    assert 0 == len(cluster.zone_members["a"])
    assert 0 == len(cluster.zone_members["b"])
    assert 0 == len(cluster.rings)


def test_add_node():
    cluster = Cluster()
    assert 0 == len(cluster.nodes)
    assert [] == cluster.zones
    assert 0 == len(cluster.zone_members)
    assert 0 == len(cluster.rings)

    cluster.add_node("2", node_zone="b")
    assert 1 == len(cluster.nodes)
    assert ["b"] == cluster.zones
    assert 1 == len(cluster.zone_members)
    assert ["2"] == sorted(cluster.zone_members["b"])
    assert 1 == len(cluster.rings)

    cluster.add_node("1", node_zone="a")
    assert 2 == len(cluster.nodes)
    assert ["a", "b"] == cluster.zones
    assert 2 == len(cluster.zone_members)
    assert ["1"] == sorted(cluster.zone_members["a"])
    assert ["2"] == sorted(cluster.zone_members["b"])
    assert 2 == len(cluster.rings)

    cluster.add_node("21", node_zone="b")
    assert 3 == len(cluster.nodes)
    assert ["a", "b"] == cluster.zones
    assert 2 == len(cluster.zone_members)
    assert ["1"] == sorted(cluster.zone_members["a"])
    assert ["2", "21"] == sorted(cluster.zone_members["b"])
    assert 2 == len(cluster.rings)

    with pytest.raises(ValueError):
        cluster.add_node("21")

    with pytest.raises(ValueError):
        cluster.add_node("21", None, None)

    with pytest.raises(ValueError):
        cluster.add_node("21", None, "b")

    cluster.add_node("22", node_zone="c")
    assert 4 == len(cluster.nodes)
    assert ["a", "b", "c"] == cluster.zones
    assert 3 == len(cluster.zone_members)
    assert ["1"] == sorted(cluster.zone_members["a"])
    assert ["2", "21"] == sorted(cluster.zone_members["b"])
    assert ["22"] == sorted(cluster.zone_members["c"])
    assert 3 == len(cluster.rings)


def test_remove_node():
    cluster_config = {
        "1": {"zone": "a"},
        "2": {"zone": "b"},
        "3": {"zone": "a"},
        "4": {"zone": "b"},
        "5": {"zone": "c"},
        "6": {"zone": "c"},
    }
    cluster = Cluster(cluster_config)

    cluster.remove_node("4", node_zone="b")
    assert 5 == len(cluster.nodes)
    assert ["a", "b", "c"] == cluster.zones
    assert 3 == len(cluster.zone_members)
    assert ["1", "3"] == sorted(cluster.zone_members["a"])
    assert ["2"] == sorted(cluster.zone_members["b"])
    assert ["5", "6"] == sorted(cluster.zone_members["c"])
    assert 3 == len(cluster.rings)

    cluster.remove_node("2", node_zone="b")
    assert 4 == len(cluster.nodes)
    assert ["a", "c"] == cluster.zones
    assert 2 == len(cluster.zone_members)
    assert ["1", "3"] == sorted(cluster.zone_members["a"])
    assert [] == sorted(cluster.zone_members["b"])
    assert ["5", "6"] == sorted(cluster.zone_members["c"])
    assert 2 == len(cluster.rings)


def test_node_name_by_id():
    cluster_config = {
        "1": {"name": "node1", "zone": "a"},
        "2": {"name": "node2", "zone": "b"},
        "3": {"name": "node3", "zone": "a"},
        "4": {"name": "node4", "zone": "b"},
        "5": {"name": "node5", "zone": "c"},
        "6": {"name": "node6", "zone": "c"},
    }
    cluster = Cluster(cluster_config)

    assert "node1" == cluster.node_name("1")
    assert "node2" == cluster.node_name("2")
    assert "node3" == cluster.node_name("3")
    assert "node4" == cluster.node_name("4")
    assert "node5" == cluster.node_name("5")
    assert "node6" == cluster.node_name("6")
    assert cluster.node_name("7") is None


def test_find_nodes():
    cluster_config = {
        "1": {"name": "node1", "zone": "a"},
        "2": {"name": "node2", "zone": "a"},
        "3": {"name": "node3", "zone": "b"},
        "4": {"name": "node4", "zone": "b"},
        "5": {"name": "node5", "zone": "c"},
        "6": {"name": "node6", "zone": "c"},
    }
    cluster = Cluster(cluster_config)

    assert ["2", "3"] == cluster.find_nodes("lol")
    assert ["6", "2"] == cluster.find_nodes("wat")
    assert ["5", "2"] == cluster.find_nodes("ok")
    assert ["1", "3"] == cluster.find_nodes("bar")
    assert ["1", "3"] == cluster.find_nodes("foo")
    assert ["2", "4"] == cluster.find_nodes("slap")


def test_find_nodes_by_index():
    cluster_config = {
        "1": {"name": "node1", "zone": "a"},
        "2": {"name": "node2", "zone": "a"},
        "3": {"name": "node3", "zone": "b"},
        "4": {"name": "node4", "zone": "b"},
        "5": {"name": "node5", "zone": "c"},
        "6": {"name": "node6", "zone": "c"},
    }
    cluster = Cluster(cluster_config)

    assert ["6", "1"] == cluster.find_nodes_by_index(1, 1)
    assert ["2", "4"] == cluster.find_nodes_by_index(1, 2)
    assert ["4", "5"] == cluster.find_nodes_by_index(1, 3)
    assert ["1", "4"] == cluster.find_nodes_by_index(2, 1)
    assert ["3", "5"] == cluster.find_nodes_by_index(2, 2)
    assert ["5", "2"] == cluster.find_nodes_by_index(2, 3)


def test_grow() -> None:
    cluster_config = {
        "1": {"name": "node1", "zone": "a"},
        "2": {"name": "node2", "zone": "a"},
        "3": {"name": "node3", "zone": "b"},
        "4": {"name": "node4", "zone": "b"},
        "5": {"name": "node5", "zone": "c"},
        "6": {"name": "node6", "zone": "c"},
    }
    cluster = Cluster(cluster_config)

    placements: Dict[str, List[str]] = {}
    for i in cluster.nodes:
        placements[i] = []
    for i in range(1000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            placements[node].append(i)

    cluster.add_node("7", node_name="node7", node_zone="a")
    cluster.add_node("8", node_name="node8", node_zone="b")
    cluster.add_node("9", node_name="node9", node_zone="c")

    new_placements: Dict[str, List[str]] = {}
    for i in cluster.nodes:
        new_placements[i] = []
    for i in range(1000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            new_placements[node].append(i)

    keys = [k for sublist in placements.values() for k in sublist]
    new_keys = [k for sublist in new_placements.values() for k in sublist]
    assert sorted(keys) == sorted(new_keys)

    added = 0
    removed = 0
    for node, assignments in new_placements.items():
        after = set(assignments)
        before = set(placements.get(node, []))
        removed += len(before.difference(after))
        added += len(after.difference(before))

    assert added == removed
    assert 1384 == (added + removed)


def test_shrink() -> None:
    cluster_config = {
        "1": {"name": "node1", "zone": "a"},
        "2": {"name": "node2", "zone": "a"},
        "3": {"name": "node3", "zone": "b"},
        "4": {"name": "node4", "zone": "b"},
        "5": {"name": "node5", "zone": "c"},
        "6": {"name": "node6", "zone": "c"},
        "7": {"name": "node7", "zone": "a"},
        "8": {"name": "node8", "zone": "a"},
        "9": {"name": "node9", "zone": "b"},
        "10": {"name": "node10", "zone": "b"},
        "11": {"name": "node11", "zone": "c"},
        "12": {"name": "node12", "zone": "c"},
    }
    cluster = Cluster(cluster_config)

    placements: Dict[str, List[int]] = {}
    for i in cluster.nodes:
        placements[i] = []
    for i in range(10000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            placements[node].append(i)

    cluster.remove_node("7", node_name="node7", node_zone="a")
    cluster.remove_node("9", node_name="node9", node_zone="b")
    cluster.remove_node("11", node_name="node11", node_zone="c")

    new_placements: Dict[str, List[int]] = {}
    for i in cluster.nodes:
        new_placements[i] = []
    for i in range(10000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            new_placements[node].append(i)

    keys = [k for sublist in placements.values() for k in sublist]
    new_keys = [k for sublist in new_placements.values() for k in sublist]
    assert sorted(keys) == sorted(new_keys)

    added = 0
    removed = 0
    for node, assignments in placements.items():
        after = set(assignments)
        before = set(new_placements.get(node, []))
        removed += len(before.difference(after))
        added += len(after.difference(before))

    assert added == removed
    assert 9804 == (added + removed)


def test_add_zone() -> None:
    cluster_config = {
        "1": {"name": "node1", "zone": "a"},
        "2": {"name": "node2", "zone": "a"},
        "3": {"name": "node3", "zone": "b"},
        "4": {"name": "node4", "zone": "b"},
    }
    cluster = Cluster(cluster_config)

    placements: Dict[str, List[str]] = {}
    for i in cluster.nodes:
        placements[i] = []
    for i in range(1000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            placements[node].append(i)

    cluster.add_node("5", node_name="node5", node_zone="c")
    cluster.add_node("6", node_name="node6", node_zone="c")

    new_placements: Dict[str, List[str]] = {}
    for i in cluster.nodes:
        new_placements[i] = []
    for i in range(1000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            new_placements[node].append(i)

    keys = [k for sublist in placements.values() for k in sublist]
    new_keys = [k for sublist in new_placements.values() for k in sublist]
    assert sorted(keys) == sorted(new_keys)

    added = 0
    removed = 0
    for node, assignments in new_placements.items():
        after = set(assignments)
        before = set(placements.get(node, []))
        removed += len(before.difference(after))
        added += len(after.difference(before))

    assert added == removed
    assert 1332 == (added + removed)


def test_remove_zone() -> None:
    cluster_config = {
        "1": {"name": "node1", "zone": "a"},
        "2": {"name": "node2", "zone": "a"},
        "3": {"name": "node3", "zone": "b"},
        "4": {"name": "node4", "zone": "b"},
        "5": {"name": "node5", "zone": "c"},
        "6": {"name": "node6", "zone": "c"},
        "7": {"name": "node7", "zone": "a"},
        "8": {"name": "node8", "zone": "a"},
        "9": {"name": "node9", "zone": "b"},
        "10": {"name": "node10", "zone": "b"},
        "11": {"name": "node11", "zone": "c"},
        "12": {"name": "node12", "zone": "c"},
    }
    cluster = Cluster(cluster_config)

    placements: Dict[str, List[str]] = {}
    for i in cluster.nodes:
        placements[i] = []
    for i in range(10000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            placements[node].append(i)

    cluster.remove_node("5", node_name="node5", node_zone="c")
    cluster.remove_node("6", node_name="node6", node_zone="c")
    cluster.remove_node("11", node_name="node11", node_zone="c")
    cluster.remove_node("12", node_name="node12", node_zone="c")
    with pytest.raises(ValueError):
        cluster.remove_node("12", "node12", "c")

    new_placements: Dict[str, List[str]] = {}
    for i in cluster.nodes:
        new_placements[i] = []
    for i in range(10000):
        nodes = cluster.find_nodes(str(i))
        for node in nodes:
            new_placements[node].append(i)

    keys = [k for sublist in placements.values() for k in sublist]
    new_keys = [k for sublist in new_placements.values() for k in sublist]
    assert sorted(keys) == sorted(new_keys)

    added = 0
    removed = 0
    for node, assignments in placements.items():
        after = set(assignments)
        before = set(new_placements.get(node, []))
        removed += len(before.difference(after))
        added += len(after.difference(before))

    assert added == removed
    assert 13332 == (added + removed)
