from typing import Dict, List

import pytest

from nucliadb_utils.clandestined import RendezvousHash


def test_init_no_options():
    rendezvous = RendezvousHash()
    assert 0 == len(rendezvous.nodes)
    assert 1361238019 == rendezvous.hash_function("6666")


def test_init():
    nodes = ["0", "1", "2"]
    rendezvous = RendezvousHash(nodes=nodes)
    assert 3 == len(rendezvous.nodes)
    assert 1361238019 == rendezvous.hash_function("6666")


def test_seed():
    rendezvous = RendezvousHash(seed=10)
    assert 2981722772 == rendezvous.hash_function("6666")


def test_add_node():
    rendezvous = RendezvousHash()
    rendezvous.add_node("1")
    assert 1 == len(rendezvous.nodes)
    rendezvous.add_node("1")
    assert 1 == len(rendezvous.nodes)
    rendezvous.add_node("2")
    assert 2 == len(rendezvous.nodes)
    rendezvous.add_node("1")
    assert 2 == len(rendezvous.nodes)


def test_remove_node():
    nodes = ["0", "1", "2"]
    rendezvous = RendezvousHash(nodes=nodes)
    rendezvous.remove_node("2")
    assert 2 == len(rendezvous.nodes)
    with pytest.raises(ValueError):
        rendezvous.remove_node("2")
    assert 2 == len(rendezvous.nodes)
    rendezvous.remove_node("1")
    assert 1 == len(rendezvous.nodes)
    rendezvous.remove_node("0")
    assert 0 == len(rendezvous.nodes)


def test_find_node():
    nodes = ["0", "1", "2"]
    rendezvous = RendezvousHash(nodes=nodes)
    assert "0" == rendezvous.find_node("ok")
    assert "1" == rendezvous.find_node("mykey")
    assert "2" == rendezvous.find_node("wat")


def test_find_node_after_removal():
    nodes = ["0", "1", "2"]
    rendezvous = RendezvousHash(nodes=nodes)
    rendezvous.remove_node("1")
    assert "0" == rendezvous.find_node("ok")
    assert "0" == rendezvous.find_node("mykey")
    assert "2" == rendezvous.find_node("wat")


def test_find_node_after_addition():
    nodes = ["0", "1", "2"]
    rendezvous = RendezvousHash(nodes=nodes)
    assert "0" == rendezvous.find_node("ok")
    assert "1" == rendezvous.find_node("mykey")
    assert "2" == rendezvous.find_node("wat")
    assert "2" == rendezvous.find_node("lol")
    rendezvous.add_node("3")
    assert "0" == rendezvous.find_node("ok")
    assert "1" == rendezvous.find_node("mykey")
    assert "2" == rendezvous.find_node("wat")
    assert "3" == rendezvous.find_node("lol")


def test_grow() -> None:
    rendezvous = RendezvousHash()

    placements: Dict[str, List[int]] = {}
    for i in range(10):
        rendezvous.add_node(str(i))
        placements[str(i)] = []
    for i in range(1000):
        node = rendezvous.find_node(str(i))
        placements[node].append(i)

    new_placements: Dict[str, List[int]] = {}
    for i in range(20):
        rendezvous.add_node(str(i))
        new_placements[str(i)] = []
    for i in range(1000):
        node = rendezvous.find_node(str(i))
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
    assert 1062 == (added + removed)


def test_shrink() -> None:
    rendezvous = RendezvousHash()

    placements: Dict[str, List[int]] = {}
    for i in range(10):
        rendezvous.add_node(str(i))
        placements[str(i)] = []
    for i in range(1000):
        node = rendezvous.find_node(str(i))
        placements[node].append(i)

    rendezvous.remove_node("9")
    new_placements: Dict[str, List[int]] = {}
    for i in range(9):
        new_placements[str(i)] = []
    for i in range(1000):
        node = rendezvous.find_node(str(i))
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

    assert 202 == (added + removed)
