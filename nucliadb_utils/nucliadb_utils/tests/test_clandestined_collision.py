from nucliadb_utils.clandestined import Cluster, RendezvousHash


def test_monkey_patch(clandestined):
    rendezvous = RendezvousHash()
    assert rendezvous.hash_function("lol") == 4294967295
    assert rendezvous.hash_function("wat") == 4294967295


def test_rendezvous_collision(clandestined):
    nodes = ["c", "b", "a"]
    rendezvous = RendezvousHash(nodes)
    for i in range(1000):
        assert "c" == rendezvous.find_node(i)


def test_rendezvous_names(clandestined):
    nodes = [1, 2, 3, "a", "b", "lol.wat.com"]
    rendezvous = RendezvousHash(nodes)
    for i in range(10):
        assert "lol.wat.com" == rendezvous.find_node(i)
    nodes = [1, "a", "0"]
    rendezvous = RendezvousHash(nodes)
    for i in range(10):
        assert "a" == rendezvous.find_node(i)


def test_cluster_collision(clandestined):
    nodes = {
        "1": {"zone": "a"},
        "2": {"zone": "a"},
        "3": {"zone": "b"},
        "4": {"zone": "b"},
    }
    cluster = Cluster(nodes)
    for n in range(100):
        for m in range(100):
            assert ["2", "4"] == sorted(cluster.find_nodes_by_index(n, m))
