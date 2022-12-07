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
