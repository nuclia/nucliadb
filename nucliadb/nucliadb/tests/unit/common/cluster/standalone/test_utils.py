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
import os
import tempfile
from unittest.mock import patch

import pytest

from nucliadb.common.cluster.settings import Settings
from nucliadb.common.cluster.standalone import utils


@pytest.fixture
def cluster_settings():
    settings = Settings()
    with patch(
        "nucliadb.common.cluster.standalone.utils.cluster_settings", settings
    ), tempfile.TemporaryDirectory() as tmpdir:
        settings.data_path = tmpdir
        yield settings


def test_get_standalone_node_id(cluster_settings: Settings):
    assert utils.get_standalone_node_id()
    assert os.path.exists(os.path.join(cluster_settings.data_path, "node.key"))


def test_get_self(cluster_settings: Settings):
    os.makedirs(os.path.join(cluster_settings.data_path, "shards", "1"))

    assert utils.get_self().shard_count == 1


def test_get_self_k8s_host(cluster_settings: Settings, monkeypatch):
    monkeypatch.setenv("NUCLIADB_SERVICE_HOST", "host")
    monkeypatch.setenv("HOSTNAME", "nucliadb-0")

    with patch(
        "nucliadb.common.cluster.standalone.grpc_node_binding.NodeWriter"
    ), patch("nucliadb.common.cluster.standalone.grpc_node_binding.NodeReader"):
        # patch because loading settings validates address now
        assert utils.get_self().address == "nucliadb-0.nucliadb"
