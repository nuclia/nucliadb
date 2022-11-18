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

from nucliadb_client.client import NucliaDBClient
from nucliadb_models.resource import KnowledgeBoxConfig


def test_knowledgebox_creation(nucliadb_client: NucliaDBClient):
    kb = nucliadb_client.create_kb(
        title="My KB", description="Its a new KB", slug="mykb"
    )
    info = kb.get()
    info.slug == "mykb"
    if info.config is None:
        info.config = KnowledgeBoxConfig()
    info.config.title == "My KB"
    info.config.description == "Its a new KB"

    assert kb.delete()
    info = kb.get()


def test_knowledgebox_counters(nucliadb_client):
    kb = nucliadb_client.create_kb(
        title="My KB", description="Its a new KB", slug="mykb"
    )
    assert kb.get()
    counters = kb.counters()
    assert counters.resources == 0
    assert counters.paragraphs == 0
    assert counters.sentences == 0


def test_knowledgebox_shards(nucliadb_client):
    kb = nucliadb_client.create_kb(
        title="My KB", description="Its a new KB", slug="mykb"
    )
    assert kb.shards()
