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

import random
import time

import pytest
from httpx import AsyncClient


@pytest.mark.deploy_modes("standalone")
async def test_text_blocks_hydration(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    # Create 30 different file fields with large extracted texts
    text_size = 2 * 1024 * 1024  # 1 MB
    large_text = "x" * text_size

    rids = {}
    for i in range(30):
        slug = f"resource-{i}"
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={"slug": slug, "texts": {"text": {"body": large_text}}},
        )
        resp.raise_for_status()
        rids[slug] = resp.json()["uuid"]

    # Generate 100 random text block ids
    text_blocks = []
    for i in range(100):
        slug = f"resource-{random.randint(0, 29)}"
        size = random.randint(10, 1000)
        start = random.randint(0, text_size // 4)
        end = start + size
        text_block_id = f"{rids[slug]}/t/text/{start}-{end}"
        text_blocks.append(text_block_id)

#    for i in [1, 5, 10, 20, 30, 50, 70, 100]:
    for i in [1, ]:
        random.shuffle(text_blocks)
        tbids = text_blocks[:i]
        start_time: float = time.time()
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/hydrate/text_blocks",
            json={"text_blocks": tbids},
        )
        resp.raise_for_status()
        print(f"Hydrated {i} text blocks in {time.time() - start_time:.2f} seconds")
        data = resp.json()
        assert data["errors"] == {}
        assert len(data["text_blocks"]) == len(tbids)
