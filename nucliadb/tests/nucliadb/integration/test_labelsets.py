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

import pytest
from httpx import AsyncClient


@pytest.mark.deploy_modes("standalone")
async def test_selection_labelsets(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/labelset/myselections",
        json={
            "title": "My Selections",
            "kind": ["SELECTIONS"],
            "labels": [
                {"title": "title"},
                {"title": "introduction"},
                {"title": "body"},
                {"title": "conclusion"},
            ],
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/labelset/myselections")
    assert resp.status_code == 200
    body = resp.json()
    labels = {label["title"] for label in body["labels"]}
    assert labels == {"title", "introduction", "body", "conclusion"}

    resp = await nucliadb_writer.delete(f"/kb/{kbid}/labelset/myselections")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/labelset/myselections")
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_duplicated_labelsets_not_allowed(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    # Create labelset
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/labelset/myselections",
        json={
            "title": "test_name",
            "kind": ["SELECTIONS"],
            "labels": [
                {"title": "label1"},
            ],
        },
    )
    assert resp.status_code == 200

    # Can edit same labelset
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/labelset/myselections",
        json={
            "title": "My Selections",
            "kind": ["SELECTIONS"],
            "labels": [
                {"title": "label1"},
            ],
        },
    )
    assert resp.status_code == 200

    # Cannot create a new labelset reusing title
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/labelset/my_new_selections",
        json={
            "title": "MY SELECTIONS",
            "kind": ["SELECTIONS"],
            "labels": [
                {"title": "label1"},
            ],
        },
    )
    assert resp.status_code == 422


@pytest.mark.deploy_modes("standalone")
async def test_duplicated_labels_not_allowed(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/labelset/myselections",
        json={
            "title": "My Selections",
            "kind": ["SELECTIONS"],
            "labels": [
                {"title": "label1"},
                {"title": "LABEL1"},
            ],
        },
    )
    assert resp.status_code == 422
