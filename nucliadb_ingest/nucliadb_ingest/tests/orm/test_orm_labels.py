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
from nucliadb_protos.knowledgebox_pb2 import Label as PBLabel
from nucliadb_protos.knowledgebox_pb2 import Labels as PBLabels

from nucliadb_ingest.orm.labels import Labels


@pytest.mark.asyncio
async def test_create_label_orm(txn, gcs_storage, fake_node, knowledgebox):
    lls = Labels(txn, knowledgebox)
    pbls = PBLabels()
    ppl = PBLabel(title="Label 1", related=None, text=None, uri=None)
    pbls.labelset["labelset1"].title = "Labelset 1"
    pbls.labelset["labelset1"].color = "#333"
    pbls.labelset["labelset1"].labels.append(ppl)
    await lls.set(pbls)


@pytest.mark.asyncio
async def test_get_label_orm(txn, gcs_storage, fake_node, knowledgebox):
    lls = Labels(txn, knowledgebox)
    pbls = PBLabels()
    ppl = PBLabel(title="Label 1", related=None, text=None, uri=None)
    pbls.labelset["labelset1"].title = "Labelset 1"
    pbls.labelset["labelset1"].color = "#333"
    pbls.labelset["labelset1"].labels.append(ppl)
    await lls.set(pbls)
    pbls2 = await lls.get()
    assert pbls2.labelset["labelset1"].title == "Labelset 1"
