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
from datetime import datetime

import pytest

from nucliadb.models.internal.processing import PushPayload
from nucliadb.writer.api.v1.resource import needs_reprocess, needs_resource_reindex
from nucliadb_models import File
from nucliadb_models.conversation import Conversation
from nucliadb_models.file import FileField
from nucliadb_models.link import LinkField
from nucliadb_models.metadata import Origin, UserMetadata
from nucliadb_models.text import TextField
from nucliadb_models.writer import UpdateResourcePayload


@pytest.mark.parametrize(
    "item,reindex_needed",
    [
        (UpdateResourcePayload(title="foo"), False),
        (UpdateResourcePayload(origin=Origin(modified=datetime.now())), True),
        (UpdateResourcePayload(origin=Origin(created=datetime.now())), True),
        (UpdateResourcePayload(origin=Origin(metadata={"foo": "bar"})), True),
        (
            UpdateResourcePayload(usermetadata=UserMetadata(classifications=[], relations=[])),
            True,
        ),
        (UpdateResourcePayload(hidden=True), True),
    ],
)
def test_needs_resource_reindex(item, reindex_needed):
    assert needs_resource_reindex(item) is reindex_needed


def push_payload(**kwargs):
    pp = PushPayload(
        kbid="kbid",
        uuid="uuid",
        userid="userid",
        partition=1,
    )
    for key, value in kwargs.items():
        setattr(pp, key, value)
    return pp


@pytest.mark.parametrize(
    "push_payload,reprocess_needed",
    [
        (push_payload(), False),
        (push_payload(genericfield={"foo": TextField(body="foo")}), True),
        (
            push_payload(
                filefield={"foo": FileField(file=File(filename="foo", payload="bar", md5="ba"))}
            ),
            True,
        ),
        (push_payload(conversationfield={"foo": Conversation(messages=[])}), True),
        (push_payload(textfield={"foo": TextField(body="foo")}), True),
        (
            push_payload(linkfield={"foo": LinkField(uri="foo")}),
            True,
        ),
    ],
)
def test_needs_reprocess(push_payload, reprocess_needed):
    assert needs_reprocess(push_payload) is reprocess_needed
