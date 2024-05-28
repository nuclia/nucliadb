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
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.writer.resource.basic import compute_title, parse_icon_on_create
from nucliadb_models.common import File
from nucliadb_models.file import FileField
from nucliadb_models.link import LinkField
from nucliadb_models.text import TextField, TextFormat
from nucliadb_models.writer import CreateResourcePayload


def get_resource_payload(link_uri=None, filename=None, slug=None, text=None, icon=None):
    payload = CreateResourcePayload()
    if link_uri:
        payload.links["mylink"] = LinkField(uri=link_uri)
    if filename is not None:
        payload.files["myfile"] = FileField(file=File(filename=filename, payload=""))
    if slug:
        payload.slug = slug
    if text:
        payload.texts["text"] = TextField(body=text, format=TextFormat.PLAIN)
    if icon:
        payload.icon = icon
    return payload


@pytest.mark.parametrize(
    "payload,resource_uuid,expected_title",
    [
        (
            get_resource_payload(
                link_uri="https://foo.com", filename="myfile.pdf", slug="myslug"
            ),
            "ruuid",
            "https://foo.com",
        ),
        (
            get_resource_payload(filename="myfile.pdf", slug="myslug"),
            "ruuid",
            "myfile.pdf",
        ),
        (get_resource_payload(filename=""), "ruuid", "ruuid"),
        (get_resource_payload(slug="myslug"), "ruuid", "myslug"),
        (get_resource_payload(), "ruuid", "ruuid"),
    ],
)
def test_compute_title(payload, resource_uuid, expected_title):
    assert compute_title(payload, resource_uuid) == expected_title


@pytest.mark.parametrize(
    "payload,expected_icon",
    [
        (get_resource_payload(icon="image/png", text="footext"), "image/png"),
        (get_resource_payload(text="sometext"), "text/plain"),
        (get_resource_payload(), "application/generic"),
    ],
)
def test_parse_icon_on_create(payload, expected_icon):
    message = BrokerMessage()
    parse_icon_on_create(message, payload)
    assert message.basic.icon == expected_icon
