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
import hashlib
from base64 import b64encode
from os.path import dirname


def load_file_as_FileB64_payload(f: str, content_type: str) -> dict:
    file_location = f"{dirname(__file__)}/{f}"
    filename = f.split("/")[-1]
    data = b64encode(open(file_location, "rb").read())

    return {
        "filename": filename,
        "content_type": content_type,
        "payload": data.decode("utf-8"),
        "md5": hashlib.md5(data).hexdigest(),
    }
