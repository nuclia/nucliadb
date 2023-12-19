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
import base64
import binascii
import re

from nucliadb.writer.tus.exceptions import InvalidTUSMetadata


def to_str(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return value


match_ascii = re.compile(r"^[ -~]+$")
match_b64 = re.compile(r"[^-A-Za-z0-9+/=]|=[^=]|={3,}$")


def parse_tus_metadata(header: str) -> dict:
    """
    https://tus.io/protocols/resumable-upload.html#upload-metadata
    """
    metadata = {}

    # Be kind, ignore trailing commas
    for pair in header.rstrip(",").split(","):
        try:
            match = re.match(r"^([^ ]+)($| $| [^ ]+$)", pair)
            if match is not None:
                key, value = match.groups()
            else:
                raise AttributeError()
        except (ValueError, TypeError, AttributeError):
            raise InvalidTUSMetadata(f"Could not parse a key value pair in {pair}")

        # Set to None if no value present, regex have already validated
        # the right format for a valueless metadata
        value = value.strip()
        if not match_ascii.match(key):
            raise InvalidTUSMetadata(f"{key} is not a valid ASCII key")
        if value:
            try:
                value = base64.b64decode(value)
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
            except binascii.Error:
                raise InvalidTUSMetadata(f"{key} value is not a valid base64 string")
            except UnicodeDecodeError:
                raise InvalidTUSMetadata(f"{key} value is not a valid ascii string")

        if key in metadata:
            raise InvalidTUSMetadata(f"{key} is repeated")

        metadata[key] = value
    return metadata
