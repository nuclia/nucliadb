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
class InvalidCloudFile(Exception):
    pass


class CouldNotCreateBucket(Exception):
    pass


class InvalidOffset(Exception):
    def __init__(self, range_header, offset):
        self.range_header = range_header
        self.offset = offset
        super().__init__(
            "nucliadb and google cloud storage "
            "offsets do not match. Google: "
            f"{range_header}, TUS(offset): {offset}"
        )


class ResumableUploadGone(Exception):
    def __init__(self, text: str):
        self.text = text
        super().__init__("Resumable upload is no longer available " "Google: \n " f"{text}")


class CouldNotCopyNotFound(Exception):
    def __init__(
        self,
        origin_uri: str,
        origin_bucket_name: str,
        destination_uri: str,
        destination_bucket_name: str,
        text: str,
    ):
        self.origin_uri = origin_uri
        self.origin_bucket_name = origin_bucket_name
        self.destination_uri = destination_uri
        self.destination_bucket_name = (destination_bucket_name,)
        self.text = text
        super().__init__(
            f"Could not copy file {self.origin_bucket_name}:{self.origin_uri}"
            f"To {self.destination_bucket_name}:{self.destination_uri}"
            "Google: \n "
            f"{text}"
        )


class IndexDataNotFound(Exception):
    """
    Raised when the index data is not found in storage
    """


class UnparsableResponse(Exception):
    """
    Raised when trying to parse a response from a storage API and it's not
    possible
    """


class ObjectNotFoundError(Exception):
    """
    Raised when the object is not found in storage
    """
