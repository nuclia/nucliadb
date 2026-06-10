# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
class InvalidCloudFile(Exception):
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
        super().__init__(f"Resumable upload is no longer available Google: \n {text}")


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
        self.destination_bucket_name = destination_bucket_name
        self.text = text
        super().__init__(
            f"Could not copy file {self.origin_bucket_name}:{self.origin_uri} "
            f"To {self.destination_bucket_name}:{self.destination_uri} "
            "Storage says: \n "
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
