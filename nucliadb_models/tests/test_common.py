# Copyright 2025 Bosutech XXI S.L.
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
#

import pytest
from pydantic_core import ValidationError

from nucliadb_models import common


def test_file_model_validator():
    common.File(uri="asdf")

    # filename is mandatory
    with pytest.raises(ValidationError):
        common.File(payload="base64content")

    # payload is mandatory
    with pytest.raises(ValidationError):
        common.File(filename="myfile")


def test_cloud_link_serialization():
    link = common.CloudLink(
        uri="/kbs/1b372a0b-5ec1-4a9a-ba71-76147ca6b265/r/2c4f1db44735409eb881d2b308bce18b/f/f/text/extracted",
    )
    assert (
        link.model_dump()["uri"]
        == "/kb/1b372a0b-5ec1-4a9a-ba71-76147ca6b265/resource/2c4f1db44735409eb881d2b308bce18b/file/text/download/field/extracted"
    )
