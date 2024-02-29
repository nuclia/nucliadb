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

import botocore  # type: ignore
import pytest

from nucliadb_utils.storages import s3
from nucliadb_utils.storages.exceptions import UnparsableResponse


@pytest.mark.parametrize(
    "code,expected",
    [
        ("200", 200),
        ("404", 404),
        ("409", 409),
        ("NoSuchBucket", 404),
        ("NoSuchKey", 404),
        ("BucketNotEmpty", 409),
    ],
)
def test_parse_status_code(response_generator, code: str, expected: int):
    resp = response_generator(code)
    assert s3.parse_status_code(resp) == expected


def test_unparsable_status_code(response_generator):
    resp = response_generator("Unknown")
    with pytest.raises(UnparsableResponse):
        s3.parse_status_code(resp)


@pytest.fixture
def response_generator():
    class TestError(botocore.exceptions.ClientError):
        def __init__(self, error_code: str):
            response = {
                "Error": {
                    "Code": error_code,
                }
            }
            super().__init__(error_response=response, operation_name="TEST")  # type: ignore

    yield TestError
