# Copyright 2026 Bosutech XXI S.L.
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

from nucliadb_models.content_types import guess, valid


def test_guess():
    # Regular types
    assert guess("example.jpg") in ("image/jpeg", "image/pjpeg")
    assert guess("example.pdf") == "application/pdf"

    # Types with special extensions (case handling via OS)
    # Different OS/Python versions might return differing cases, but it should be standard
    res_xlsm = guess("test.xlsm")
    assert res_xlsm is not None and res_xlsm.lower() == "application/vnd.ms-excel.sheet.macroenabled.12"

    # No extension or unknown extension
    assert guess("example") is None
    assert guess("test.madeup") is None


def test_valid():
    # Standard valid MIME types
    assert valid("image/jpeg") is True
    assert valid("application/json") is True
    assert valid("text/html") is True

    # Case-insensitivity support for the Python bug fallback and extra types
    assert valid("application/vnd.ms-excel.sheet.macroEnabled.12") is True
    assert valid("application/vnd.ms-excel.sheet.macroenabled.12") is True
    assert valid("application/VND.MS-EXCEL.SHEET.MACROENABLED.12") is True

    # Custom EXTRA types
    assert valid("video/YouTube") is True
    assert valid("video/youtube") is True

    # Unknown suffixes but known base
    # (e.g. valid checks endswith features directly like +aitable)
    assert valid("application/pdf+aitable") is True
    assert valid("text/plain+blankline") is True

    # Invalid MIME types
    assert valid("invalid") is False
    assert valid("some/made-up-type") is False
