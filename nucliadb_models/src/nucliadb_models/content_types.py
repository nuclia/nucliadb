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


import mimetypes
from typing import Optional

GENERIC_MIME_TYPE = "application/generic"

NUCLIA_CUSTOM_CONTENT_TYPES = {
    GENERIC_MIME_TYPE,
    "application/stf-link",
    "application/conversation",
}

EXTRA_VALID_CONTENT_TYPES = {
    "application/font-woff",
    "application/mp4",
    "application/toml",
    "application/vnd.jgraph.mxfile",
    "application/vnd.ms-excel.sheet.macroenabled.12",
    "application/vnd.ms-outlook",
    "application/vnd.ms-word.document.macroenabled.12",
    "application/vnd.rar",
    "application/x-aportisdoc",
    "application/x-archive",
    "application/x-git",
    "application/x-gzip",
    "application/x-iwork-pages-sffpages",
    "application/x-mach-binary",
    "application/x-mobipocket-ebook",
    "application/x-ms-shortcut",
    "application/x-msdownload",
    "application/x-ndjson",
    "application/x-openscad",
    "application/x-sql",
    "application/x-zip-compressed",
    "application/zstd",
    "audio/vnd.dlna.adts",
    "audio/wav",
    "audio/x-m4a",
    "model/stl",
    "multipart/form-data",
    "text/jsx",
    "text/markdown",
    "text/mdx",
    "text/rtf",
    "text/x-c++",
    "text/x-java-source",
    "text/x-log",
    "text/x-python-script",
    "text/yaml",
    "video/x-m4v",
    "video/YouTube",
} | NUCLIA_CUSTOM_CONTENT_TYPES


def guess(filename: str) -> Optional[str]:
    """
    Guess the content type of a file based on its filename.
    Returns None if the content type could not be guessed.
    >>> guess("example.jpg")
    'image/jpeg'
    >>> guess("example")
    None
    """
    guessed, _ = mimetypes.guess_type(filename, strict=False)
    return guessed


def valid(content_type: str) -> bool:
    """
    Check if a content type is valid.
    >>> valid("image/jpeg")
    True
    >>> valid("invalid")
    False
    """
    # The AI tables feature has been implemented via a custom mimetype suffix.
    # Keep this until we have a better solution to handle this.
    content_type = content_type.split("+aitable")[0]
    in_standard = mimetypes.guess_extension(content_type, strict=False) is not None
    return in_standard or content_type in EXTRA_VALID_CONTENT_TYPES
