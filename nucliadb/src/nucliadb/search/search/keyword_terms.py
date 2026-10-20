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
import json
import re
from importlib.resources import files

LOADED_LANGUAGES = ("fr", "it", "es", "en", "ca", "de", "nl", "pt")
STOP_WORDS = frozenset(
    word
    for language in LOADED_LANGUAGES
    for word in json.loads(
        files("nucliadb.search.search")
        .joinpath("stop_words", f"{language}.json")
        .read_text(encoding="utf-8")
    )
)


def keyword_terms(text: str) -> list[str]:
    terms = re.findall(r'-?"[^"]*"|\S+', text)
    if not terms:
        return []
    # Keep the final token for prefix/fuzzy search, matching the Rust query parser.
    return [
        term for term in terms[:-1] if term.startswith(('"', "-")) or term.lower() not in STOP_WORDS
    ] + terms[-1:]
