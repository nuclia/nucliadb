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

BASE_LABELS: dict[str, list[str]] = {
    "t": [],  # doc tags
    "l": [],  # doc labels
    "n": [],  # type of element: i (Icon). s (Processing Status)
    "e": [],  # entities e/type/entityid
    "s": [],  # languages p (Principal) s (ALL)
    "u": [],  # contributors s (Source) o (Origin)
    "f": [],  # field keyword field (field/keyword)
    "fg": [],  # field keyword (keywords) flat
    "m": [],  # origin metadata in the form of (key/value). Max key/value size is 255
}


def flatten_resource_labels(tags_dict: dict[str, list[str]]) -> list[str]:
    flat_tags = []
    for key, values in tags_dict.items():
        for value in values:
            flat_tags.append(f"/{key}/{value}")
    return flat_tags
