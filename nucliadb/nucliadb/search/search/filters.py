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
from nucliadb_models.labels import translate_alias_to_system_label
from nucliadb_telemetry.metrics import Counter

from .exceptions import InvalidQueryError

ENTITY_PREFIX = "/e/"
CLASSIFICATION_LABEL_PREFIX = "/l/"


def translate_label_filters(filters: list[str]) -> list[str]:
    """
    Translate friendly filter names to the shortened filter names.
    """
    output = []
    for fltr in filters:
        if len(fltr) == 0:
            raise InvalidQueryError("filters", f"Invalid empty label")
        if fltr[0] != "/":
            raise InvalidQueryError(
                "filters", f"Invalid label. It must start with a `/`: {fltr}"
            )

        output.append(translate_alias_to_system_label(fltr))
    return output


def record_filters_counter(filters: list[str], counter: Counter) -> None:
    counter.inc({"type": "filters"})
    filters.sort()
    entity_found = False
    label_found = False
    for fltr in filters:
        if entity_found and label_found:
            break
        if not entity_found and fltr.startswith(ENTITY_PREFIX):
            entity_found = True
            counter.inc({"type": "filters_entities"})
        elif not label_found and fltr.startswith(CLASSIFICATION_LABEL_PREFIX):
            label_found = True
            counter.inc({"type": "filters_labels"})
