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
from typing import Optional

from nucliadb_protos.knowledgebox_pb2 import Labels, LabelSet

from nucliadb.common.datamanagers.labels import LabelsDataManager
from nucliadb.common.maindb.utils import get_driver
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


async def split_filters_by_label_type(
    kbid, filters: list[str]
) -> tuple[list[str], list[str]]:
    """ """
    labels = []
    paragraph_labels = []
    cache: dict[str, bool] = {}
    try:
        for fltr in filters:
            if len(fltr) == 0 or fltr[0] != "/":
                continue

            if not fltr.startswith(CLASSIFICATION_LABEL_PREFIX):
                labels.append(fltr)
                continue

            # It should have the form /l/labelset/label
            parts = fltr.split("/")
            if len(parts) < 4:
                labels.append(fltr)
                continue

            labelset_id = parts[2]
            if await cached_is_paragraph_labelset_kind(kbid, labelset_id, cache):
                paragraph_labels.append(fltr)
            else:
                labels.append(fltr)

        return labels, paragraph_labels
    finally:
        cache.clear()


async def cached_is_paragraph_labelset_kind(
    kbid, labelset_id: str, cache: dict[str, bool]
) -> bool:
    result = cache.get(labelset_id, None)
    if result is None:
        result = await is_paragraph_labelset_kind(kbid, labelset_id)
        cache[labelset_id] = result
    return result


async def is_paragraph_labelset_kind(kbid, labelset_id: str) -> bool:
    driver = get_driver()
    ldm = LabelsDataManager(driver)
    labels: Labels = await ldm.get_labels(kbid)
    try:
        labelset: Optional[LabelSet] = labels.labelset.get(labelset_id)
        if labelset is None:
            return False
        return LabelSet.LabelSetKind.PARAGRAPHS in labelset.kind
    except KeyError:
        # labelset_id not found
        return False
