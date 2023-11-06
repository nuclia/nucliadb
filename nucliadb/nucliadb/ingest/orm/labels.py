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
from typing import Optional, Callable
from nucliadb_protos import resources_pb2
from nucliadb_models.metadata import ResourceProcessingStatus
from dataclasses import dataclass

BASE_LABELS: dict[str, list[str]] = {
    "t": [],  # doc tags
    "l": [],  # doc labels
    "n": [],  # type of element: i (Icon). s (Processing Status)
    "e": [],  # entities e/type/entityid
    "s": [],  # languages p (Principal) s (ALL)
    "u": [],  # contributors s (Source) o (Origin)
    "f": [],  # field keyword field (field/keyword)
    "fg": [],  # field keyword (keywords) flat
}

METADATA_STATUS_PB_TYPE_TO_NAME_MAP = {
    resources_pb2.Metadata.Status.ERROR: ResourceProcessingStatus.ERROR.name,
    resources_pb2.Metadata.Status.PROCESSED: ResourceProcessingStatus.PROCESSED.name,
    resources_pb2.Metadata.Status.PENDING: ResourceProcessingStatus.PENDING.name,
    resources_pb2.Metadata.Status.BLOCKED: ResourceProcessingStatus.BLOCKED.name,
    resources_pb2.Metadata.Status.EXPIRED: ResourceProcessingStatus.EXPIRED.name,
}


@dataclass
class LabelIndexProvider:
    short: str
    long: str
    func: Callable[[resources_pb2.Basic, Optional[resources_pb2.Origin]], list[str]]


@dataclass
class LabelType:
    short: str
    long: str


_label_types = [
    LabelType(short="f", long="field"),
    LabelType(short="fg", long="fieldvalue"),
]
_registered_label_index_providers: list[LabelIndexProvider] = []


def get_label_index_providers() -> list[LabelIndexProvider]:
    return _registered_label_index_providers


def register_label_index_provider(*, short: str, long: str):
    def decorator(func):
        _registered_label_index_providers.append(
            LabelIndexProvider(short=short, long=long, func=func)
        )
        _label_types.append(LabelType(short=short, long=long))

        return func

    return decorator


@register_label_index_provider(short="t", long="tags")
def tags_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if origin is None:
        return []
    return origin.tags


@register_label_index_provider(short="u/s", long="origin/source_id")
def origin_source_id_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if origin is None or not origin.source_id:
        return []
    return [origin.source_id]


@register_label_index_provider(short="u/o", long="origin/contributors")
def origin_contributors_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if origin is None or not origin.colaborators:
        return []

    return list(origin.colaborators)


@register_label_index_provider(short="n/i", long="icon")
def icon_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if not basic.icon:
        return []
    return [basic.icon]


@register_label_index_provider(short="n/s", long="processing-status")
def processing_status_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if not basic.metadata.useful:
        return ["EMPTY"]
    return [METADATA_STATUS_PB_TYPE_TO_NAME_MAP[basic.metadata.status]]


@register_label_index_provider(short="s/p", long="language/primary")
def primary_language_language_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if not basic.metadata.language:
        return []
    return [basic.metadata.language]


@register_label_index_provider(short="s/s", long="language/secondary")
def secondar_language_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if not basic.metadata.languages:
        return []
    return list(basic.metadata.languages)


@register_label_index_provider(short="l", long="labelsets")
def labelset_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    """
    Applied labels to a resource obviously also are applied as labels to
    an indexed resource
    """
    if not basic.usermetadata.classifications:
        return []
    return [
        f"{classification.labelset}/{classification.label}"
        for classification in basic.usermetadata.classifications
    ]


def _clip_label(label: str) -> str:
    return label[:255]


@register_label_index_provider(short="m", long="metadata")
def origin_metadata_label_index_provider(
    basic: resources_pb2.Basic, origin: Optional[resources_pb2.Origin]
) -> list[str]:
    if origin is not None and origin.metadata:
        return []
    return [
        f"{_clip_label(key)}/{_clip_label(value)}"
        for key, value in origin.metadata.items()
    ]


def flatten_resource_labels(labels_dict: dict[str, list[str]]):
    flattened_labels = []
    for key, values in labels_dict.items():
        for value in values:
            flattened_labels.append(f"/{key}/{value}")
    return flattened_labels
