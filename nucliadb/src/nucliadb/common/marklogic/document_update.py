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
"""Partial updates of top-level JSON document properties, applied as an Optic patch."""

from __future__ import annotations

import builtins
import json
import re
from dataclasses import dataclass, field
from typing import Any

_PROPERTY_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")


@dataclass
class DocumentUpdate:
    """Pending changes to the top-level properties of one JSON document.

    Build it anywhere (`update["basic"] = ...`, `update.set(slug=...)`, `del update["extra"]`) and
    apply it with `marklogic_documents.update_document`. Nested values are always replaced whole.
    If the document does not exist and `upsert` is set, it is created from `defaults` plus the
    changes.
    """

    uri: str
    collection: str
    defaults: dict[str, Any] = field(default_factory=dict)
    upsert: bool = True
    _set: dict[str, Any] = field(default_factory=dict, init=False)
    _removed: builtins.set[str] = field(default_factory=builtins.set, init=False)

    @staticmethod
    def _validate(name: str) -> str:
        if not _PROPERTY_NAME.match(name):
            raise ValueError(f"Invalid document property name: {name!r}")
        return name

    def set(self, **values: Any) -> DocumentUpdate:
        for name, value in values.items():
            self[name] = value
        return self

    def remove(self, *names: str) -> DocumentUpdate:
        for name in names:
            del self[name]
        return self

    def __setitem__(self, name: str, value: Any) -> None:
        self._set[self._validate(name)] = value
        self._removed.discard(name)

    def __delitem__(self, name: str) -> None:
        self._removed.add(self._validate(name))
        self._set.pop(name, None)

    def __bool__(self) -> bool:
        return bool(self._set or self._removed)

    @property
    def changes(self) -> dict[str, Any]:
        return dict(self._set)

    @property
    def removed(self) -> frozenset[str]:
        return frozenset(self._removed)

    def new_document(self) -> dict[str, Any]:
        content = {**self.defaults, **self._set}
        for name in self._removed:
            content.pop(name, None)
        return content

    def to_optic(self) -> str:
        return _patch_plan(
            f"op.fromLiterals([{{uri: {json.dumps(self.uri)}}}])", self._removed, self._set
        )


def patch_matching_optic(query: str, values: dict[str, Any]) -> str:
    """Optic plan replacing top-level properties of every document matching the cts `query`."""
    for name in values:
        DocumentUpdate._validate(name)
    return _patch_plan(f"op.fromDocUris({query})", builtins.set(), values)


def _patch_plan(source: str, removed: builtins.set[str], values: dict[str, Any]) -> str:
    patches = ""
    for name in sorted(removed):
        path = json.dumps(f'node("{name}")')
        patches += f".remove({path})"
    for name, value in values.items():
        path = json.dumps(f'node("{name}")')
        patches += f".remove({path}).insertNamedChild('.', {json.dumps(name)}, {json.dumps(value)})"
    return (
        f"{source}"
        ".joinDoc(op.col('doc'), op.col('uri'))"
        f".patch(op.col('doc'), op.patchBuilder('/'){patches})"
        ".write().select(['uri'])"
    )
