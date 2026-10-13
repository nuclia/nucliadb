"""Partial updates of top-level JSON document properties, applied as an Optic patch."""

from __future__ import annotations

import builtins
import json
import re
from dataclasses import dataclass, field
from typing import Any

_PROPERTY_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")


def _literal(value: Any) -> str:
    return json.dumps(value, allow_nan=False)


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
        """Optic Update DSL patching the document in place; yields no rows if it doesn't exist."""
        patches = [f".remove({_literal(name)})" for name in sorted(self._removed)]
        # insertNamedChild fails on an existing key, so drop it first to get replace-or-insert.
        patches.extend(
            f".remove({_literal(name)}).insertNamedChild('/', {_literal(name)}, {_literal(value)})"
            for name, value in self._set.items()
        )
        return (
            f"op.fromDocUris(cts.documentQuery([{_literal(self.uri)}]))"
            ".joinDocCols(null, op.fragmentIdCol('fragmentId'))"
            f".patch(op.col('doc'), op.patchBuilder('/'){''.join(patches)})"
            ".write()"
        )
