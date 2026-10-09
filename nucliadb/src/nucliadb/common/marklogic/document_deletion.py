"""Batched document deletions by exact URI or by directory, applied as one Optic remove."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field


@dataclass
class DocumentDeletion:
    """Documents to delete, built anywhere and applied with `marklogic_documents.delete_documents`.

    `directory()` deletes recursively under a URI prefix, which must be a directory (end with `/`)
    so `/resources/a/` can't also match `/resources/ab/`. It can be restricted to `collections`.
    """

    _uris: set[str] = field(default_factory=set, init=False)
    _directories: dict[str, frozenset[str]] = field(default_factory=dict, init=False)

    def uri(self, *uris: str) -> DocumentDeletion:
        for uri in uris:
            if not uri.startswith("/") or uri.endswith("/"):
                raise ValueError(f"Document URI must start with '/' and not end with it: {uri!r}")
            self._uris.add(uri)
        return self

    def directory(self, directory: str, collections: Iterable[str] = ()) -> DocumentDeletion:
        if not directory.startswith("/") or not directory.endswith("/"):
            raise ValueError(f"Directory must start and end with '/': {directory!r}")
        collections = frozenset(collections)
        previous = self._directories.get(directory)
        # Merging with an unrestricted deletion of the same directory keeps it unrestricted.
        if previous is not None and (not previous or not collections):
            collections = frozenset()
        elif previous is not None:
            collections |= previous
        self._directories[directory] = collections
        return self

    def __bool__(self) -> bool:
        return bool(self._uris or self._directories)

    @property
    def uris(self) -> frozenset[str]:
        return frozenset(self._uris)

    @property
    def directories(self) -> dict[str, frozenset[str]]:
        return dict(self._directories)

    def to_optic(self) -> str:
        queries = []
        if self._uris:
            queries.append(f"cts.documentQuery({json.dumps(sorted(self._uris))})")
        for directory, collections in sorted(self._directories.items()):
            query = f"cts.directoryQuery({json.dumps(directory)}, 'infinity')"
            if collections:
                query = (
                    f"cts.andQuery([{query}, cts.collectionQuery({json.dumps(sorted(collections))})])"
                )
            queries.append(query)
        if not queries:
            raise ValueError("Nothing to delete")
        query = queries[0] if len(queries) == 1 else f"cts.orQuery([{', '.join(queries)}])"
        return f"op.fromDocUris({query}).remove()"
