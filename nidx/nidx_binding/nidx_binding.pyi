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

class NidxBinding:
    """Embedded nidx instance that runs an indexer, searcher, scheduler and
    worker in-process.  Intended for use in standalone / testing deployments
    where a full nidx service is not available.
    """

    searcher_port: int
    """TCP port on localhost where the searcher gRPC API is listening."""

    api_port: int
    """TCP port on localhost where the management/API gRPC server is listening."""

    def __init__(self, settings: dict[str, str]) -> None:
        """Start an embedded nidx instance.

        Args:
            settings: Configuration key/value pairs that mirror the nidx
                environment-variable schema (e.g.
                ``{"METADATA__DATABASE_URL": "postgresql://…",
                   "STORAGE__OBJECT_STORE": "file", …}``).
                ``INDEXER__NATS_SERVER`` is always overridden to an empty
                string so that the in-process indexer is used instead of NATS.

        Raises:
            Exception: If nidx fails to start (database unreachable, bad
                configuration, etc.).
        """
        ...

    def index(self, bytes: bytes) -> int:
        """Index a single message.

        Args:
            bytes: A protobuf-serialised ``IndexMessage`` (see
                ``nidx_protos.nodewriter_pb2.IndexMessage``).

        Returns:
            The sequence number assigned to the indexed message.

        Raises:
            Exception: If indexing fails.  The internal sequence counter is
                still incremented so that subsequent calls are not affected.
        """
        ...

    def wait_for_sync(self) -> None:
        """Block until the searcher has completed a full synchronisation cycle.

        This method is primarily intended for use in tests where you need to
        ensure that a previously indexed message is visible to search queries
        before proceeding.
        """
        ...
