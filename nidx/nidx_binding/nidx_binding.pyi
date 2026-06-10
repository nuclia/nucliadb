# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
