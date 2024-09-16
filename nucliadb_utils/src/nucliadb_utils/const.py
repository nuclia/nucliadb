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
class PubSubChannels:
    # stream that ingest/node publishes to for information
    RESOURCE_NOTIFY = "notify.{kbid}"


class Streams:
    class INGEST:
        """
        Writing resource changes go to this steam/consumer group.
        """

        name = "nucliadb"
        subject = "ndb.consumer.{partition}"
        group = "nucliadb-{partition}"

    class INGEST_PROCESSED:
        """
        Resources that have been processed by learning need to be
        written to the database and then Indexed.
        """

        name = "nucliadb"
        subject = "ndb.consumer.processed"
        group = "nucliadb-processed"

    class INDEX:
        """
        Indexing resources on the IndexNode
        """

        name = "node"
        subject = "node.{node}"
        group = "node-{node}"

    class KB_EXPORTS:
        """
        Exporting kbs
        """

        name = "ndb-exports"
        subject = "ndb-exports"
        group = "ndb-exports"

    class KB_IMPORTS:
        """
        Importing kbs
        """

        name = "ndb-imports"
        subject = "ndb-imports"
        group = "ndb-imports"


class Features:
    WAIT_FOR_INDEX = "nucliadb_wait_for_resource_index"
    READ_REPLICA_SEARCHES = "nucliadb_read_replica_searches"
    VERSIONED_PRIVATE_PREDICT = "nucliadb_versioned_private_predict"
    REBALANCE_KB = "nucliadb_rebalance_kb"
    SKIP_EXTERNAL_INDEX = "nucliadb_skip_external_index"
    NATS_SYNC_ACK = "nucliadb_nats_sync_ack"
    LOG_REQUEST_PAYLOADS = "nucliadb_log_request_payloads"
