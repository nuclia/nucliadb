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

    class INGEST_PROCESSED_V2:
        """
        Pointer messages to resources that have been processed by learning need to be
        written to the database and then Indexed. The resources are actually stored in
        the storage layer.
        """

        name = "nucliadb"
        subject = "ndb.consumer.processed-v2"
        group = "nucliadb-processed-v2"

    class INDEX:
        """
        Indexing resources on the IndexNode
        """

        name = "node"
        subject = "node.{node}"
        group = "node-{node}"


class Features:
    WAIT_FOR_INDEX = "nucliadb_wait_for_resource_index"
    DEFAULT_MIN_SCORE = "nucliadb_default_min_score"
    ROLLOVER_SHARDS = "nuclaidb_rollover_shards"
    INGEST_PROCESSED_V2 = "nucliadb_ingest_processed_v2"
