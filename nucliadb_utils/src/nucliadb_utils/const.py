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
class PubSubChannels:
    # stream that ingest/node publishes to for information
    RESOURCE_NOTIFY = "notify.{kbid}"


class Streams:
    """
    NOTE: groups can't contain '.', '*' or '>' characters.
    """

    class INGEST:
        """
        Writing resource changes go to this steam/consumer group.
        """

        name = "nucliadb"
        subject = "ndb.consumer.{partition}"
        group = "nucliadb-pull-{partition}"


class Features:
    SKIP_EXTERNAL_INDEX = "nucliadb_skip_external_index"
    LOG_REQUEST_PAYLOADS = "nucliadb_log_request_payloads"
    IGNORE_EXTRACTED_IN_SEARCH = "nucliadb_ignore_extracted_in_search"
    SEMANTIC_GRAPH = "nucliadb_semantic_graph"
    AUDIT_RETRIEVE_AND_AUGMENT = "nucliadb_audit_retrieve_and_augment"


_FliptFeatures: set[str] = set([])
