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
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb_telemetry import metrics

processor_observer = metrics.Observer(
    "nucliadb_ingest_processor",
    labels={"type": ""},
    error_mappings={"kb_conflict": KnowledgeBoxConflict},
)


index_message_observer = metrics.Observer(
    "index_message_builder",
    labels={"type": ""},
)

brain_observer = metrics.Observer(
    "brain",
    labels={"type": ""},
)
