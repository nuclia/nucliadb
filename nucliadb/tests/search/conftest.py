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
pytest_plugins = [
    "pytest_docker_fixtures",
    "tests.ndbfixtures.maindb",
    "tests.ndbfixtures.processing",
    "tests.ndbfixtures.standalone",
    "tests.ingest.fixtures",  # should be refactored out
    "tests.ndbfixtures.nidx",
    "tests.ndbfixtures.search",
    "nucliadb_utils.tests.fixtures",
    "nucliadb_utils.tests.gcs",
    "nucliadb_utils.tests.s3",
    "nucliadb_utils.tests.azure",
    "nucliadb_utils.tests.nats",
    "nucliadb_utils.tests.asyncbenchmark",
]
