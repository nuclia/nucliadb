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
import logging
import os

logger = logging.getLogger("nucliadb_ingest")


def assign_partitions(settings):
    # partitions start from 1, instead of 0
    all_partitions = [str(part + 1) for part in range(settings.nuclia_partitions)]

    # get replica number and total replicas from environment
    logger.info(f"PARTITIONS: Total Replicas = {settings.total_replicas}")
    logger.info(f"PARTITIONS: Replica Number = {settings.replica_number}")

    # calculate assigned partitions based on total replicas and own replica number
    partitions_list = all_partitions[settings.replica_number :: settings.total_replicas]

    # update settings AND Environment Varialbe (for this process and its childs) with partition list
    settings.partitions = partitions_list
    os.environ["PARTITIONS"] = str(partitions_list)
    logger.info(
        f"PARTITIONS: Assigned Partitions (in settings) = {settings.partitions}"
    )
    logger.info(
        f"PARTITIONS: Assigned Partitions (in environment) = {os.environ['PARTITIONS']}"
    )
