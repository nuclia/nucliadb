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
from nucliadb_protos.train_pb2 import Type

import pydantic_argparse

from nucliadb_dataset import DatasetType


DATASET_TYPE_MAPPING = {
    DatasetType.FIELD_CLASSIFICATION: Type.FIELD_CLASSIFICATION,
    DatasetType.PARAGRAPH_CLASSIFICATION: Type.PARAGRAPH_CLASSIFICATION,
    DatasetType.SENTENCE_CLASSIFICATION: Type.SENTENCE_CLASSIFICATION,
    DatasetType.TOKEN_CLASSIFICATION: Type.TOKEN_CLASSIFICATION,
}


def run():
    from nucliadb_dataset.settings import RunningSettings

    parser = pydantic_argparse.ArgumentParser(
        model=RunningSettings,
        prog="NucliaDB Datasets",
        description="Generate Arrow files from NucliaDB KBs",
    )
    nucliadb_args = parser.parse_typed_args()
