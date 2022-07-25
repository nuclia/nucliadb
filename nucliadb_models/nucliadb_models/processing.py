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
from enum import Enum
from typing import TYPE_CHECKING, Dict, Optional

from nucliadb_protos.resources_pb2 import CloudFile
from pydantic import BaseModel, Field

import nucliadb_models as models

if TYPE_CHECKING:
    SourceValue = CloudFile.Source.V
else:
    SourceValue = int


class Source(SourceValue, Enum):  # type: ignore
    HTTP = 0
    INGEST = 1


class PushProcessingOptions(BaseModel):
    # Enable ML processing
    ml_text: Optional[bool] = True


class PushPayload(BaseModel):
    # There are multiple options of payload
    uuid: str
    slug: Optional[str] = None
    kbid: str
    source: Optional[Source] = None
    userid: str

    genericfield: Dict[str, models.Text] = {}

    # New File
    filefield: Dict[str, str] = {}

    # New Link
    linkfield: Dict[str, models.LinkUpload] = {}

    # Diff on Text Field
    textfield: Dict[str, models.Text] = {}

    # Diff on a Layout Field
    layoutfield: Dict[str, models.LayoutDiff] = {}

    # New conversations to process
    conversationfield: Dict[str, models.PushConversation] = {}

    # Only internal
    partition: int

    # List of available processing options (with default values)
    processing_options: Optional[PushProcessingOptions] = Field(
        default_factory=PushProcessingOptions
    )


class PushResponse(BaseModel):
    seqid: Optional[int] = None
