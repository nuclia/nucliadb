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
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from nucliadb_models import CloudLink, File

# Shared classes
# - Nothing to see here

# Visualization classes (Those used on reader endpoints)


class FieldFile(BaseModel):
    added: Optional[datetime] = None
    file: Optional[CloudLink] = None
    language: Optional[str] = None
    password: Optional[str] = None
    external: bool = False


# Creation and update classes (Those used on writer endpoints)


class FileField(BaseModel):
    language: Optional[str] = None
    password: Optional[str] = None
    file: File


# Processing classes (Those used to sent to push endpoints)
# - No class used, all files are send to push as opaque string tokens
#   set directly in the files dict on PushPayload
