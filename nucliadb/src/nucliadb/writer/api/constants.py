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
from fastapi.params import Header

X_SKIP_STORE = Header(
    description="If set to true, file fields will not be saved in the blob storage. They will only be sent to process.",
)
X_NUCLIADB_USER = Header("")
X_FILE_PASSWORD = Header(
    None,
    description="If a file is password protected, the password must be provided here for the file to be processed",
)
X_EXTRACT_STRATEGY = Header(
    description="Extract strategy to use when uploading a file. If not provided, the default strategy will be used.",
)
