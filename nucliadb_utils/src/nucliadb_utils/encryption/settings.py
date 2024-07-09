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
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class EncryptionSettings(BaseSettings):
    encryption_secret_key: Optional[str] = Field(
        default=None,
        title="Encryption Secret Key",
        description="""Secret key used for encryption and decryption of sensitive data in the database.
The key must be a 32-byte string, base64 encoded. You can generate a new key with the following unix command:
head -c 32 /dev/urandom | base64
""",
        examples=[
            "6TGjSkvX6qkFOo/BJKl5OY1fwJoWnMaSVI7VOJjU07Y=",
        ],
    )


settings = EncryptionSettings()
