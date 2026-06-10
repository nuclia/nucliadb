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

from pydantic import Field
from pydantic_settings import BaseSettings


class EncryptionSettings(BaseSettings):
    encryption_secret_key: str | None = Field(
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
