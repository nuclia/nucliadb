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
import base64
from uuid import uuid4

import pytest


@pytest.fixture()
def encryption_settings():
    from nucliadb_utils.encryption.settings import settings

    # Get a 32-bytes random string
    secret_key = uuid4().hex[:32]
    # Encode the secret key in base64
    b64_encoded_secret_key = base64.b64encode(secret_key.encode()).decode()
    settings.encryption_secret_key = b64_encoded_secret_key

    yield settings


async def test_endecryptor(encryption_settings):
    import nacl.utils

    from nucliadb_utils.encryption import EndecryptorUtility

    endecryptor = EndecryptorUtility(key=base64.b64decode(encryption_settings.encryption_secret_key))
    message = b"This is a test message"
    encrypted = await endecryptor.encrypt(message)
    assert isinstance(encrypted, nacl.utils.EncryptedMessage)
    decrypted = await endecryptor.decrypt(encrypted)
    assert decrypted == message

    text = "This is some other text"
    encrypted_text = await endecryptor.encrypt_text(text)
    assert isinstance(encrypted_text, str)
    decrypted_text = await endecryptor.decrypt_text(encrypted_text)
    assert decrypted_text == text
