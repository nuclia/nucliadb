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

from nucliadb_utils.encryption import DecryptionDataError, DecryptionError, EndecryptorUtility


def get_encoded_secret_key():
    # Get a 32-bytes random string
    secret_key = uuid4().hex[:32]
    # Encode the secret key in base64
    b64_encoded_secret_key = base64.b64encode(secret_key.encode()).decode()
    return b64_encoded_secret_key


def test_endecryptor():
    # Key generated with: head -c 32 /dev/urandom | base64
    key = "gXGebeyXDimP2qXsSdQ+W1GoQKIjbBmrZBxQHiks7I0="
    endecryptor = EndecryptorUtility.from_b64_encoded_secret_key(key)

    text = "This is some other text"
    encrypted_text = endecryptor.encrypt(text)
    assert isinstance(encrypted_text, str)

    decrypted_text = endecryptor.decrypt(encrypted_text)
    assert decrypted_text == text


def test_errors():
    # Encrypt some text with a key
    key1 = get_encoded_secret_key()
    endecryptor = EndecryptorUtility.from_b64_encoded_secret_key(key1)
    text = "This is some other text"
    encrypted_text = endecryptor.encrypt(text)

    # Should not be able to decrypt with a different key
    key2 = get_encoded_secret_key()
    endecryptor = EndecryptorUtility.from_b64_encoded_secret_key(key2)
    with pytest.raises(DecryptionError):
        endecryptor.decrypt(encrypted_text)

    # Wrongly formatted encrypted text
    with pytest.raises(DecryptionDataError):
        endecryptor.decrypt("wrongly formatted encrypted text")
