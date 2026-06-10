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
