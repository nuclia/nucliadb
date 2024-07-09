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
import binascii

import nacl.exceptions
import nacl.secret
import nacl.utils

from nucliadb_telemetry.metrics import Observer


class DecryptionError(Exception): ...


class DecryptionDataError(DecryptionError): ...


endecryptor_observer = Observer(
    "endecryptor",
    error_mappings={
        "DecryptionError": DecryptionError,
        "DecryptionDataError": DecryptionDataError,
    },
    labels={"operation": ""},
)


class EndecryptorUtility:
    """
    Utility class for encryption and decryption of sensitive data using the nacl library.
    """

    def __init__(self, secret_key: bytes):
        try:
            self.box = nacl.secret.SecretBox(secret_key)
        except nacl.exceptions.TypeError as ex:
            raise ValueError("Invalid secret key") from ex

    @classmethod
    def from_b64_encoded_secret_key(cls, encoded_secret_key: str):
        try:
            secret_key = base64.b64decode(encoded_secret_key)
        except binascii.Error as ex:
            raise ValueError("Invalid base64 encoding") from ex
        return cls(secret_key=secret_key)

    @endecryptor_observer.wrap({"operation": "encrypt"})
    def encrypt(self, text: str) -> str:
        text_bytes = text.encode()
        encrypted = self.box.encrypt(text_bytes)
        return encrypted.hex()

    @endecryptor_observer.wrap({"operation": "decrypt"})
    def decrypt(self, encrypted_text: str) -> str:
        try:
            encrypted = nacl.utils.EncryptedMessage.fromhex(encrypted_text)
        except ValueError as ex:
            raise DecryptionDataError("Error decrypting the message") from ex
        try:
            decrypted_bytes = self.box.decrypt(encrypted)
        except nacl.exceptions.CryptoError as ex:
            raise DecryptionError("Error decrypting the message") from ex
        return decrypted_bytes.decode()
