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

import asyncio
from concurrent.futures import ThreadPoolExecutor

import nacl.secret
import nacl.utils


class EndecryptorUtility:
    """
    Utility class for encryption and decryption of sensitive data using the nacl library.
    It uses a thread pool to avoid blocking the event loop.
    """

    def __init__(self, key: bytes, max_thread_pool_size: int = 1):
        self.box = nacl.secret.SecretBox(key)
        self.executor = ThreadPoolExecutor(max_workers=max_thread_pool_size)

    async def encrypt_text(self, message: str) -> str:
        message_bytes = message.encode()
        encrypted = await self.encrypt(message_bytes)
        return encrypted.hex()

    async def encrypt(self, message: bytes) -> nacl.utils.EncryptedMessage:
        loop = asyncio.get_running_loop()
        func = self.box.encrypt
        encrypted = await loop.run_in_executor(self.executor, func, message)
        return encrypted

    async def decrypt_text(self, encrypted_hex: str) -> str:
        encrypted = nacl.utils.EncryptedMessage.fromhex(encrypted_hex)
        decrypted_bytes = await self.decrypt(encrypted)
        return decrypted_bytes.decode()

    async def decrypt(self, encrypted: nacl.utils.EncryptedMessage) -> bytes:
        loop = asyncio.get_running_loop()
        func = self.box.decrypt
        result_bytes = await loop.run_in_executor(self.executor, func, encrypted)
        return result_bytes
