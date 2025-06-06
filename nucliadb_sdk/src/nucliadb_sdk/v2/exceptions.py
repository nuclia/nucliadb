# Copyright 2025 Bosutech XXI S.L.
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
from typing import Optional


class ClientError(Exception):
    pass


class NotFoundError(ClientError):
    pass


class AuthError(ClientError):
    pass


class AccountLimitError(ClientError):
    pass


class RateLimitError(ClientError):
    def __init__(self, message, try_after: Optional[float] = None):
        super().__init__(message)
        self.try_after = try_after


class ConflictError(ClientError):
    pass


class UnknownError(ClientError):
    pass


class AskResponseError(ClientError):
    pass
