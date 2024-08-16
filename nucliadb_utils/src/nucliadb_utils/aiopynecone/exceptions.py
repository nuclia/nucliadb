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

from typing import Any, Optional

import httpx

from nucliadb_telemetry.metrics import Counter

pinecone_errors_counter = Counter("pinecone_errors", labels={"type": "", "status_code": ""})


class PineconeAPIError(Exception):
    """
    Generic Pinecone API error.
    """

    def __init__(
        self,
        http_status_code: int,
        code: Optional[str] = None,
        message: Optional[str] = None,
        details: Optional[Any] = None,
    ):
        self.http_status_code = http_status_code
        self.code = code or ""
        self.message = message or ""
        self.details = details or {}
        exc_message = '[{http_status_code}] message="{message}" code={code} details={details}'.format(
            http_status_code=http_status_code,
            message=message,
            code=code,
            details=details,
        )
        super().__init__(exc_message)


class RetriablePineconeAPIError(PineconeAPIError):
    """
    Raised when the client can retry the operation.
    """

    pass


class PineconeRateLimitError(RetriablePineconeAPIError):
    """
    Raised when the client has exceeded the rate limit to be able to backoff and retry.
    """

    pass


class PineconeNeedsPlanUpgradeError(PineconeAPIError):
    """
    Raised when the client needs to upgrade the plan to continue using the service.
    """

    pass


class MetadataTooLargeError(ValueError):
    """
    Raised when the metadata of a vector to be upserted is too large.
    """

    pass


def raise_for_status(operation: str, response: httpx.Response):
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        pinecone_errors_counter.inc(labels={"type": operation, "status_code": str(response.status_code)})
        code = None
        message = None
        details = None
        try:
            resp_json = response.json()
            error = resp_json.get("error") or {}
            code = resp_json.get("code") or error.get("code")
            message = resp_json.get("message") or error.get("message") or ""
            details = resp_json.get("details") or error.get("details")
        except Exception:  # pragma: no cover
            message = response.text
        if response.status_code == 429:
            if "month" in message:
                raise PineconeNeedsPlanUpgradeError(
                    http_status_code=response.status_code,
                    code=code,
                    message=message,
                    details=details,
                )
            raise PineconeRateLimitError(
                http_status_code=response.status_code,
                code=code,
                message=message,
                details=details,
            )

        if str(response.status_code).startswith("5"):
            raise RetriablePineconeAPIError(
                http_status_code=response.status_code,
                code=code,
                message=message,
                details=details,
            )

        raise PineconeAPIError(
            http_status_code=response.status_code,
            code=code,
            message=message,
            details=details,
        )
