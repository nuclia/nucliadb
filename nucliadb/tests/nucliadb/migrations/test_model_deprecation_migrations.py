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

import pytest

from nucliadb.common.maindb.driver import Driver
from tests.nucliadb.migrations import get_migration

from .utils import _test_model_deprecation_migration


@pytest.mark.parametrize(
    "migration_number,deprecated,replacement",
    [
        (40, "claude-3-5-small", "claude-4-5-sonnet"),
        (40, "gcp-claude-3-5-sonnet-v2", "gcp-claude-4-5-sonnet"),
        (44, "claude-3-5-fast", "claude-4-5-sonnet"),
        (44, "claude-3", "claude-4-7-opus"),
        (44, "azure-mistral-large-2", "chatgpt-azure-5"),
        (44, "llama-3.2-90b-vision-instruct-maas", "llama-4-scout-17b-16e-instruct-maas"),
        (44, "gemini-3-pro", "gemini-3.1-pro"),
        (44, "aws-claude-3-7-sonnet", "aws-claude-4-6-sonnet"),
        (44, "gcp-claude-3-7-sonnet", "gcp-claude-4-6-sonnet"),
        (44, "claude-4-opus", "claude-4-7-opus"),
        (44, "aws-claude-4-opus", "aws-claude-4-6-opus"),
        (44, "gemini-2.0-flash", "gemini-2.5-flash"),
        (44, "gemini-2.0-flash-lite", "gemini-2.5-flash-lite"),
        (45, "claude-4-sonnet", "claude-4-6-sonnet"),
        (46, "chatgpt4", "chatgpt-4.1"),
        (47, "aws-claude-4-sonnet", "aws-claude-4-6-sonnet"),
        (48, "chatgpt-azure-5-chat", "chatgpt-azure-5.6-luna"),
        (48, "chatgpt-5-chat", "chatgpt-5.6-luna"),
        (49, "azure-deepseek-r1", "chatgpt-azure-5.6-luna"),
        (51, "nova-premier", "nova-pro"),
        (52, "claude-4-5-sonnet", "claude-5-sonnet"),
        (52, "chatgpt-azure-4o", "chatgpt-azure-5.6-terra"),
        (52, "chatgpt-azure-4o-mini", "chatgpt-azure-5.6-luna"),
        (52, "chatgpt-azure-o3-mini", "chatgpt-azure-5.6-luna"),
        (53, "aws-claude-4-1-opus", "aws-claude-4-8-opus"),
        (54, "chatgpt-o3-mini", "chatgpt-5.6-sol"),
        (55, "chatgpt-o1", "chatgpt-5.6-sol"),
        (56, "chatgpt-azure-o1", "chatgpt-azure-5.6-sol"),
        (56, "chatgpt-azure-o3", "chatgpt-azure-5.6-sol"),
        (56, "chatgpt-azure-o4-mini", "chatgpt-azure-5.6-terra"),
    ],
)
async def test_model_deprecation_migration(
    maindb_driver: Driver, migration_number: int, deprecated: str, replacement: str
):
    await _test_model_deprecation_migration(
        maindb_driver,
        get_migration(migration_number),
        deprecated_model=deprecated,
        replacement_model=replacement,
    )
