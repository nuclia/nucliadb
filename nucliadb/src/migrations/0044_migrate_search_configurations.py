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

"""Migration #44

Replaces deprecated and removed generative models from search configurations.

Mirrors the following learning_config migrations:
- 655b2334f507: claude-3-5-fast -> claude-4-5-sonnet
- 2502bf147840: claude-3 -> claude-4-opus
- 969983af44ff: azure-mistral-large-2 -> chatgpt-azure-5
- e8e89c216c70: llama-3.2-90b-vision-instruct-maas -> llama-4-scout-17b-16e-instruct-maas
- 305cf1ea7d52: gemini-3-pro -> gemini-3.1-pro
- 7dfe226bafe5: aws-claude-3-7-sonnet -> aws-claude-4-6-sonnet
- 5e72244eff39: gcp-claude-3-7-sonnet -> gcp-claude-4-6-sonnet
- 9c4ec078b5f3: claude-4-opus -> claude-4-7-opus
                aws-claude-4-opus -> aws-claude-4-6-opus
                gemini-2.0-flash -> gemini-2.5-flash
                gemini-2.0-flash-lite -> gemini-2.5-flash-lite

"""

from nucliadb.migrator.context import ExecutionContext
from nucliadb.migrator.search_configurations import migrate_search_configurations

REPLACEMENTS = {
    "claude-3-5-fast": "claude-4-5-sonnet",
    "claude-3": "claude-4-7-opus",
    "azure-mistral-large-2": "chatgpt-azure-5",
    "llama-3.2-90b-vision-instruct-maas": "llama-4-scout-17b-16e-instruct-maas",
    "gemini-3-pro": "gemini-3.1-pro",
    "aws-claude-3-7-sonnet": "aws-claude-4-6-sonnet",
    "gcp-claude-3-7-sonnet": "gcp-claude-4-6-sonnet",
    "claude-4-opus": "claude-4-7-opus",
    "aws-claude-4-opus": "aws-claude-4-6-opus",
    "gemini-2.0-flash": "gemini-2.5-flash",
    "gemini-2.0-flash-lite": "gemini-2.5-flash-lite",
}


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    await migrate_search_configurations(kbid, REPLACEMENTS)
