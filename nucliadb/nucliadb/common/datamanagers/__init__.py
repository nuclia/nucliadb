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
# ==============================================================================
# Rules for datamanagers:
#   - Try to be consistent
#   - All datamanagers should be imported in __init__.py
#   - Manage keys of K/V Storage and try not to leak them
#   - Transactions are managed OUTSIDE of the datamanagers module
#   - There shouldn't be ANY db commits in the datamanagers module, that is handled outside right now.
#   - Module level functions only
#   - First argument is always a transaction, all other arguments are keyword arguments and must be explicit
#     (better for readability and code editors)
# ==============================================================================
from . import (
    atomic,
    cluster,
    entities,
    exceptions,
    fields,
    kb,
    labels,
    processing,
    resources,
    rollover,
    synonyms,
    vectorsets,
)
from .utils import with_ro_transaction, with_rw_transaction, with_transaction

__all__ = (
    "atomic",
    "cluster",
    "entities",
    "exceptions",
    "fields",
    "kb",
    "labels",
    "processing",
    "resources",
    "rollover",
    "synonyms",
    "vectorsets",
    "with_transaction",
    "with_rw_transaction",
    "with_ro_transaction",
)
