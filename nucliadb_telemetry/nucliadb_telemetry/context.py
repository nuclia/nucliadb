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

# ABOUT
# The module is meant to manage telemetry oriented context data.
# This is a little different than the span context because we do
# not necessarily want to inject all context data into every span,
# only particular ones like the request handler root span.
#
# This allows us to leverage context data for both tracing and logs.
#
import contextvars

context_data = contextvars.ContextVar("data", default=None)


def add_context(new_data: dict[str, str]):
    """
    This implementation always merges and sets the context, even if is was already set.

    This is so data is propated forward but not backward.
    """
    data = context_data.get()
    if data is None:
        data = {}
    else:
        data = data.copy()
    data.update(new_data)
    context_data.set(data)  # always set the context


def get_context() -> dict[str, str]:
    return context_data.get() or {}
