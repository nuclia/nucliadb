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
#

# ABOUT
# The module is meant to manage telemetry oriented context data.
# This is a little different than the span context because we do
# not necessarily want to inject all context data into every span,
# only particular ones like the request handler root span.
#
# This allows us to leverage context data for both tracing and logs.
#
import contextvars
from typing import Dict, Optional, Sequence, Union

from opentelemetry.trace import get_current_span

from nucliadb_telemetry.settings import telemetry_settings

context_data = contextvars.ContextVar[Optional[Dict[str, str]]]("data", default=None)


def add_context(new_data: Dict[str, str]):
    """
    This implementation always merges and sets the context, even if is was already set.

    This is so data is propated forward but not backward.
    """

    # set the data on the current active span
    set_info_on_span({f"nuclia.{key}": value for key, value in new_data.items()})

    data = context_data.get()
    if data is None:
        data = {}
    else:
        data = data.copy()

    data.update(new_data)
    context_data.set(data)  # always set the context


def clear_context():
    context_data.set({})


def get_context() -> Dict[str, str]:
    return context_data.get() or {}


def set_info_on_span(
    headers: Dict[
        str,
        Union[
            str,
            bool,
            int,
            float,
            Sequence[str],
            Sequence[bool],
            Sequence[int],
            Sequence[float],
        ],
    ],
):
    if telemetry_settings.jaeger_enabled:
        span = get_current_span()
        if span is not None:
            span.set_attributes(headers)
