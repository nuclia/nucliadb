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
from collections.abc import Callable
from contextlib import contextmanager
from unittest.mock import patch

from nucliadb_models.internal.predict import (
    QueryInfo,
)
from nucliadb_utils.utilities import (
    Utility,
    get_utility,
)


@contextmanager
def predict_query_hook(post: Callable[[QueryInfo], QueryInfo]):
    """Add a callback for PredictEngine.query function."""
    predict = get_utility(Utility.PREDICT)
    original = predict.query

    async def wrapped_query(*args, **kwargs):
        nonlocal original

        query_info = await original(*args, **kwargs)
        return post(query_info)

    with patch.object(predict, "query", wrapped_query):
        yield


@contextmanager
def custom_default_min_score(min_score: float):
    predict = get_utility(Utility.PREDICT)
    with patch.object(predict, "default_semantic_threshold", min_score):
        yield
