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

from typing import TypeVar

T = TypeVar("T")


def cut_page(items: list[T], page_size: int, page_number: int) -> tuple[list[T], bool]:
    """Return a slice of `items` representing the specified page and a boolean
    indicating whether there is a next page or not"""
    start = page_size * page_number
    end = start + page_size
    next_page = len(items) > end
    return items[start:end], next_page
