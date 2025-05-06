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
import asyncio
from typing import Optional, TypeVar, Union

from typing_extensions import TypeIs

from .models import ExecutionContext, PlanStep

T = TypeVar("T")
A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


# We use a class as cache miss marker to allow None values in the cache and to
# make mypy happy with typing
class NotCached:
    pass


def is_cached(field: Union[T, NotCached]) -> TypeIs[T]:
    return not isinstance(field, NotCached)


# class DiamondStep(PlanStep[tuple[A, B]]):
#     def __init__(self, )


class CachedStep(PlanStep[T]):
    """Executes a plan step and caches the result. Following calls to `execute`
    will return the cached result.

    This is useful to build diamonds in a tree. I.e., we want this:
       A
     // \\
     B   C
     \\ //
       D

    and implement it like this:
       A
     // \\
     B   C
     |   |
     D'  D'

    where D' is cached D step

    """

    def __init__(self, step: PlanStep[T]):
        self.step = step
        self.lock = asyncio.Lock()
        self.cache: Union[T, NotCached] = NotCached()

    async def execute(self, context: ExecutionContext) -> T:
        async with self.lock:
            if not is_cached(self.cache):
                print("not cached")
                self.cache = await self.step.execute(context)
                print("caching")
                print("self is", self)
            else:
                print("is cached")
        return self.cache

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "Cached": self.step.explain(),
        }


class Parallel(PlanStep[tuple[A, B]]):
    """A parallel step executes multiple steps in different asyncio tasks and
    gathers and returns all results. This is useful when multiple steps can be
    done concurrently.

    """

    def __init__(self, a: PlanStep[A], b: PlanStep[B]):
        self.a = a
        self.b = b

    async def execute(self, context: ExecutionContext) -> tuple[A, B]:
        a, b = await asyncio.gather(
            self.a.execute(context),
            self.b.execute(context),
        )
        return (a, b)

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "Parallel": {
                "A": self.a.explain(),
                "B": self.b.explain(),
            }
        }


class Group(PlanStep[tuple[A, B]]):
    """Executes a series of steps in order and return all its values. This is
    useful when we need to group results from multiple steps that depend on each
    other.

    """

    def __init__(self, a: PlanStep[A], b: PlanStep[B]):
        self.a = a
        self.b = b

    async def execute(self, context: ExecutionContext) -> tuple[A, B]:
        a = await self.a.execute(context)
        b = await self.b.execute(context)
        return (a, b)

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "Group": {
                "A": self.a.explain(),
                "B": self.b.explain(),
            }
        }


# TODO: implement a more generic Flatten
class Flatten(PlanStep[tuple[A, B, C]]):
    def __init__(self, step: PlanStep[tuple[tuple[A, B], C]]):
        self.step = step

    async def execute(self, context: ExecutionContext) -> tuple[A, B, C]:
        (a, b), c = await self.step.execute(context)
        return (a, b, c)

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {"Flatten": self.step.explain()}


class OptionalStep(PlanStep[Optional[T]]):
    def __init__(self, step: Optional[PlanStep[T]]):
        self.step = step

    async def execute(self, context: ExecutionContext) -> Optional[T]:
        if self.step is None:
            return None
        else:
            return await self.step.execute(context)

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        if self.step is None:
            return {"Optional": "None"}
        else:
            return {"Optional": self.step.explain()}
