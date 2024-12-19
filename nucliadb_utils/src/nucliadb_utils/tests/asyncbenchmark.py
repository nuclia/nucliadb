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

# This code is derived from pytest-benchmark at https://github.com/ionelmc/pytest-benchmark

from __future__ import division, print_function

import gc
import sys
import time
from math import ceil
from typing import Any, Awaitable, Callable, Coroutine, Dict, TypeVar

import pytest
from pytest_benchmark.session import BenchmarkSession  # type: ignore
from pytest_benchmark.stats import Metadata  # type: ignore
from pytest_benchmark.timers import compute_timer_precision  # type: ignore
from pytest_benchmark.utils import NameWrapper, format_time  # type: ignore

T = TypeVar("T")


class FixtureAlreadyUsed(Exception): ...  # noqa


class AsyncBenchmarkFixture(object):  # pragma: no cover
    _precisions: Dict[Callable, float] = {}

    def __init__(
        self,
        node,
        disable_gc,
        timer,
        min_rounds,
        min_time,
        max_time,
        warmup,
        warmup_iterations,
        calibration_precision,
        add_stats,
        logger,
        warner,
        disabled,
        cprofile,
        group=None,
        **kwargs,  # We get called from pytest-benchmark. Ignoring all extra params to be compatible when they add more
    ):
        self.name = node.name
        self.fullname = node._nodeid
        self.disabled = disabled
        if hasattr(node, "callspec"):
            self.param = node.callspec.id
            self.params = node.callspec.params
        else:
            self.param = None
            self.params = None
        self.group = group
        self.has_error = False
        self.extra_info = {}
        self.skipped = False

        self._disable_gc = disable_gc
        self._timer = timer.target
        self._min_rounds = min_rounds
        self._max_time = float(max_time)
        self._min_time = float(min_time)
        self._add_stats = add_stats
        self._calibration_precision = calibration_precision
        self._warmup = warmup and warmup_iterations
        self._logger = logger
        self._warner = warner
        self._cleanup_callbacks = []
        self._mode = None
        self.cprofile = cprofile
        self.cprofile_stats = None
        self.stats = None

    @property
    def enabled(self):
        return not self.disabled

    def _get_precision(self, timer):
        if timer in self._precisions:
            timer_precision = self._precisions[timer]
        else:
            timer_precision = self._precisions[timer] = compute_timer_precision(timer)
            self._logger.debug("")
            self._logger.debug(
                "Computing precision for %s ... %ss."
                % (NameWrapper(timer), format_time(timer_precision)),
                blue=True,
                bold=True,
            )
        return timer_precision

    async def _make_runner(self, function_to_benchmark: Callable[..., Awaitable[T]], args, kwargs):
        async def runner(loops_range, timer=self._timer):
            gc_enabled = gc.isenabled()
            if self._disable_gc:
                gc.disable()
            tracer = sys.gettrace()
            sys.settrace(None)
            try:
                if loops_range:
                    start = timer()
                    for _ in loops_range:
                        await function_to_benchmark(*args, **kwargs)
                    end = timer()
                    return end - start
                else:
                    start = timer()
                    result = await function_to_benchmark(*args, **kwargs)
                    end = timer()
                    return end - start, result
            finally:
                sys.settrace(tracer)
                if gc_enabled:
                    gc.enable()

        return runner

    def _make_stats(self, iterations):
        bench_stats = Metadata(
            self,
            iterations=iterations,
            options={
                "disable_gc": self._disable_gc,
                "timer": self._timer,
                "min_rounds": self._min_rounds,
                "max_time": self._max_time,
                "min_time": self._min_time,
                "warmup": self._warmup,
            },
        )
        self._add_stats(bench_stats)
        self.stats = bench_stats
        return bench_stats

    async def __call__(
        self,
        function_to_benchmark: Callable[..., Coroutine[Any, Any, T]],
        *args,
        **kwargs,
    ) -> T:
        if self._mode:
            self.has_error = True
            raise FixtureAlreadyUsed(
                "Fixture can only be used once. Previously it was used in %s mode." % self._mode
            )
        try:
            self._mode = "benchmark(...)"
            return await self._raw(function_to_benchmark, *args, **kwargs)
        except Exception:
            self.has_error = True
            raise

    async def _raw(
        self,
        function_to_benchmark: Callable[..., Coroutine[Any, Any, T]],
        *args,
        **kwargs,
    ) -> T:
        if self.enabled:
            runner = await self._make_runner(function_to_benchmark, args, kwargs)

            duration, iterations, loops_range = await self._calibrate_timer(runner)

            # Choose how many time we must repeat the test
            rounds = int(ceil(self._max_time / duration))
            rounds = max(rounds, self._min_rounds)
            rounds = min(rounds, sys.maxsize)

            stats = self._make_stats(iterations)

            self._logger.debug(
                "  Running %s rounds x %s iterations ..." % (rounds, iterations),
                yellow=True,
                bold=True,
            )
            run_start = time.time()
            if self._warmup:
                warmup_rounds = min(rounds, max(1, int(self._warmup / iterations)))
                self._logger.debug(
                    "  Warmup %s rounds x %s iterations ..." % (warmup_rounds, iterations)
                )
                for _ in range(warmup_rounds):
                    await runner(loops_range)
            for _ in range(rounds):
                stats.update(await runner(loops_range))
            self._logger.debug(
                "  Ran for %ss." % format_time(time.time() - run_start),
                yellow=True,
                bold=True,
            )
        function_result: T = await function_to_benchmark(*args, **kwargs)
        return function_result

    def _cleanup(self):
        while self._cleanup_callbacks:
            callback = self._cleanup_callbacks.pop()
            callback()
        if not self._mode and not self.skipped:
            self._logger.warning(
                "Benchmark fixture was not used at all in this test!",
                warner=self._warner,
                suspend=True,
            )

    async def _calibrate_timer(self, runner):
        timer_precision = self._get_precision(self._timer)
        min_time = max(self._min_time, timer_precision * self._calibration_precision)
        min_time_estimate = min_time * 5 / self._calibration_precision
        self._logger.debug("")
        self._logger.debug(
            "  Calibrating to target round %ss; will estimate when reaching %ss "
            "(using: %s, precision: %ss)."
            % (
                format_time(min_time),
                format_time(min_time_estimate),
                NameWrapper(self._timer),
                format_time(timer_precision),
            ),
            yellow=True,
            bold=True,
        )

        loops = 1
        while True:
            loops_range = range(loops)
            duration = await runner(loops_range)
            if self._warmup:
                warmup_start = time.time()
                warmup_iterations = 0
                warmup_rounds = 0
                while time.time() - warmup_start < self._max_time and warmup_iterations < self._warmup:
                    duration = min(duration, await runner(loops_range))
                    warmup_rounds += 1
                    warmup_iterations += loops
                self._logger.debug(
                    "    Warmup: %ss (%s x %s iterations)."
                    % (format_time(time.time() - warmup_start), warmup_rounds, loops)
                )

            self._logger.debug(
                "    Measured %s iterations: %ss." % (loops, format_time(duration)),
                yellow=True,
            )
            if duration >= min_time:
                break

            if duration >= min_time_estimate:
                # coarse estimation of the number of loops
                loops = int(ceil(min_time * loops / duration))
                self._logger.debug("    Estimating %s iterations." % loops, green=True)
                if loops == 1:
                    # If we got a single loop then bail early - nothing to calibrate if the the
                    # test function is 100 times slower than the timer resolution.
                    loops_range = range(loops)
                    break
            else:
                loops *= 10
        return duration, loops, loops_range


@pytest.fixture(scope="function")
async def asyncbenchmark(request: pytest.FixtureRequest):  # pragma: no cover
    bs: BenchmarkSession = request.config._benchmarksession  # type: ignore

    if bs.skip:
        pytest.skip("Benchmarks are skipped (--benchmark-skip was used).")
    else:
        node = request.node
        marker = node.get_closest_marker("benchmark")
        options = dict(marker.kwargs) if marker else {}
        if "timer" in options:
            options["timer"] = NameWrapper(options["timer"])
        fixture = AsyncBenchmarkFixture(
            node,
            add_stats=bs.benchmarks.append,
            logger=bs.logger,
            warner=request.node.warn,
            disabled=bs.disabled,
            **dict(bs.options, **options),
        )
        request.addfinalizer(fixture._cleanup)
        return fixture
