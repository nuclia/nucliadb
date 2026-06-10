# Copyright 2021 Bosutech XXI S.L.
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
import contextlib
import linecache
import tracemalloc


@contextlib.contextmanager
def profile_memory(top_lines: int = 10):  # pragma: no cover
    tracemalloc.start()
    before = tracemalloc.take_snapshot()
    try:
        yield
    finally:
        after = tracemalloc.take_snapshot()
        display_top(after, limit=top_lines)
        display_difference(before, after, limit=top_lines)
        tracemalloc.stop()


def display_difference(before, after, key_type="lineno", limit=10):  # pragma: no cover
    difference = after.compare_to(before, key_type)
    print(f"Top {limit} difference")
    for i, stat in enumerate(difference[:limit]):
        print(f"#{i}: {stat}")


def display_top(snapshot, key_type="lineno", limit=10):  # pragma: no cover
    snapshot = snapshot.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        )
    )
    top_stats = snapshot.statistics(key_type)

    print(f"Top {limit} lines")
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        print(f"#{index}: {frame.filename}:{frame.lineno}: {stat.size / 1024:.1f} KiB")
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print("    %s" % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print(f"{len(other)} other: {size / 1024:.1f} KiB")
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))
