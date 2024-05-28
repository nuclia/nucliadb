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
        print(
            "#%s: %s:%s: %.1f KiB"
            % (index, frame.filename, frame.lineno, stat.size / 1024)
        )
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print("    %s" % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))
