import os
from functools import cache
from typing import Optional

from faker import Faker
from molotov import global_setup, global_teardown, scenario

from nucliadb_performance.settings import settings
from nucliadb_performance.utils.kbs import parse_input_kb_slug
from nucliadb_performance.utils.metrics import (
    print_metrics,
    save_benchmark_json_results,
)
from nucliadb_performance.utils.misc import (
    get_kb,
    get_kb_request,
    make_kbid_request,
    print_errors,
)
from nucliadb_performance.utils.vectors import compute_vector

fake = Faker()


@cache
def get_search_tags() -> Optional[tuple[str]]:
    envvar = os.environ.get("SEARCH_TAGS")
    if not envvar:
        return None
    tags = envvar.split(",")
    if len(tags) == 1 and tags[0] == "":
        return None
    return tuple(tags)


@cache
def get_test_kb():
    slug = parse_input_kb_slug()
    kbid = get_kb(slug=slug)
    return (kbid, slug)


@global_setup()
def init_test(args):
    get_test_kb()


@scenario(weight=1)
async def test_find_with_filter(session):
    kbid, slug = get_test_kb()
    payload = get_kb_request(slug, with_tags=get_search_tags())
    payload["vector"] = compute_vector(payload["query"])
    await make_kbid_request(
        session,
        kbid,
        "POST",
        f"/v1/kb/{kbid}/find",
        json=payload,
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    if settings.benchmark_output:
        save_benchmark_json_results(settings.benchmark_output)
    print_metrics()
    print_errors()
