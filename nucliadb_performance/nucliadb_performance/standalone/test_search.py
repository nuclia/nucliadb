from functools import cache

from molotov import global_setup, global_teardown, scenario

from nucliadb_performance.settings import settings
from nucliadb_performance.utils.kbs import parse_input_kb_slug
from nucliadb_performance.utils.metrics import (
    print_metrics,
    save_benchmark_json_results,
)
from nucliadb_performance.utils.misc import (
    get_kb,
    get_request,
    make_kbid_request,
    print_errors,
)
from nucliadb_performance.utils.vectors import compute_vector


@cache
def get_test_kb():
    slug = parse_input_kb_slug()
    kbid = get_kb(slug=slug)
    return (kbid, slug)


@global_setup()
def init_test(args):
    get_test_kb()


@scenario(weight=1)
async def test_find(session):
    kbid, slug = get_test_kb()
    request = get_request(slug, with_tags=settings.request_tags)
    request.payload["vector"] = compute_vector(request.payload["query"])
    await make_kbid_request(
        session,
        kbid,
        request.method.upper(),
        request.url.format(kbid=kbid),
        json=request.payload,
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    if settings.benchmark_output:
        save_benchmark_json_results(settings.benchmark_output)
    print_metrics()
    print_errors()
