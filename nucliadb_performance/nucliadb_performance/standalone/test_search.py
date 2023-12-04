import re
from functools import cache

from molotov import global_setup, global_teardown, scenario

from nucliadb_models.resource import ReleaseChannel
from nucliadb_performance.settings import settings
from nucliadb_performance.utils.export_import import import_kb
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
from nucliadb_performance.utils.nucliadb import get_nucliadb_client
from nucliadb_performance.utils.saved_requests import load_all_saved_requests
from nucliadb_performance.utils.vectors import compute_vector
from nucliadb_sdk.v2.exceptions import ConflictError

FIND_WEIGHT = 1
SEARCH_WEIGHT = 1
SUGGEST_WEIGHT = 1


def precompute_vectors():
    """
    Precompute vectors for all saved requests at the beginning of the test.
    """
    print("Precomputing vectors for all saved requests...")
    saved_requests = load_all_saved_requests(settings.saved_requests_file)
    for request_set in saved_requests.sets.values():
        for saved_request in request_set.requests:
            if saved_request.request.payload is None:
                continue
            if "query" not in saved_request.request.payload:
                continue
            compute_vector(saved_request.request.payload["query"])


@cache
def get_test_kb():
    slug = parse_input_kb_slug()
    kbid = get_kb(slug=slug)
    return (kbid, slug)


def create_kb(kb_slug: str):
    ndb = get_nucliadb_client(local=True)
    release_channel = (
        ReleaseChannel.EXPERIMENTAL
        if "experimental" in kb_slug
        else ReleaseChannel.STABLE
    )
    print(f"Creating KB {kb_slug}...")
    ndb.writer.create_knowledge_box(slug=kb_slug, release_channel=release_channel)


def import_test_data(kb_slug: str):
    ndb = get_nucliadb_client(local=True)
    export_path = settings.exports_folder + f"/{kb_slug}.export"
    print(f"Importing test data from {export_path}...")
    import_kb(
        ndb,
        uri=export_path,
        kb=kb_slug,
    )


def maybe_import_test_data(kb_slug: str):
    try:
        create_kb(kb_slug)
    except ConflictError:
        print(f"KB {kb_slug} already exists. Skipping importing data.")
        pass
    else:
        import_test_data(kb_slug)


@global_setup()
def init_test(args):
    kb_slug = parse_input_kb_slug()
    maybe_import_test_data(kb_slug)
    get_test_kb()
    precompute_vectors()


@scenario(weight=FIND_WEIGHT)
async def test_find(session):
    kbid, slug = get_test_kb()
    request = get_request(slug, endpoint="find")
    if request.payload is not None and "query" in request.payload:
        request.payload["vector"] = compute_vector(request.payload["query"])
    resp = await make_kbid_request(
        session,
        kbid,
        request.method.upper(),
        request.url.format(kbid=kbid),
        json=request.payload,
    )
    assert len(resp["resources"]) > 0


@scenario(weight=SEARCH_WEIGHT)
async def test_search(session):
    kbid, slug = get_test_kb()
    request = get_request(slug, endpoint="search")
    if request.payload is not None and "query" in request.payload:
        request.payload["vector"] = compute_vector(request.payload["query"])
    resp = await make_kbid_request(
        session,
        kbid,
        request.method.upper(),
        request.url.format(kbid=kbid),
        json=request.payload,
    )
    assert len(resp["resources"]) > 0


@scenario(weight=SUGGEST_WEIGHT)
async def test_suggest(session):
    kbid, slug = get_test_kb()
    request = get_request(slug, endpoint="suggest")
    resp = await make_kbid_request(
        session,
        kbid,
        request.method.upper(),
        request.url.format(kbid=kbid),
        params=request.params,
    )
    assert (
        len(resp["paragraphs"]["results"]) > 0 or len(resp["entities"]["entities"]) > 0
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    if settings.benchmark_output:
        save_benchmark_json_results(settings.benchmark_output)
    print_metrics()
    print_errors()
