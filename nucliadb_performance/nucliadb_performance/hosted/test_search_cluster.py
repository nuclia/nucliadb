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

import asyncio
import random

from faker import Faker
from molotov import get_context, global_setup, global_teardown, scenario

from nucliadb_performance.utils.errors import print_errors
from nucliadb_performance.utils.misc import (
    get_fake_word,
    load_kbs,
    make_kbid_request,
    pick_kb,
)

fake = Faker()

# These weights have been obtained from grafana in production by looking at
# some time period where we had some traffic and getting the average
# request rate for each endpoint during that time period.
SUGGEST_WEIGHT = 40.4
CATALOG_WEIGHT = 38.4
CHAT_WEIGHT = 10
FIND_WEIGHT = 5.3
SEARCH_WEIGHT = 5.6


def get_kb_for_worker(session):
    worker_id = get_context(session).worker_id
    kbid = pick_kb(worker_id)
    return kbid


@global_setup()
def init_test(args):
    kbs = load_kbs()

    print(f"Running cluster test. {len(kbs)} found")


@scenario(weight=SUGGEST_WEIGHT)
async def test_suggest(session):
    kbid = get_kb_for_worker(session)
    await make_kbid_request(
        session,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/suggest",
        params={"query": get_fake_word()},
    )


@scenario(weight=CATALOG_WEIGHT)
async def test_catalog(session):
    kbid = get_kb_for_worker(session)
    await make_kbid_request(
        session,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/catalog",
    )


@scenario(weight=CHAT_WEIGHT)
async def test_chat(session):
    kbid = get_kb_for_worker(session)
    # To avoid calling the LLM in the performance test, we simulate the
    # chat intraction as a find (the retrieval phase) plus some synthetic
    # sleep time (the LLM answer generation streaming time)
    await make_kbid_request(
        session,
        kbid,
        "POST",
        f"/v1/kb/{kbid}/find",
        json={"query": fake.sentence()},
    )
    sleep_time = abs(random.gauss(2, 1))
    await asyncio.sleep(sleep_time)


@scenario(weight=FIND_WEIGHT)
async def test_find(session):
    kbid = get_kb_for_worker(session)
    await make_kbid_request(
        session,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/find",
        params={"query": fake.sentence()},
    )


@scenario(weight=SEARCH_WEIGHT)
async def test_search(session):
    kbid = get_kb_for_worker(session)
    await make_kbid_request(
        session,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/search",
        params={"query": fake.sentence()},
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    print_errors()
