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

import argparse
import os

import httpx

API_PREFIX = "api"
KB_PREFIX = "/kb"


class APIError(Exception):
    def __init__(self, resp):
        self.resp = resp

    def __str__(self):
        status_code = self.resp.status_code
        content = self.resp.content().decode()
        return f"APIError(status_code={status_code}, content={content}"


class APIClient:
    def __init__(self, api_host, bearer, schema="https"):
        base_url = f"{schema}://{api_host}/{API_PREFIX}/v1"
        headers = {
            "Authorization": f"Bearer {bearer}",
            "host": api_host,
        }
        self.http_client = httpx.Client(
            base_url=base_url,
            headers=headers,
        )

    def get_kb(self, kbid: str):
        resp = self.http_client.get(f"{KB_PREFIX}/{kbid}")
        self._check_status_code(resp, 200)
        return resp.json()

    def iter_resources_in_kb(self, kbid: str, page_size=20):
        current_page = 0
        is_last_page = False
        while not is_last_page:
            results = self._get_resources_page(kbid, current_page, page_size)
            for resource in results["resources"]:
                yield resource
            is_last_page = results["pagination"]["last"]
            current_page += 1

    def _check_status_code(self, resp, status_code=None):
        if not status_code and str(resp.status_code).startswith("2"):
            return
        if status_code and status_code == resp.status_code:
            return
        raise APIError(resp)

    def _get_resources_page(self, kbid, current_page, page_size=20):
        resp = self.http_client.get(
            f"{KB_PREFIX}/{kbid}/resources?page={current_page}&page_size={page_size}"
        )
        self._check_status_code(resp, 200)
        return resp.json()

    def reprocess_resource(self, kbid, rid):
        resp = self.http_client.post(f"{KB_PREFIX}/{kbid}/resource/{rid}/reprocess")
        self._check_status_code(resp, 202)

    def reindex_resource(self, kbid, rid):
        resp = self.http_client.post(f"{KB_PREFIX}/{kbid}/resource/{rid}/reindex")
        self._check_status_code(resp, 200)


def get_bearer():
    try:
        return os.environ["BEARER"]
    except KeyError:
        print("You need to set the BEARER environment variable")
        raise


class KnowledgeBoxAdmin:
    def __init__(self, api_host, kbid):
        self.kbid = kbid
        self.client = APIClient(api_host, bearer=get_bearer())

    def reprocess(self, offset=0):
        for index, resource in enumerate(self.client.iter_resources_in_kb(self.kbid)):
            if index < offset:
                print(f"{index}: Skipping: ", resource["title"])
                continue
            print(f"{index}: Reprocessing: ", resource["title"])
            self.client.reprocess_resource(self.kbid, resource["id"])

    def reindex(self, offset=0):
        for index, resource in enumerate(self.client.iter_resources_in_kb(self.kbid)):
            if index < offset:
                print(f"{index}: Skipping: ", resource["title"])
                continue
            print(f"{index}: Reindexing: ", resource["title"])
            self.client.reindex_resource(self.kbid, resource["id"])


def main(args):
    admin = KnowledgeBoxAdmin(args.host, args.kb)
    if args.action == "reprocess":
        admin.reprocess(offset=args.offset)
    elif args.action == "reindex":
        admin.reindex(offset=args.offset)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Tool to perform admin tasks on a KB")
    parser.add_argument(
        "--host",
        dest="host",
        required=True,
        help="Api host e.g: europe-1.stashify.cloud",
    )
    parser.add_argument("--kb", dest="kb", required=True, help="KB uuid")
    parser.add_argument("--action", required=True, choices=["reprocess", "reindex"])
    parser.add_argument("--offset", required=False, type=int)

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
