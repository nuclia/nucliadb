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
import asyncio
import base64
import random
import tempfile
from dataclasses import dataclass
from typing import List

import aiofiles
from nucliadb.models.metadata import InputMetadata, Origin
from nucliadb.models.text import TextField
from nucliadb.models.writer import CreateResourcePayload
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.utils_pb2 import Vector
from sentence_transformers import SentenceTransformer  # type: ignore

from nucliadb_client.client import NucliaDBClient
from nucliadb_client.knowledgebox import CODEX


model = SentenceTransformer("all-MiniLM-L6-v2")

CACHE_FILENAME = tempfile.mktemp(suffix=".nucliadb")


@dataclass
class Article:
    id: str
    title: str
    body: List[str]


ARTICLES = [
    Article(
        id="semantic-search",
        title="Semantic search",
        body=[
            (
                "Semantic search denotes search with meaning, as distinguished from "
                "lexical search where the search engine looks for "
                "literal matches of the query words or variants of "
                "them, without understanding the overall meaning of the "
                "query. Semantic search seeks to improve search "
                "accuracy by understanding the searcher's intent and "
                "the contextual meaning of terms as they appear in the "
                "searchable dataspace, whether on the Web or within a "
                "closed system, to generate more relevant "
                "results. Content that ranks well in semantic search is "
                "well-written in a natural voice, focuses on the user's "
                "intent, and considers related topics that the user may "
                "look for in the future. "
            ),
            (
                "Some authors regard semantic search as a set of techniques for "
                "retrieving knowledge from richly structured data "
                "sources like ontologies and XML as found on the "
                "Semantic Web. Such technologies enable the formal "
                "articulation of domain knowledge at a high level of "
                "expressiveness and could enable the user to specify "
                "their intent in more detail at query time. "
            )
        ]
    ),

    Article(
        id="database",
        title="Database",
        body=[
            (
                "In computing, a database is an organized collection of "
                "data stored and accessed electronically. Small "
                "databases can be stored on a file system, while large "
                "databases are hosted on computer clusters or cloud "
                "storage. The design of databases spans formal "
                "techniques and practical considerations, including "
                "data modeling, efficient data representation and "
                "storage, query languages, security and privacy of "
                "sensitive data, and distributed computing issues, "
                "including supporting concurrent access and fault "
                "tolerance."
            ),
            (
                "A database management system (DBMS) is the software that interacts "
                "with end users, applications, and the database itself "
                "to capture and analyze the data. The DBMS software "
                "additionally encompasses the core facilities provided "
                "to administer the database. The sum total of the "
                "database, the DBMS and the associated applications can "
                "be referred to as a database system. Often the term "
                "\"database\" is also used loosely to refer to any of "
                "the DBMS, the database system or an application "
                "associated with the database. "
            ),
            (
                "Computer scientists may classify database management systems "
                "according to the database models that they "
                "support. Relational databases became dominant in the "
                "1980s. These model data as rows and columns in a "
                "series of tables, and the vast majority use SQL for "
                "writing and querying data. In the 2000s, "
                "non-relational databases became popular, collectively "
                "referred to as NoSQL, because they use different query "
                "languages. "
            )
        ]
    ),
]


def process_articles(kb):
    cache_file = open(CACHE_FILENAME, "w+")

    for article in ARTICLES:
        payload = CreateResourcePayload()
        payload.title = article.title
        payload.slug = article.id
        payload.icon = "text/plain"
        payload.metadata = InputMetadata()
        payload.metadata.language = "en"
        payload.origin = Origin()

        for i, paragraph in enumerate(article.body):
            payload.texts[f"paragraph-{i}"] = TextField(body=paragraph)

        resource = kb.create_resource(payload)

        embeddings = model.encode(article.body)
        for i, paragraph in enumerate(article.body):
            field = f"paragraph-{i}"
            resource.add_text(field, FieldType.TEXT, paragraph)

            vector = Vector(
                start=0,
                end=len(paragraph),
                start_paragraph=0,
                end_paragraph=len(paragraph),
            )
            vector.vector.extend(embeddings[i])

            resource.add_vectors(field, FieldType.TEXT, [vector])  # type: ignore

            bm = base64.b64encode(resource.serialize()).decode()
            cache_file.write(CODEX.RES + bm + "\n")

    cache_file.close()


async def upload_extracted(kb):
    kb.init_async_grpc()
    async with aiofiles.open(CACHE_FILENAME, "r") as cache_file:
        for line in await cache_file.readlines():
            line = line.strip()
            if not line:
                continue
            await kb.import_export(line)


def print_random_vector():
    article = ARTICLES[random.randint(0, len(ARTICLES) - 1)]
    paragraph = article.body[random.randint(0, len(article.body) - 1)]
    embeddings = model.encode([paragraph])
    print(list(embeddings[0]))


def main():
    client = NucliaDBClient(
        host=args.host, grpc=args.grpc, http=args.http, train=args.train
    )
    kb = client.get_kb(slug=args.kb)
    if kb is None:
        kb = client.create_kb(slug=args.kb, title="Simple NucliaDB example")

    process_articles(kb)
    asyncio.run(upload_extracted(kb))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple NucliaDB example")

    parser.add_argument(
        "--host",
        dest="host",
        required=True,
    )

    parser.add_argument(
        "--grpc",
        dest="grpc",
        required=True,
    )

    parser.add_argument(
        "--http",
        dest="http",
        required=True,
    )

    parser.add_argument(
        "--train",
        dest="train",
        required=True,
    )

    parser.add_argument("--kb", dest="kb", required=True, help="KB slug")

    parser.add_argument(
        "--print-random-vector", dest="print_random_vector", action="store_true"
    )

    args = parser.parse_args()

    if args.print_random_vector:
        print_random_vector()
    else:
        main()
