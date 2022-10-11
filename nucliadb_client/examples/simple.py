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
import json
import random
import tempfile
from dataclasses import dataclass
from typing import List

import aiofiles
from nucliadb_models.metadata import InputMetadata, Origin
from nucliadb_models.text import TextField
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.utils_pb2 import Vector
from sentence_transformers import SentenceTransformer  # type: ignore

from nucliadb_client.client import NucliaDBClient
from nucliadb_client.knowledgebox import CODEX

model = SentenceTransformer("all-MiniLM-L6-v2")

DATA_FILENAME = "articles.json"
CACHE_FILENAME = tempfile.mktemp(suffix=".nucliadb")


@dataclass
class Article:
    id: str
    title: str
    body: str


def load_articles(filename: str) -> List[Article]:
    with open(filename, "r") as f:
        texts = json.load(f)

    articles = []
    for id in texts:
        articles.append(
            Article(
                id=id,
                title=texts[id]["title"],
                body=texts[id]["body"],
            )
        )
    return articles


def process_articles(kb):
    articles = load_articles(DATA_FILENAME)
    cache_file = open(CACHE_FILENAME, "w+")

    for article in articles:
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

            resource.add_vectors(field, FieldType.TEXT, [vector])

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
    articles = load_articles(DATA_FILENAME)
    article = articles[random.randint(0, len(articles) - 1)]
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
