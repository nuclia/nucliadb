# -*- coding: utf-8 -*-
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

from __future__ import annotations

import argparse
import asyncio
import base64
import re
import xml.sax
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import aiofiles
from bs4 import BeautifulSoup  # type: ignore
from dateutil.parser import parse
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.utils_pb2 import Vector
from sentence_transformers import SentenceTransformer  # type: ignore

from nucliadb_client.client import NucliaDBClient
from nucliadb_client.knowledgebox import KnowledgeBox
from nucliadb_client.resource import Resource
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
)
from nucliadb_models.metadata import InputMetadata, Origin
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import CreateResourcePayload

model = SentenceTransformer("all-MiniLM-L6-v2")

QUESTIONS: Dict[str, Post] = {}

ANSWERS: Dict[str, str] = {}
client: Optional[NucliaDBClient] = None
kb: Optional[KnowledgeBox] = None


@dataclass
class Post:
    id: str
    title: Optional[str] = None
    body: Optional[str] = None
    creation: Optional[datetime] = None
    answer: Optional[Post] = None
    tags: Optional[List[str]] = None
    who: Optional[str] = None


class StreamHandler(xml.sax.handler.ContentHandler):

    lastEntry = None
    lastName = None
    count = 0

    def __init__(self, file_obj, num: int = 10000):
        self.num = num
        self.file_obj = file_obj
        super(StreamHandler, self).__init__()

    def upload_post(self, question: Post, answer: Post) -> Resource:

        payload = CreateResourcePayload()
        payload.title = question.title
        payload.origin = Origin()
        if question.tags is None:
            tags = []
        else:
            tags = question.tags
        payload.origin.tags = tags
        payload.icon = "text/conversation"
        payload.metadata = InputMetadata()
        payload.metadata.language = "en"
        payload.slug = question.id  # type: ignore

        imq = InputMessage(
            timestamp=question.creation,
            who=question.who,
            ident=question.id,
            content=InputMessageContent(text=question.body),
        )

        ima = InputMessage(
            timestamp=answer.creation,
            who=answer.who,
            ident=answer.id,
            content=InputMessageContent(text=answer.body),
        )

        icf = InputConversationField()
        icf.messages.append(imq)
        icf.messages.append(ima)
        payload.conversations[FieldIdString("stack")] = icf

        # Create the resource with the row value
        if kb is None:
            raise Exception("No KB defined")
        resource = kb.create_resource(payload)

        return resource

    def compute_vectors(self, resource: Resource, question: Post, answer: Post):
        # Upload search information
        # Vectors
        embeddings = model.encode([question.title, question.body, answer.body])

        # Title vector
        vector = Vector(
            start=0,
            end=len(question.title) if question.title else 0,
            start_paragraph=0,
            end_paragraph=len(question.title) if question.title else 0,
        )
        vector.vector.extend(embeddings[0])
        resource.add_vectors(
            "title",
            FieldType.GENERIC,  # type: ignore
            [vector],
        )

        # Question vector
        vector = Vector(
            start=0,
            end=len(question.body) if question.body else 0,
            start_paragraph=0,
            end_paragraph=len(question.body) if question.body else 0,
        )
        vector.vector.extend(embeddings[1])

        resource.add_vectors(
            "stack",
            FieldType.CONVERSATION,  # type: ignore
            [vector],
            split=question.id,
        )

        # Vector of answer
        vector = Vector(
            start=0,
            end=len(answer.body) if answer.body else 0,
            start_paragraph=0,
            end_paragraph=len(answer.body) if answer.body else 0,
        )
        vector.vector.extend(embeddings[2])

        resource.add_vectors(
            "stack",
            FieldType.CONVERSATION,  # type: ignore
            [vector],
            split=answer.id,
        )

    def compute_text(self, resource: Resource, question: Post, answer: Post):
        # Text
        resource.add_text(
            "stack",
            FieldType.CONVERSATION,  # type: ignore
            question.body if question.body else "",
            split=question.id,
        )
        resource.add_text("stack", FieldType.CONVERSATION, answer.body, split=answer.id)  # type: ignore

    def startElement(self, name, attrs):
        self.lastName = name
        if name == "posts":
            self.lastEntry = {}
        elif name == "row":
            self.lastEntry = attrs

    def endElement(self, name):
        if name == "row":

            post_id = self.lastEntry.get("Id")
            post = Post(id=post_id)
            post.title = self.lastEntry.get("Title")
            body = self.lastEntry.get("Body")
            answer = self.lastEntry.get("AcceptedAnswerId")
            ANSWERS[answer] = post_id
            post.creation = parse(self.lastEntry.get("CreationDate"))
            post.who = self.lastEntry.get("OwnerUserId")
            tree = BeautifulSoup(body, features="html.parser")
            good_html = tree.get_text().replace("\n", " \n ")
            post.body = good_html

            tags = self.lastEntry.get("Tags")
            if tags is not None:
                tags = re.sub(
                    ">$",
                    "",
                    re.sub("^<", "", tags.replace("><", ",")),
                ).split(",")
                post.tags = tags

            type_post = self.lastEntry.get("PostTypeId")
            if type_post == "1":
                QUESTIONS[post_id] = post

            if type_post == "2":
                if post_id in ANSWERS:
                    # We found an answer
                    question_id = ANSWERS[post_id]
                    question_post = QUESTIONS[question_id]
                    resource = self.upload_post(question=question_post, answer=post)
                    self.compute_vectors(
                        resource=resource, question=question_post, answer=post
                    )
                    self.compute_text(
                        resource=resource, question=question_post, answer=post
                    )

                    print("Found one on compute!")
                    self.file_obj.write(
                        base64.b64encode(resource.serialize()).decode() + "\n"
                    )
                    resource.cleanup()
                    self.count += 1
                    if self.count % 50 == 0:
                        print(f"{self.count} {datetime.now()}")

                    if self.count > self.num and self.num != -1:
                        raise StopIteration
                    del QUESTIONS[question_id]
                    del ANSWERS[post_id]

            self.lastEntry = None
        elif name == "posts":
            raise StopIteration

    def characters(self, content):
        pass


async def upload(args, kb):
    print(f"{datetime.now()} lets upload")
    kb.init_async_grpc()
    async with aiofiles.open(args.cache, "r") as cache_file:
        TASKS = []

        for line in await cache_file.readlines():
            if len(line.strip()) == 0:
                continue
            TASKS.append(kb.import_export(line.strip()))
            if len(TASKS) > 10:
                await asyncio.gather(*TASKS)
                print(f"{datetime.now()} upload {len(TASKS)}")
                TASKS = []

        if len(TASKS):
            await asyncio.gather(*TASKS)
            print(f"{datetime.now()} upload {len(TASKS)}")


if __name__ == "__main__":
    # use default ``xml.sax.expatreader``

    parser = argparse.ArgumentParser(description="Ingest stackoverflow")

    parser.add_argument(
        "--dump",
        dest="dump",
    )

    parser.add_argument(
        "--host",
        dest="host",
    )

    parser.add_argument(
        "--grpc",
        dest="grpc",
    )

    parser.add_argument(
        "--http",
        dest="http",
    )

    parser.add_argument(
        "--train",
        dest="train",
    )

    parser.add_argument("--cache", dest="cache", default="cache.nucliadb")

    parser.add_argument("--compute", dest="compute", action="store_true")

    parser.add_argument("--num", dest="num", type=int, default=100000)

    args = parser.parse_args()
    client = NucliaDBClient(
        host=args.host, grpc=args.grpc, http=args.http, train=args.train
    )
    kb = client.get_kb(slug="stackoverflow")
    if kb is None:
        kb = client.create_kb(slug="stackoverflow", title="StackOverflow Example")

    print(f"0 {datetime.now()}")

    if args.compute:
        xml_parser = xml.sax.make_parser()
        with open(args.cache, "w+") as cache_file:
            handler = StreamHandler(cache_file, args.num)
            xml_parser.setContentHandler(handler)
            # if you can provide a file-like object it's as simple as
            with open(args.dump) as f:
                try:
                    xml_parser.parse(f)
                except StopIteration:
                    pass
        print(f"Compute done with {handler.count}")

    asyncio.run(upload(args, kb))
