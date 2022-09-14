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

from dataclasses import dataclass
from typing import Dict, List, Optional
import xml.sax
from time import time
from bs4 import BeautifulSoup
from nucliadb_client.resource import Resource
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
)
from nucliadb_models.metadata import InputMetadata, Origin
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.utils_pb2 import Vector
from sentence_transformers import SentenceTransformer
from dateutil.parser import parse
from datetime import datetime
import re
import argparse

from nucliadb_client.client import NucliaDBClient
from nucliadb_client.knowledgebox import KnowledgeBox


model = SentenceTransformer("all-MiniLM-L6-v2")

QUESTIONS: Dict[str, Post] = {}

ANSWERS: Dict[str, str] = {}
client: Optional[NucliaDBClient] = None
kb: Optional[KnowledgeBox] = None

PAYLOADS = []
BROKER_MESSAGES = []


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

    def upload_post(self, question: Post, answer: Post) -> Resource:

        payload = CreateResourcePayload()
        payload.title = question.title
        payload.origin = Origin()
        payload.origin.tags = question.tags
        payload.icon = "text/conversation"
        payload.metadata = InputMetadata()
        payload.metadata.language = "en"
        payload.slug = question.id

        self.count += 1
        if self.count % 50 == 0:
            print(f"{self.count} {datetime.now()}")
        if self.count == 500_000:
            print("DONE")
            exit(0)

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
        payload.conversations["stack"] = icf

        # Create the resource with the row value
        resource = kb.create_resource(payload)

        return resource

    def compute_vectors(self, resource: Resource, question: Post, answer: Post):
        # Upload search information
        # Vectors
        embeddings = model.encode([question.title, question.body, answer.body])

        # Title vector
        vector = Vector(
            start=0,
            end=len(question.title),
            start_paragraph=0,
            end_paragraph=len(question.title),
        )
        vector.vector.extend(embeddings[0])
        resource.add_vectors(
            "title",
            FieldType.GENERIC,
            [vector],
        )

        # Question vector
        vector = Vector(
            start=0,
            end=len(question.body),
            start_paragraph=0,
            end_paragraph=len(question.body),
        )
        vector.vector.extend(embeddings[1])

        resource.add_vectors(
            "stack",
            FieldType.CONVERSATION,
            [vector],
            split=question.id,
        )

        # Vector of answer
        vector = Vector(
            start=0,
            end=len(answer.body),
            start_paragraph=0,
            end_paragraph=len(answer.body),
        )
        vector.vector.extend(embeddings[2])

        resource.add_vectors(
            "stack",
            FieldType.CONVERSATION,
            [vector],
            split=answer.id,
        )

    def compute_text(self, resource: Resource, question: Post, answer: Post):
        # Text
        resource.add_text(
            "stack",
            FieldType.CONVERSATION,
            question.body,
            split=question.id,
        )
        resource.add_text("stack", FieldType.CONVERSATION, answer.body, split=answer.id)

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
                    resource.commit()

                    del QUESTIONS[question_id]
                    del ANSWERS[post_id]

            self.lastEntry = None
        elif name == "posts":
            raise StopIteration

    def characters(self, content):
        pass


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

    args = parser.parse_args()
    client = NucliaDBClient(
        host=args.host, grpc=args.grpc, http=args.http, train=args.train
    )
    kb = client.get_kb(slug="stackoverflow")
    if kb is None:
        kb = client.create_kb(slug="stackoverflow", title="StackOverflow Example")

    print(f"0 {datetime.now()}")

    parser = xml.sax.make_parser()
    parser.setContentHandler(StreamHandler())
    # if you can provide a file-like object it's as simple as
    with open(args.dump) as f:
        parser.parse(f)
