# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
import xml.sax
import sys
from bs4 import BeautifulSoup
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


@dataclass
class Post:
    title: Optional[str] = None
    body: Optional[str] = None
    vectors: Optional[List[float]] = None
    creation: Optional[datetime] = None
    answer: Optional[Post] = None
    tags: Optional[List[str]] = None
    who: Optional[str] = None


class StreamHandler(xml.sax.handler.ContentHandler):

    lastEntry = None
    lastName = None
    count = 0

    def startElement(self, name, attrs):
        self.lastName = name
        if name == "posts":
            self.lastEntry = {}
        elif name == "row":
            self.lastEntry = attrs

    def endElement(self, name):
        if name == "row":

            post = Post()
            post.title = self.lastEntry.get("Title")
            post_id = self.lastEntry.get("Id")
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

            # Sentences are encoded by calling model.encode()
            embeddings = model.encode([good_html])
            post.vectors = embeddings[0]

            type_post = self.lastEntry.get("PostTypeId")
            if type_post == "1":
                QUESTIONS[post_id] = post

            if type_post == "2":
                if post_id in ANSWERS:
                    # We found an answer
                    question_id = ANSWERS[post_id]
                    uploading_post = QUESTIONS[question_id]

                    payload = CreateResourcePayload()
                    payload.title = uploading_post.title
                    payload.origin = Origin()
                    payload.origin.tags = uploading_post.tags
                    payload.icon = "text/conversation"
                    payload.metadata = InputMetadata()
                    payload.metadata.language = "en"
                    payload.slug = post_id

                    self.count += 1
                    if self.count % 50 == 0:
                        print(f"{self.count} {datetime.now()}")
                    if self.count == 500_000:
                        print("DONE")
                        exit(0)

                    imq = InputMessage(
                        timestamp=uploading_post.creation,
                        who=uploading_post.who,
                        ident=question_id,
                        content=InputMessageContent(text=uploading_post.body),
                    )

                    ima = InputMessage(
                        timestamp=post.creation,
                        who=post.who,
                        ident=post_id,
                        content=InputMessageContent(text=post.body),
                    )

                    icf = InputConversationField()
                    icf.messages.append(imq)
                    icf.messages.append(ima)
                    payload.conversations["stack"] = icf
                    resource = kb.create_resource(payload)
                    vector = Vector(
                        start=0,
                        end=len(uploading_post.body),
                        start_paragraph=0,
                        end_paragraph=len(uploading_post.body),
                    )
                    vector.vector.extend(uploading_post.vectors)

                    resource.add_vectors(
                        "stack",
                        FieldType.CONVERSATION,
                        [vector],
                        split=question_id,
                    )

                    vector = Vector(
                        start=0,
                        end=len(post.body),
                        start_paragraph=0,
                        end_paragraph=len(post.body),
                    )
                    vector.vector.extend(post.vectors)

                    resource.add_vectors(
                        "stack",
                        FieldType.CONVERSATION,
                        [vector],
                    )
                    resource.add_text(
                        "stack",
                        FieldType.CONVERSATION,
                        uploading_post.body,
                        split=question_id,
                    )
                    resource.add_text(
                        "stack", FieldType.CONVERSATION, post.body, split=post_id
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

    parser = xml.sax.make_parser()
    parser.setContentHandler(StreamHandler())
    # if you can provide a file-like object it's as simple as
    with open(args.dump) as f:
        parser.parse(f)
