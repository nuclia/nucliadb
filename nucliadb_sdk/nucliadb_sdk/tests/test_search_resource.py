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

from typing import Any, Dict

import pytest
from datasets import load_dataset
from sentence_transformers import SentenceTransformer  # type: ignore

from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.labels import Label
from nucliadb_sdk.vectors import Vector


def test_search_resource(knowledgebox: KnowledgeBox):
    # Lets create a bunch of resources

    ds: Dict[str, Any] = load_dataset("tweet_eval", "emoji")  # type: ignore
    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    for index, train in enumerate(ds["train"]):
        if index == 50:
            break
        label = train["label"]
        knowledgebox.upload(
            text=train["text"],
            labels=[f"emoji/{label}"],
            vectors={"all-MiniLM-L6-v2": encoder.encode([train["text"]])[0].tolist()},
        )

    assert len(knowledgebox) == 50
    labels = knowledgebox.get_uploaded_labels()

    assert labels["emoji"].count == 50
    assert labels["emoji"].labels["0"] == 9

    resources = knowledgebox.search(text="love")
    assert resources.fulltext.total == 5
    assert len(resources.resources) == 5

    resources = knowledgebox.search(filter=[Label(labelset="emoji", label="0")])

    assert resources.fulltext.total == 9

    resources = knowledgebox.search(filter=["emoji/0"])

    assert resources.fulltext.total == 9

    vector_q = encoder.encode([ds["train"][0]["text"]])[0].tolist()
    resources = knowledgebox.search(
        vector=vector_q, vectorset="all-MiniLM-L6-v2", min_score=0.70
    )
    assert len(resources.sentences.results) == 1
    assert (
        "Sunday afternoon walking through Venice" in resources.sentences.results[0].text
    )


def test_search_resource_simple_label(knowledgebox: KnowledgeBox):
    # Lets create a bunch of resources

    ds: Dict[str, Any] = load_dataset("tweet_eval", "emoji")  # type: ignore
    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    for index, train in enumerate(ds["train"]):
        if index == 50:
            break
        label = train["label"]
        knowledgebox.upload(
            text=train["text"],
            labels=[str(label)],
            vectors={"all-MiniLM-L6-v2": encoder.encode([train["text"]])[0]},
        )

    assert len(knowledgebox) == 50
    labels = knowledgebox.get_uploaded_labels()

    assert labels["default"].count == 50
    assert labels["default"].labels["0"] == 9

    resources = knowledgebox.search(text="love")
    assert resources.fulltext.total == 5
    assert len(resources.resources) == 5

    resources = knowledgebox.search(filter=["12"])

    vector_q = encoder.encode([ds["train"][0]["text"]])[0]
    resources = knowledgebox.search(
        vector=vector_q,
        vectorset="all-MiniLM-L6-v2",
    )


@pytest.mark.asyncio
async def test_search_resource_async(knowledgebox: KnowledgeBox):

    knowledgebox.new_vectorset("all-MiniLM-L6-v2", 384)

    ds: Dict[str, Any] = load_dataset("tweet_eval", "emoji")  # type: ignore
    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    for index, train in enumerate(ds["train"]):
        if index == 50:
            break
        label = train["label"]
        await knowledgebox.async_upload(
            text=train["text"],
            labels=[Label(label=str(label), labelset="emoji")],
            vectors=[
                Vector(
                    value=encoder.encode([train["text"]])[0],
                    vectorset="all-MiniLM-L6-v2",
                )
            ],
        )

    assert len(knowledgebox) == 50
    labels = await knowledgebox.async_get_uploaded_labels()

    assert labels["emoji"].count == 50
    assert labels["emoji"].labels["0"] == 9

    resources = await knowledgebox.async_search(text="love")
    assert resources.fulltext.total == 5
    assert len(resources.resources) == 5

    resources = await knowledgebox.async_search(
        filter=[Label(labelset="emoji", label="12")]
    )

    vector_q = encoder.encode([ds["train"][0]["text"]])[0]
    resources = await knowledgebox.async_search(
        vector=vector_q,
        vectorset="all-MiniLM-L6-v2",
    )
