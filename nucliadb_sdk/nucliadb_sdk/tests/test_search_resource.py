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

    resources = knowledgebox.search(filter=["emoji/0"])

    vector_q = encoder.encode([ds["train"][0]["text"]])[0].tolist()
    resources = knowledgebox.search(vector=vector_q, vectorset="all-MiniLM-L6-v2")
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
