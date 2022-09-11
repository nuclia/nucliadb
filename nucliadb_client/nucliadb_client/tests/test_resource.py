from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
    MessageFormat,
)
from nucliadb_models.datetime import FieldDatetime
from nucliadb_models.text import TextField
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_client.knowledgebox import KnowledgeBox
from datetime import datetime
from uuid import uuid4


def test_resource_creation(nucliadb_knowledgebox: KnowledgeBox):
    payload = CreateResourcePayload()
    payload.icon = "plain/text"
    payload.title = "My Resource"
    payload.summary = "My long summary of the resource"
    payload.slug = "myresource"

    payload.conversations["conv1"] = InputConversationField(
        message=[
            InputMessage(
                timestamp=datetime.now(),
                who="myself",
                to=["you", "he"],
                content=InputMessageContent(
                    text="Hello how are you ?", format=MessageFormat.PLAIN
                ),
                ident=uuid4().hex,
            ),
            InputMessage(
                timestamp=datetime.now(),
                who="you",
                to=["myself", "he"],
                content=InputMessageContent(
                    text="Superlekker!", format=MessageFormat.PLAIN
                ),
                ident=uuid4().hex,
            ),
        ]
    )

    payload.texts["text1"] = TextField(body="My lovely text")
    payload.texts["text2"] = TextField(body="My second lovely text")

    payload.datetimes["date1"] = FieldDatetime(value=datetime.now())
    payload.datetimes["date2"] = FieldDatetime(value=datetime.now())

    res = nucliadb_knowledgebox.create_resource(payload)

    info = res.get(values=True)
    assert info.title == "My Resource"
