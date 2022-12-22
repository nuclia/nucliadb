from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.labels import LabelType
import pytest
from nucliadb_sdk.labels import Label, Labels


@pytest.fixture(scope="function")
def upload_data_field_classification(knowledgebox: KnowledgeBox):
    knowledgebox.set_labels("labelset1", ["label1", "label2"], LabelType.RESOURCES)
    knowledgebox.upload(
        "doc1", text="This is my lovely text", labels=["labelset1/label1"]
    )
    knowledgebox.upload(
        "doc2",
        text="This is my lovely text2",
        labels=["labelset1/label1", "labelset1/label2"],
    )
