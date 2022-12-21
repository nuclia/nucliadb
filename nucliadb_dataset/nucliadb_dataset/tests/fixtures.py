from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.labels import LabelType
import pytest
from nucliadb_sdk.labels import Label, Labels


@pytest.fixture(scope="function")
def upload_data_resource_classification(knowledgebox: KnowledgeBox):
    knowledgebox.set_labels("labelset1", ["label1", "label2"], LabelType.RESOURCE)
    rid1 = knowledgebox.upload("doc1", text="This is my lovely text", labels=["label1"])
    rid2 = knowledgebox.upload("doc2", text="This is my lovely text", labels=["label1"])
