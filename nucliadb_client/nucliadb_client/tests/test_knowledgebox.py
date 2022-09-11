from nucliadb_client.client import NucliaDBClient


def test_knowledgebox_creation(nucliadb_client: NucliaDBClient):
    kb = nucliadb_client.create_kb(
        title="My KB", description="Its a new KB", slug="mykb"
    )
    info = kb.get()
    info.slug == "mykb"
    info.config.title == "My KB"
    info.config.description == "Its a new KB"

    assert kb.delete()
    info = kb.get()
