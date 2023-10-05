from molotov import scenario
import nucliadb_sdk
from faker import Faker

fake = Faker()
REGION = "europe-1"
ENV = "stashify.cloud"
URL = None

def get_base_url():
    if URL is not None:
        return URL
    return f"https://{REGION}.{ENV}/api/v1"


def get_url(path):
    return get_base_url() + path


kbid = "13c0f7ee-cd27-4739-a41b-898653b5885b"


@scenario(weight=50)
async def test_find_default(session):
    find = get_url(f"/kb/{kbid}/find?query={fake.sentence()}")
    async with session.get(find) as resp:
        assert resp.status == 200, resp.status
