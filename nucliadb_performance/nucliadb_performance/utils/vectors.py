import httpx
from sentence_transformers import SentenceTransformer  # type: ignore

from .misc import cache_to_disk

ENCODER = None


def get_encoder():
    global ENCODER
    if ENCODER is not None:
        return ENCODER
    ENCODER = SentenceTransformer("all-MiniLM-L6-v2")
    return ENCODER


@cache_to_disk
def compute_vector(sentence: str) -> list[float]:
    encoder = get_encoder()
    return encoder.encode([sentence])[0].tolist()


@cache_to_disk
async def predict_sentence_to_vector(
    predict_url: str, kbid: str, sentence: str
) -> list[float]:
    client = httpx.AsyncClient(base_url=predict_url)
    resp = await client.get(
        "/sentence",
        headers={"X-STF-KBID": kbid},
        params={"text": sentence},
    )
    resp.raise_for_status()
    data = resp.json()
    return data["data"]
