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
