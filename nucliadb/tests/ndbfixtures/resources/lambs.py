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
#

import base64
import random

from google.protobuf.json_format import ParseDict
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.writer.api.v1.router import KB_PREFIX
from nucliadb_models.conversation import (
    MessageType,
)
from nucliadb_protos.resources_pb2 import FieldType, Paragraph
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources._vectors import (
    adjust_kb_vectorsets,
    lambs_split_2_vector,
    lambs_split_3_vector,
    lambs_split_4_vector,
    lambs_split_5_vector,
    lambs_split_6_vector,
    lambs_split_7_vector,
    lambs_split_9_vector,
    lambs_split_10_vector,
    lambs_split_11_vector,
    lambs_split_12_vector,
    lambs_summary_vector,
    lambs_title_vector,
)
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync


async def lambs_resource(
    kbid: str,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
) -> str:
    """Resource with a conversation between Clarice and Dr. Lecter.

    TODO(decoupled-ask): more details about the resource, attachments...

    TL;DR

    In order to test with more realistic examples, this processor broker message
    has been generated using stage's learning processor.

    To recreate, use a local nucliadb with a breakpoint in ingest Processor.
    POST to /resources (as above) and wait for the processing broker message to
    arrive. Once received, use google.protobuf.json_format.MessageToDict to
    generate a Python dict for the message. Copy and paste it here.

    In this test we then use google.protobuf.json_format.ParseDict to build the
    protobuffer again.

    With time, BrokerMessage fields may change, feel free to replace this
    messages with new ones if needed.

    """

    slug = "lambs"
    conversation_field_id = "lambs"

    clarice = "Clarice"
    lecter = "Dr. Lecter"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "slug": slug,
            "title": "The silence of the lambs",
            "summary": "Short phone call dialogue between Clarice and Dr. Lecter from pages 142-143 of the script",
            "metadata": {
                "language": "en",
            },
            "usermetadata": {
                "classifications": [
                    {"labelset": "art", "label": "film"},
                    {"labelset": "genre", "label": "psychological"},
                    {"labelset": "genre", "label": "horror"},
                    {"labelset": "genre", "label": "thriller"},
                ]
            },
            "conversations": {
                conversation_field_id: {
                    "messages": [
                        {
                            "ident": "1",
                            "who": clarice,
                            "to": [lecter],
                            "content": {
                                "text": "Starling.",
                            },
                        },
                        {
                            "ident": "2",
                            "who": lecter,
                            "to": [clarice],
                            "content": {
                                "text": "Well, Clarice, have the lambs stopped screaming...?",
                                "attachments_fields": [
                                    {
                                        "field_type": "file",
                                        "field_id": "attachment:lamb",
                                    },
                                ],
                            },
                            "type": MessageType.QUESTION.value,
                        },
                        {
                            "ident": "3",
                            "who": lecter,
                            "to": [clarice],
                            "content": {
                                "text": "Don't bother with a trace, I won't be on long enough.",
                            },
                        },
                        {
                            "ident": "4",
                            "who": clarice,
                            "to": [lecter],
                            "content": {
                                "text": "Where are you, Dr. Lecter?",
                            },
                            "type": MessageType.QUESTION.value,
                        },
                        {
                            "ident": "5",
                            "who": lecter,
                            "to": [clarice],
                            "content": {
                                "text": "Where I have a view, Clarice...",
                            },
                            "type": MessageType.ANSWER.value,
                        },
                        {
                            "ident": "6",
                            "who": lecter,
                            "to": [clarice],
                            "content": {
                                "text": "Orion is looking splendid tonight, and Arcturus, the Herdsman, with his flock...",
                            },
                        },
                        {
                            "ident": "7",
                            "who": lecter,
                            "to": [clarice],
                            "content": {
                                "text": "Your lambs are still for now, Clarice, but not forever... You'll have to earn it again and again, this blessed silence. Because it's the plight that drives you, and the plight will never end.",
                            },
                        },
                        {
                            "ident": "8",
                            "who": clarice,
                            "to": [lecter],
                            "content": {
                                "text": "Dr. Lecter -",
                            },
                        },
                        {
                            "ident": "9",
                            "who": lecter,
                            "to": [clarice],
                            "content": {
                                "text": "I have no plans to call on you, Clarice, the world being more interesting with you in it. Be sure you extend me the same courtesy.",
                            },
                        },
                        {
                            "ident": "10",
                            "who": clarice,
                            "to": [lecter],
                            "content": {
                                "text": "You know I can't make that promise.",
                            },
                        },
                        {
                            "ident": "11",
                            "who": lecter,
                            "to": [clarice],
                            "content": {
                                "text": "Goodbye, Clarice... You looked - so very lovely today in your blue suit.",
                                "attachments_fields": [
                                    {
                                        "field_type": "file",
                                        "field_id": "attachment:blue-suit",
                                    },
                                ],
                            },
                        },
                        {
                            "ident": "12",
                            "who": clarice,
                            "to": [lecter],
                            "content": {
                                "text": "Dr. Lecter... Dr. Lecter...!",
                            },
                        },
                    ]
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    processor_bm = BrokerMessage()
    ParseDict(
        {
            "kbid": kbid,
            "uuid": rid,
            "slug": "lambs",
            "extractedText": [
                {
                    "body": {
                        "splitText": {
                            "5": "Where I have a view, Clarice...",
                            "11": "Goodbye, Clarice... You looked - so very lovely today in your blue suit.",
                            "8": "Dr. Lecter -",
                            "2": "Well, Clarice, have the lambs stopped screaming...?",
                            "3": "Don't bother with a trace, I won't be on long enough.",
                            "1": "Starling.",
                            "9": "I have no plans to call on you, Clarice, the world being more interesting with you in it. Be sure you extend me the same courtesy.",
                            "7": "Your lambs are still for now, Clarice, but not forever... You'll have to earn it again and again, this blessed silence. Because it's the plight that drives you, and the plight will never end.",
                            "4": "Where are you, Dr. Lecter?",
                            "6": "Orion is looking splendid tonight, and Arcturus, the Herdsman, with his flock...",
                            "12": "Dr. Lecter... Dr. Lecter...!",
                            "10": "You know I can't make that promise.",
                        }
                    },
                    "field": {"fieldType": "CONVERSATION", "field": "lambs"},
                },
                {
                    "body": {"text": "The silence of the lambs"},
                    "field": {"fieldType": "GENERIC", "field": "title"},
                },
                {
                    "body": {
                        "text": "Short phone call dialogue between Clarice and Dr. Lecter from pages 142-143 of the script"
                    },
                    "field": {"fieldType": "GENERIC", "field": "summary"},
                },
            ],
            "fieldMetadata": [
                {
                    "metadata": {
                        "metadata": {"lastProcessingStart": "2025-12-09T15:25:56.670724Z"},
                        "splitMetadata": {
                            "5": {
                                "paragraphs": [{"end": 31, "sentences": [{"end": 31}]}],
                                "lastUnderstanding": "2025-12-09T15:25:57.452082Z",
                                "lastExtract": "2025-12-09T15:25:57.377092Z",
                                "language": "en",
                                "entities": {
                                    "processor": {
                                        "entities": [
                                            {
                                                "text": "Clarice",
                                                "label": "PERSON",
                                                "positions": [{"start": "21", "end": "28"}],
                                            }
                                        ]
                                    }
                                },
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "11": {
                                "paragraphs": [
                                    {"end": 72, "sentences": [{"end": 20}, {"start": 20, "end": 72}]}
                                ],
                                "lastUnderstanding": "2025-12-09T15:25:58.551028Z",
                                "lastExtract": "2025-12-09T15:25:58.417291Z",
                                "language": "en",
                                "entities": {
                                    "processor": {
                                        "entities": [
                                            {
                                                "text": "Clarice",
                                                "label": "PERSON",
                                                "positions": [{"start": "9", "end": "16"}],
                                            },
                                            {
                                                "text": "today",
                                                "label": "DATE",
                                                "positions": [{"start": "48", "end": "53"}],
                                            },
                                        ]
                                    }
                                },
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "8": {
                                "paragraphs": [
                                    {"end": 12, "sentences": [{"end": 4}, {"start": 4, "end": 12}]}
                                ],
                                "lastUnderstanding": "2025-12-09T15:25:58.188974Z",
                                "lastExtract": "2025-12-09T15:25:58.080976Z",
                                "language": "fr",
                                "entities": {"processor": {}},
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "2": {
                                "paragraphs": [{"end": 51, "sentences": [{"end": 51}]}],
                                "lastUnderstanding": "2025-12-09T15:25:57.557541Z",
                                "lastExtract": "2025-12-09T15:25:57.473878Z",
                                "language": "en",
                                "entities": {
                                    "processor": {
                                        "entities": [
                                            {
                                                "text": "Clarice",
                                                "label": "PERSON",
                                                "positions": [{"start": "6", "end": "13"}],
                                            }
                                        ]
                                    }
                                },
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "3": {
                                "paragraphs": [{"end": 53, "sentences": [{"end": 53}]}],
                                "lastUnderstanding": "2025-12-09T15:25:58.062501Z",
                                "lastExtract": "2025-12-09T15:25:57.986601Z",
                                "language": "en",
                                "entities": {"processor": {}},
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "1": {
                                "paragraphs": [{"end": 9, "sentences": [{"end": 9}]}],
                                "lastUnderstanding": "2025-12-09T15:25:57.633157Z",
                                "lastExtract": "2025-12-09T15:25:57.573873Z",
                                "language": "tl",
                                "entities": {"processor": {}},
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "9": {
                                "paragraphs": [
                                    {"end": 130, "sentences": [{"end": 90}, {"start": 90, "end": 130}]}
                                ],
                                "lastUnderstanding": "2025-12-09T15:25:57.242862Z",
                                "lastExtract": "2025-12-09T15:25:57.107817Z",
                                "language": "en",
                                "entities": {
                                    "processor": {
                                        "entities": [
                                            {
                                                "text": "Clarice",
                                                "label": "PERSON",
                                                "positions": [{"start": "32", "end": "39"}],
                                            }
                                        ]
                                    }
                                },
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "7": {
                                "paragraphs": [
                                    {
                                        "end": 191,
                                        "sentences": [
                                            {"end": 58},
                                            {"start": 58, "end": 120},
                                            {"start": 120, "end": 191},
                                        ],
                                    }
                                ],
                                "lastUnderstanding": "2025-12-09T15:25:58.397684Z",
                                "lastExtract": "2025-12-09T15:25:58.206236Z",
                                "language": "en",
                                "entities": {
                                    "processor": {
                                        "entities": [
                                            {
                                                "text": "Clarice",
                                                "label": "PERSON",
                                                "positions": [{"start": "30", "end": "37"}],
                                            }
                                        ]
                                    }
                                },
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "4": {
                                "paragraphs": [{"end": 26, "sentences": [{"end": 26}]}],
                                "lastUnderstanding": "2025-12-09T15:25:57.967421Z",
                                "lastExtract": "2025-12-09T15:25:57.894928Z",
                                "language": "en",
                                "entities": {
                                    "processor": {
                                        "entities": [
                                            {
                                                "text": "Lecter",
                                                "label": "PERSON",
                                                "positions": [{"start": "19", "end": "25"}],
                                            }
                                        ]
                                    }
                                },
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "6": {
                                "paragraphs": [{"end": 80, "sentences": [{"end": 80}]}],
                                "lastUnderstanding": "2025-12-09T15:25:57.357954Z",
                                "lastExtract": "2025-12-09T15:25:57.270168Z",
                                "language": "en",
                                "relations": [
                                    {
                                        "relations": [
                                            {
                                                "relation": "OTHER",
                                                "source": {"value": "tonight", "subtype": "TIME"},
                                                "to": {"value": "Arcturus", "subtype": "PERSON"},
                                                "relationLabel": "participant",
                                                "metadata": {
                                                    "paragraphId": "9425ef00effb4c1cb60ea4a7c2d0615b/c/lambs/6/0-80",
                                                    "sourceStart": 26,
                                                    "sourceEnd": 33,
                                                    "toStart": 39,
                                                    "toEnd": 47,
                                                },
                                            },
                                            {
                                                "relation": "OTHER",
                                                "source": {"value": "tonight", "subtype": "TIME"},
                                                "to": {"value": "Herdsman", "subtype": "PERSON"},
                                                "relationLabel": "participant",
                                                "metadata": {
                                                    "paragraphId": "9425ef00effb4c1cb60ea4a7c2d0615b/c/lambs/6/0-80",
                                                    "sourceStart": 26,
                                                    "sourceEnd": 33,
                                                    "toStart": 53,
                                                    "toEnd": 61,
                                                },
                                            },
                                        ]
                                    }
                                ],
                                "entities": {
                                    "processor": {
                                        "entities": [
                                            {
                                                "text": "Orion",
                                                "label": "PERSON",
                                                "positions": [{"end": "5"}],
                                            },
                                            {
                                                "text": "tonight",
                                                "label": "TIME",
                                                "positions": [{"start": "26", "end": "33"}],
                                            },
                                            {
                                                "text": "Arcturus",
                                                "label": "PERSON",
                                                "positions": [{"start": "39", "end": "47"}],
                                            },
                                            {
                                                "text": "Herdsman",
                                                "label": "PERSON",
                                                "positions": [{"start": "53", "end": "61"}],
                                            },
                                        ]
                                    }
                                },
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "12": {
                                "paragraphs": [
                                    {
                                        "end": 28,
                                        "sentences": [
                                            {"end": 4},
                                            {"start": 4, "end": 14},
                                            {"start": 14, "end": 18},
                                            {"start": 18, "end": 28},
                                        ],
                                    }
                                ],
                                "lastUnderstanding": "2025-12-09T15:25:57.877861Z",
                                "lastExtract": "2025-12-09T15:25:57.649982Z",
                                "language": "fr",
                                "entities": {"processor": {}},
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                            "10": {
                                "paragraphs": [{"end": 35, "sentences": [{"end": 35}]}],
                                "lastUnderstanding": "2025-12-09T15:25:57.080101Z",
                                "lastExtract": "2025-12-09T15:25:56.979291Z",
                                "language": "en",
                                "entities": {"processor": {}},
                                "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                            },
                        },
                    },
                    "field": {"fieldType": "CONVERSATION", "field": "lambs"},
                },
                {
                    "metadata": {
                        "metadata": {
                            "paragraphs": [{"end": 24, "sentences": [{"end": 24}]}],
                            "lastUnderstanding": "2025-12-09T15:25:58.647289Z",
                            "lastExtract": "2025-12-09T15:25:58.575339Z",
                            "language": "en",
                            "mimeType": "text/plain",
                            "entities": {
                                "processor": {
                                    "entities": [
                                        {
                                            "text": "The silence of the lambs",
                                            "label": "PERSON",
                                            "positions": [{"end": "24"}],
                                        }
                                    ]
                                }
                            },
                            "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                        }
                    },
                    "field": {"fieldType": "GENERIC", "field": "title"},
                },
                {
                    "metadata": {
                        "metadata": {
                            "paragraphs": [{"end": 89, "sentences": [{"end": 89}]}],
                            "lastUnderstanding": "2025-12-09T15:25:58.749312Z",
                            "lastExtract": "2025-12-09T15:25:58.669744Z",
                            "language": "en",
                            "relations": [
                                {
                                    "relations": [
                                        {
                                            "relation": "OTHER",
                                            "source": {"value": "Clarice", "subtype": "PERSON"},
                                            "to": {"value": "Lecter", "subtype": "PERSON"},
                                            "relationLabel": "characters",
                                            "metadata": {
                                                "paragraphId": "9425ef00effb4c1cb60ea4a7c2d0615b/a/summary/0-89",
                                                "sourceStart": 34,
                                                "sourceEnd": 41,
                                                "toStart": 50,
                                                "toEnd": 56,
                                            },
                                        }
                                    ]
                                }
                            ],
                            "mimeType": "text/plain",
                            "entities": {
                                "processor": {
                                    "entities": [
                                        {
                                            "text": "Clarice",
                                            "label": "PERSON",
                                            "positions": [{"start": "34", "end": "41"}],
                                        },
                                        {
                                            "text": "Lecter",
                                            "label": "PERSON",
                                            "positions": [{"start": "50", "end": "56"}],
                                        },
                                    ]
                                }
                            },
                            "lastProcessingStart": "2025-12-09T15:25:56.670724Z",
                        }
                    },
                    "field": {"fieldType": "GENERIC", "field": "summary"},
                },
            ],
            "fieldVectors": [
                {
                    "vectors": {
                        "splitVectors": {
                            "5": {
                                "vectors": [
                                    {
                                        "end": 31,
                                        "endParagraph": 31,
                                        "vector": lambs_split_5_vector,
                                    }
                                ]
                            },
                            "11": {
                                "vectors": [
                                    {
                                        "end": 72,
                                        "endParagraph": 72,
                                        "vector": lambs_split_11_vector,
                                    }
                                ]
                            },
                            "8": {},
                            "2": {
                                "vectors": [
                                    {
                                        "end": 51,
                                        "endParagraph": 51,
                                        "vector": lambs_split_2_vector,
                                    }
                                ]
                            },
                            "3": {
                                "vectors": [
                                    {
                                        "end": 53,
                                        "endParagraph": 53,
                                        "vector": lambs_split_3_vector,
                                    }
                                ]
                            },
                            "1": {},
                            "9": {
                                "vectors": [
                                    {
                                        "end": 130,
                                        "endParagraph": 130,
                                        "vector": lambs_split_9_vector,
                                    }
                                ]
                            },
                            "7": {
                                "vectors": [
                                    {
                                        "end": 191,
                                        "endParagraph": 191,
                                        "vector": lambs_split_7_vector,
                                    }
                                ]
                            },
                            "4": {
                                "vectors": [
                                    {
                                        "end": 26,
                                        "endParagraph": 26,
                                        "vector": lambs_split_4_vector,
                                    }
                                ]
                            },
                            "6": {
                                "vectors": [
                                    {
                                        "end": 80,
                                        "endParagraph": 80,
                                        "vector": lambs_split_6_vector,
                                    }
                                ]
                            },
                            "12": {
                                "vectors": [
                                    {
                                        "end": 28,
                                        "endParagraph": 28,
                                        "vector": lambs_split_12_vector,
                                    }
                                ]
                            },
                            "10": {
                                "vectors": [
                                    {
                                        "end": 35,
                                        "endParagraph": 35,
                                        "vector": lambs_split_10_vector,
                                    }
                                ]
                            },
                        }
                    },
                    "field": {"fieldType": "CONVERSATION", "field": "lambs"},
                    "vectorsetId": "en-2024-04-24",
                },
                {
                    "vectors": {
                        "vectors": {
                            "vectors": [
                                {
                                    "end": 24,
                                    "endParagraph": 24,
                                    "vector": lambs_title_vector,
                                }
                            ]
                        }
                    },
                    "field": {"fieldType": "GENERIC", "field": "title"},
                    "vectorsetId": "en-2024-04-24",
                },
                {
                    "vectors": {
                        "vectors": {
                            "vectors": [
                                {
                                    "end": 89,
                                    "endParagraph": 89,
                                    "vector": lambs_summary_vector,
                                }
                            ]
                        }
                    },
                    "field": {"fieldType": "GENERIC", "field": "summary"},
                    "vectorsetId": "en-2024-04-24",
                },
            ],
            "doneTime": "2025-12-09T15:25:58.750653Z",
            "processingId": "45870135-5e7e-4b80-9ca5-b35be5ccf2f0",
            "source": "PROCESSOR",
            "questionAnswers": [
                {"field": {"fieldType": "CONVERSATION", "field": "lambs"}},
                {"field": {"fieldType": "GENERIC", "field": "title"}},
                {"field": {"fieldType": "GENERIC", "field": "summary"}},
            ],
            "generatedBy": [{"processor": {}}],
        },
        processor_bm,
    )

    await adjust_kb_vectorsets(kbid, processor_bm)

    # ingest the processed BM
    await inject_message(nucliadb_ingest_grpc, processor_bm)
    await wait_for_sync()

    # now, we'll patch the resource, add the attachments and generate a broker
    # message from processor to add extracted data.
    #
    # We could do it in a single POST but this makes it easier to reproduce the
    # processor bm
    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/resource/{rid}",
        json={
            "files": {
                "attachment:lamb": {
                    "file": {
                        "filename": "lamb.png",
                        "content_type": "image/png",
                        "payload": base64.b64encode(b"some content we're not going to check").decode(),
                    }
                },
                "attachment:blue-suit": {
                    "file": {
                        "filename": "clarice-blue-suit.pdf",
                        "content_type": "application/pdf",
                        "payload": base64.b64encode(b"some content we're not going to check").decode(),
                    }
                },
            }
        },
    )
    assert resp.status_code == 200

    vectorsets = {}
    async with datamanagers.with_ro_transaction() as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vectorsets[vectorset_id] = vs
    # use a controlled random seed for vector generation
    random.seed(32)

    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        slug=slug,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )

    file_field = bmb.field_builder("attachment:lamb", FieldType.FILE)
    file_field.add_paragraph(
        "A beautiful picture of some happy lambs in the middle of a green field",
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
        kind=Paragraph.TypeParagraph.INCEPTION,
    )

    file_field = bmb.field_builder("attachment:blue-suit", FieldType.FILE)
    file_field.add_paragraph(
        "Clarice's blue suit",
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
        kind=Paragraph.TypeParagraph.TEXT,
    )

    attachments_bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, attachments_bm)
    await wait_for_sync()

    return rid
