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
import re
from collections import OrderedDict

from nucliadb.train.generators.token_classifier import process_entities


def test_process_entities_simple():
    split_text = {"__main__": "This is a bird, its a plane, no, its el Super Fran"}
    split_ners = {"__main__": OrderedDict([((37, 50), ("PERSON", "el Super Fran"))])}
    split_paragaphs = {"__main__": []}
    entities = list(
        process_entities(
            split_text["__main__"], split_ners["__main__"], split_paragaphs["__main__"]
        )
    )
    assert entities == [
        [
            ("This", "O"),
            ("is", "O"),
            ("a", "O"),
            ("bird,", "O"),
            ("its", "O"),
            ("a", "O"),
            ("plane,", "O"),
            ("no,", "O"),
            ("its", "O"),
            ("el", "B-PERSON"),
            ("Super", "I-PERSON"),
            ("Fran", "I-PERSON"),
        ]
    ]


def test_process_entities():
    paragraphs = [
        "The Legend of Zelda is a video game franchise created by Japanese video game"
        "designers Shigeru Miyamoto and Takashi Tezuka.\n",
        "It is mainly developed and published by Nintendo.\n",
        "The universe of the Legend of Zelda series consists of a variety of lands, the"
        "most predominant being Hyrule.\n",
        "Most games in The Legend of Zelda series follow a similar storyline, which"
        "involves the protagonist Link battling monsters to save Princess Zelda"
        "and defeat an evil villain, which is often the series' main antagonist,"
        "Ganon.",
    ]
    text = "".join(paragraphs)
    paragraph_positions = []
    for paragraph in paragraphs:
        start = text.index(paragraph)
        end = start + len(paragraph) - 1
        paragraph_positions.append((start, end))

    print("POSITIONS")
    print(paragraph_positions)

    ners = [
        ("GAME", "The Legend of Zelda"),
        ("PERSON", "Shigeru Miyamoto"),
        ("PERSON", "Takashi Tezuka"),
        ("COMPANY", "Nintendo"),
        ("CHARACTER", "Princess Zelda"),
        ("CHARACTER", "Link"),
        ("CHARACTER", "Ganon"),
        ("PLACE", "Hyrule"),
    ]

    located_ners = {}
    for kind, name in ners:
        for match in re.finditer(name, text):
            position = (match.start(), match.end())
            located_ners[position] = (kind, name)
    located_ners = OrderedDict(located_ners)
    print(located_ners)

    expected_segments = [
        [
            ("The", "B-GAME"),
            ("Legend", "I-GAME"),
            ("of", "I-GAME"),
            ("Zelda", "I-GAME"),
            ("is", "O"),
            ("a", "O"),
            ("video", "O"),
            ("game", "O"),
            ("franchise", "O"),
            ("created", "O"),
            ("by", "O"),
            ("Japanese", "O"),
            ("video", "O"),
            ("gamedesigners", "O"),
            ("Shigeru", "B-PERSON"),
            ("Miyamoto", "I-PERSON"),
            ("and", "O"),
            ("Takashi", "B-PERSON"),
            ("Tezuka", "I-PERSON"),
            (".", "O"),
        ],
        [
            ("It", "O"),
            ("is", "O"),
            ("mainly", "O"),
            ("developed", "O"),
            ("and", "O"),
            ("published", "O"),
            ("by", "O"),
            ("Nintendo", "B-COMPANY"),
            (".", "O"),
        ],
        [
            ("The", "O"),
            ("universe", "O"),
            ("of", "O"),
            ("the", "O"),
            ("Legend", "O"),
            ("of", "O"),
            ("Zelda", "O"),
            ("series", "O"),
            ("consists", "O"),
            ("of", "O"),
            ("a", "O"),
            ("variety", "O"),
            ("of", "O"),
            ("lands,", "O"),
            ("themost", "O"),
            ("predominant", "O"),
            ("being", "O"),
            ("Hyrule", "B-PLACE"),
            (".", "O"),
        ],
        [
            ("Most", "O"),
            ("games", "O"),
            ("in", "O"),
            ("The", "B-GAME"),
            ("Legend", "I-GAME"),
            ("of", "I-GAME"),
            ("Zelda", "I-GAME"),
            ("series", "O"),
            ("follow", "O"),
            ("a", "O"),
            ("similar", "O"),
            ("storyline,", "O"),
            ("whichinvolves", "O"),
            ("the", "O"),
            ("protagonist", "O"),
            ("Link", "O"),
            ("battling", "O"),
            ("monsters", "O"),
            ("to", "O"),
            ("save", "O"),
            ("Princess", "B-CHARACTER"),
            ("Zelda", "I-CHARACTER"),
            ("and", "O"),
            ("defeat", "O"),
            ("an", "O"),
            ("evil", "O"),
            ("villain,", "O"),
            ("which", "O"),
            ("is", "O"),
            ("often", "O"),
            ("the", "O"),
            ("series'", "O"),
            ("main", "O"),
            ("antagonist,", "O"),
            ("Ganon", "B-CHARACTER"),
        ],
    ]

    segments = list(process_entities(text, located_ners, paragraph_positions))
    assert segments == expected_segments
