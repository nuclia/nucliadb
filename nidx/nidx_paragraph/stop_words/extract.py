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
import os
import json

import nltk

iso_codes = {
    "azerbaijani": "az",
    "arabic": "ar",
    "basque": "eu",
    "bengali": "bn",
    "catalan": "ca",
    "chinese": "ch",
    "czech": "cs",
    "danish": "da",
    "german": "de",
    "greek": "el",
    "english": "en",
    "spanish": "es",
    "finnish": "fi",
    "french": "fr",
    "hungarian": "hu",
    "hebrew": "he",
    "indonesian": "id",
    "icelandic": "is",
    "italian": "it",
    "nepali": "ne",
    "norwegian": "no",
    "kazakh": "kk",
    "latvian": "lv",
    "dutch": "nl",
    "polish": "pl",
    "portuguese": "pt",
    "romanian": "ro",
    "russian": "ru",
    "slovak": "sk",
    "slovenian": "sl",
    "slovene": "sl",
    "swedish": "sv",
    "tamil": "ta",
    "tajik": "tg",
    "turkish": "tr",
}


nltk.download("stopwords")
languages = nltk.corpus.stopwords.fileids()
output_dir = os.path.dirname(__file__)

for lang in languages:
    stop_words = nltk.corpus.stopwords.words(lang)
    if lang not in iso_codes:
        print(f"Can't find {lang}")
        continue
    iso_code = iso_codes[lang]
    output_file = os.path.join(output_dir, f"{iso_code}.json")

    with open(output_file, "w") as file:
        json.dump(stop_words, file)

    print(f"Stop words for {iso_code} saved to {output_file}")
