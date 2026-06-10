# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import os

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
