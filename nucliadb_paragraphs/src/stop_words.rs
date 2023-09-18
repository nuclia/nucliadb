// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;

use lazy_static::lazy_static;
use lingua::Language;
use lingua::Language::English;
use lingua::{LanguageDetector, LanguageDetectorBuilder};
use serde_json;

lazy_static! {
    static ref LANGUAGE_DETECTOR: LanguageDetector = {
        let detector = LanguageDetectorBuilder::from_all_spoken_languages()
            .with_preloaded_language_models()
            .build();
        detector
    };
}

struct StopWords {
    map: HashMap<String, HashSet<String>>,
}

/// HashMap with the stop words for each language
impl StopWords {
    fn new() -> StopWords {
        StopWords {
            map: HashMap::new(),
        }
    }

    /// LOads the stop words for the given language, from a JSON file
    fn load_language(&mut self, language_code: &str, json_data: &str) -> Result<(), String> {
        let stop_words: HashSet<String> = serde_json::from_str(json_data)
            .map_err(|err| format!("Error parsing stop words for {}: {}", language_code, err))?;
        self.map.insert(language_code.to_string(), stop_words);
        Ok(())
    }

    /// Returns true if `word` is a stop word in `language_code`
    fn is_stop_word(&self, word: &str, language_code: &str) -> bool {
        if let Some(stop_words) = self.map.get(language_code) {
            return stop_words.contains(word);
        }
        false // Language not found
    }
}

// Loads all JSON files as resources
static FR: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/fr.json"));
static IT: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/it.json"));
static ES: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/es.json"));
static EN: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/en.json"));
static CA: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/ca.json"));
static DE: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/de.json"));
static NL: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/nl.json"));
static PT: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/stop_words/pt.json"));
static LOADED_LANGUAGES: [[&str; 2]; 8] = [
    ["fr", FR],
    ["it", IT],
    ["es", ES],
    ["en", EN],
    ["ca", CA],
    ["de", DE],
    ["nl", NL],
    ["pt", PT],
];

lazy_static! {
    static ref STOP_WORDS: StopWords = {
        let mut stop_words = StopWords::new();

        for loaded in &LOADED_LANGUAGES {
            let code = loaded[0];
            let data = loaded[1];
            if let Err(err) = stop_words.load_language(code, data) {
                eprintln!("Error loading stop words for {}: {}", code, err);
            }
        }

        stop_words
    };
}

/// Returns the detected language, defaults to English
pub fn detect_language(query: &str) -> Language {
    let detected_language: Option<Language> = LANGUAGE_DETECTOR.detect_language_of(query);

    detected_language.unwrap_or(English)
}

/// Returns `true` if the word is a stop word
pub fn is_stop_word(x: &str, lang: Option<Language>) -> bool {
    let lang = lang.unwrap_or(English);

    STOP_WORDS.is_stop_word(x, lang.iso_code_639_1().to_string().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lingua::Language;

    #[test]
    fn it_detects_language() {
        // cache warm up
        let start_time = std::time::Instant::now();
        let _ = detect_language("I am just here to lazy-build the detector");
        let elapsed = start_time.elapsed().as_millis() as f64;
        // make sure we never spend more than 500 ms for the cache warmup
        // TODO: is this ok? should we preload at startup so the first call is not slow?
        assert_eq!(elapsed < 500.0, true, "{}", elapsed);

        let tests = [
            (
                "nuclia is a database for unstructured data",
                Language::English,
            ),
            ("nuclia is a database for the", Language::English),
            ("is a for and", Language::English),
            ("what does stop is?", Language::English),
            ("", Language::English),
            (
                "comment s'appelle le train à grande vitesse",
                Language::French,
            ),
            (
                "¿Qué significa la palabra sentence en español?",
                Language::Spanish,
            ),
            (
                "Per què les vaques no són de color rosa?",
                Language::Catalan,
            ),
        ];

        for (query, expected_language) in tests {
            let start_time = std::time::Instant::now();
            let detected_language = detect_language(query);
            let elapsed = start_time.elapsed().as_micros() as f64;
            assert_eq!(expected_language, detected_language);
            // make sure we never spend more than ~1 ms
            // setting it to 4ms for slow CI boxes
            assert_eq!(elapsed < 4000.0, true, "{}", elapsed);
        }
    }

    #[test]
    fn it_finds_stop_words() {
        // cache warm up
        let start_time = std::time::Instant::now();
        let _ = is_stop_word("detector", Some(Language::English));
        let elapsed = start_time.elapsed().as_millis() as f64;
        // make sure we never spend more than 100 ms for the cache warmup
        assert_eq!(elapsed < 100.0, true, "{}", elapsed);

        let tests = [
            ("nuclia", Language::English, false),
            ("is", Language::English, true),
            ("le", Language::French, true),
            ("el", Language::Spanish, true),
            ("stop", Language::French, false),
            ("stop", Language::English, false),
        ];

        for (word, lang, expected) in tests {
            let start_time = std::time::Instant::now();
            let matches = is_stop_word(word, Some(lang));
            let elapsed = start_time.elapsed().as_micros() as f64;
            assert_eq!(matches, expected);
            // make sure we never spend more than 1 ms
            assert_eq!(elapsed < 1000.0, true, "{}", elapsed);
        }
    }
}
