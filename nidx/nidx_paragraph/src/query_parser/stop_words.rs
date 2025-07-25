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
use std::collections::HashSet;
use std::env;

use lazy_static::lazy_static;

use super::Token;

lazy_static! {
    static ref STOP_WORDS: StopWords = build_stop_words();
}

#[inline]
fn build_stop_words() -> StopWords {
    let mut stop_words = StopWords::new();

    for loaded in &LOADED_LANGUAGES {
        let code = loaded[0];
        let data = loaded[1];
        if let Err(err) = stop_words.load_language(code, data) {
            eprintln!("Error loading stop words for {}: {}", code, err);
        }
    }

    stop_words
}

struct StopWords {
    words: HashSet<String>,
}

/// HashMap with the stop words for each language
impl StopWords {
    fn new() -> StopWords {
        StopWords { words: HashSet::new() }
    }

    /// LOads the stop words for the given language, from a JSON file
    fn load_language(&mut self, language_code: &str, json_data: &str) -> Result<(), String> {
        let stop_words: HashSet<String> = serde_json::from_str(json_data)
            .map_err(|err| format!("Error parsing stop words for {}: {}", language_code, err))?;
        self.words.extend(stop_words);
        Ok(())
    }

    /// Returns true if `word` is a stop word in `language_code`
    fn is_stop_word(&self, word: &str) -> bool {
        self.words.contains(word)
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

/// Returns `true` if the word is a stop word
pub fn is_stop_word(word: &str) -> bool {
    STOP_WORDS.is_stop_word(word)
}

/// Consume a tokenized query and filter out any stop words from it.
///
/// A stop words is any literal matching the following criteria:
/// - It is present in the given list of stop words
/// - Is **not** the last term in the query
///
/// The last term of the query can be used as fuzzy prefix and must be preserved.
pub fn remove_stop_words(mut query: Vec<Token>) -> Vec<Token> {
    if query.is_empty() {
        return query;
    }
    // as we don't want to remove the last literal, we pop the last element and
    // always put it in the filtered query. We don't really care if the last
    // it's a literal or not.
    let last: Token<'_> = query.pop().unwrap(); // safe as query is not empty

    query
        .into_iter()
        .filter(|token| !is_stop_word_token(token))
        .chain([last])
        .collect()
}

fn is_stop_word_token(token: &Token) -> bool {
    if let Token::Literal(lit) = token {
        is_stop_word(&lit.to_lowercase())
    } else {
        false
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn it_finds_stop_words() {
        // cache warm up
        let start_time = std::time::Instant::now();
        let _ = is_stop_word("detector");
        let elapsed = start_time.elapsed().as_millis() as f64;
        // make sure we never spend more than 100 ms for the cache warmup
        assert!(elapsed < 100.0, "{}", elapsed);

        let tests = [
            ("nuclia", false),
            ("is", true),
            ("le", true),
            ("el", true),
            ("stop", false),
            ("stop", false),
        ];

        for (word, expected) in tests {
            let start_time = std::time::Instant::now();
            let matches = is_stop_word(word);
            let elapsed = start_time.elapsed().as_micros() as f64;
            assert_eq!(matches, expected);
            // make sure we never spend more than 1 ms
            assert!(elapsed < 1000.0, "{}", elapsed);
        }
    }

    #[test]
    fn test_stop_word_tokens() {
        assert!(is_stop_word_token(&Token::Literal("is".into())));
        assert!(!is_stop_word_token(&Token::Excluded("is".into())));
        assert!(!is_stop_word_token(&Token::Quoted("is".into())));

        assert!(!is_stop_word_token(&Token::Literal("music".into())));
        assert!(!is_stop_word_token(&Token::Excluded("music".into())));
        assert!(!is_stop_word_token(&Token::Quoted("music".into())));
    }

    #[test]
    fn test_stop_words_detection_is_case_insensitive() {
        assert!(is_stop_word_token(&Token::Literal("is".into())));
        assert!(is_stop_word_token(&Token::Literal("IS".into())));
    }

    #[test]
    fn test_token_with_stop_word_detection() {
        // Stop word in the middle is removed
        let tokens = vec![
            Token::Literal("music".into()),
            Token::Literal("is".into()),
            Token::Literal("classical".into()),
        ];
        let filtered = remove_stop_words(tokens);
        assert_eq!(
            filtered,
            vec![Token::Literal("music".into()), Token::Literal("classical".into()),]
        );

        // Only literals are stop words
        let tokens = vec![
            Token::Literal("music".into()),
            Token::Quoted("is".into()),
            Token::Literal("classical".into()),
        ];
        let filtered = remove_stop_words(tokens);
        assert_eq!(
            filtered,
            vec![
                Token::Literal("music".into()),
                Token::Quoted("is".into()),
                Token::Literal("classical".into()),
            ]
        );

        // Stop words at the end are not removed
        let tokens = vec![
            Token::Literal("classical".into()),
            Token::Literal("music".into()),
            Token::Literal("is".into()),
        ];
        let filtered = remove_stop_words(tokens);
        assert_eq!(
            filtered,
            vec![
                Token::Literal("classical".into()),
                Token::Literal("music".into()),
                Token::Literal("is".into()),
            ]
        );

        // Everything is a stop word, but last is not removed
        let tokens = vec![
            Token::Literal("we".into()),
            Token::Literal("shouldn't".into()),
            Token::Literal("be".into()),
            Token::Literal("here".into()),
        ];
        let filtered = remove_stop_words(tokens);
        assert_eq!(filtered, vec![Token::Literal("here".into()),]);
    }
}
