// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//! Custom tantivy tokenizers
//!

use tantivy::tokenizer::{LowerCaser, TextAnalyzer, TokenizerManager};

use crate::tokenizers::magic_char::MagicCharTokenizer;

pub fn custom_tokenizer_manager() -> TokenizerManager {
    let manager = TokenizerManager::default();
    manager.register(
        magic_char::MAGIC_CHAR_TOKENIZER_NAME,
        TextAnalyzer::builder(MagicCharTokenizer::default())
            .filter(LowerCaser)
            .build(),
    );
    manager
}

pub mod magic_char {
    //! A `MagicCharTokenizer` splits text by a predefined magic character.

    use std::str::CharIndices;

    use tantivy::tokenizer::Token;
    use tantivy::tokenizer::TokenStream;
    use tantivy::tokenizer::Tokenizer;

    pub const MAGIC_CHAR: char = '\0';
    pub const MAGIC_CHAR_TOKENIZER_NAME: &str = "magic_char";

    /// Tokenize text by splitting on a magic character.
    ///
    /// This tokenizer is heavily inspired by the SimpleTokenizer, but utilizing a
    /// magic char as separator.
    #[derive(Clone, Default)]
    pub struct MagicCharTokenizer {
        token: Token,
    }

    /// Token stream produced by `MagicCharTokenizer`.
    pub struct MagitCharTokenStream<'a> {
        text: &'a str,
        chars: CharIndices<'a>,
        token: &'a mut Token,
    }

    impl Tokenizer for MagicCharTokenizer {
        type TokenStream<'a> = MagitCharTokenStream<'a>;

        fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
            self.token.reset();
            MagitCharTokenStream {
                text,
                chars: text.char_indices(),
                token: &mut self.token,
            }
        }
    }

    impl MagitCharTokenStream<'_> {
        // search for the end of the current token.
        fn search_token_end(&mut self) -> usize {
            (&mut self.chars)
                .filter(|(_, c)| *c == MAGIC_CHAR)
                .map(|(offset, _)| offset)
                .next()
                .unwrap_or(self.text.len())
        }
    }

    impl TokenStream for MagitCharTokenStream<'_> {
        fn advance(&mut self) -> bool {
            self.token.text.clear();
            self.token.position = self.token.position.wrapping_add(1);
            while let Some((offset_from, c)) = self.chars.next() {
                if c != MAGIC_CHAR {
                    let offset_to = self.search_token_end();
                    self.token.offset_from = offset_from;
                    self.token.offset_to = offset_to;
                    self.token.text.push_str(&self.text[offset_from..offset_to]);
                    return true;
                }
            }
            false
        }

        fn token(&self) -> &Token {
            self.token
        }

        fn token_mut(&mut self) -> &mut Token {
            self.token
        }
    }

    #[cfg(test)]
    mod tests {
        use tantivy::tokenizer::TextAnalyzer;

        use super::*;

        #[test]
        fn test_magic_char_tokenizer() {
            let text = format!("This is{MAGIC_CHAR}my text{MAGIC_CHAR}");

            let mut tokenizer = MagicCharTokenizer::default();
            let mut stream = tokenizer.token_stream(&text);
            assert_eq!(stream.token().text, "");
            assert!(stream.advance());
            assert_eq!(stream.token().text, "This is");
            assert!(stream.advance());
            assert_eq!(stream.token().text, "my text");
            assert!(!stream.advance());
        }

        #[test]
        fn test_magic_char_tokenizer_with_text_analyzer() {
            let text = format!("This is{MAGIC_CHAR}my text{MAGIC_CHAR}");

            let mut analyzer = TextAnalyzer::from(MagicCharTokenizer::default());
            let mut token_stream = analyzer.token_stream(&text);
            let mut tokens: Vec<Token> = vec![];
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            token_stream.process(&mut add_token);

            assert_eq!(tokens.len(), 2);

            assert_eq!(tokens[0].position, 0);
            assert_eq!(tokens[0].text, "This is");
            assert_eq!(tokens[0].offset_from, 0);
            assert_eq!(tokens[0].offset_to, 7);

            assert_eq!(tokens[1].position, 1);
            assert_eq!(tokens[1].text, "my text");
            assert_eq!(tokens[1].offset_from, 8);
            assert_eq!(tokens[1].offset_to, 15);
        }
    }
}
