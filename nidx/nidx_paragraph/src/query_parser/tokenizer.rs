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
use std::borrow::Cow;

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{is_a, is_not, take_while1};
use nom::character::complete::{char, multispace0};
use nom::combinator::{eof, map, opt};
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom::{Finish, IResult};
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};
use tracing::warn;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Token<'a> {
    Literal(Cow<'a, str>),
    Quoted(Cow<'a, str>),
    Excluded(Cow<'a, str>),
}

/// Tokenize a user query and return a list of lexical tokens.
///
/// Currently, our query language only supports:
/// - literal terms (words)
/// - excluded terms (using a prefix -)
/// - exact match terms (using double quotes)
///
/// This is a lenient parser implementation, unclosed quotes will be removed
/// from the tokenized result and no error will be returned.
///
/// Note this tokenization also applies the default tantivy tokenizer.
/// Punctuation and other special characters will be removed from the query.
///
pub fn tokenize_query_infallible(input: &str) -> Vec<Token<'_>> {
    let r = nidx_grammar_tokenize(input);

    // safe to use finish. As we are using a complete parser, it'll never panic for Incomplete
    match r.finish() {
        Ok((rest, tokens)) => {
            debug_assert!(rest.is_empty(), "parser should be complete and parse all input");

            // after our own grammar tokenization, we pass the tantivy one to apply the same changes
            // to the query than the ones the index applies to the text
            tantivy_retokenize(tokens)
        }
        Err(error) => {
            warn!("Parsing error for query '{}'. {:?}", input, error);
            // Fallback to tantivy's tokenizer and parse the whole query as
            // literals. This makes tokenization infallible at expenses of
            // losing our grammar if the parser is incorrect.
            tantivy_retokenize(vec![Token::Literal(Cow::Borrowed(input))])
        }
    }
}

fn nidx_grammar_tokenize(input: &str) -> IResult<&str, Vec<Token>, nom::error::Error<&str>> {
    let is_literal_char = |c: char| -> bool {
        c.is_alphanumeric()
            || (
                // a ~is_punctuation excluding quotes
                !c.is_whitespace() && !c.is_control() && c != '"'
            )
    };

    // detect alphanumeric words like: abc, 123, a1b2, a-b.3c, etc.
    let literal = delimited(multispace0, take_while1(is_literal_char), multispace0);

    // detect a quoted string: "xxx"
    let quoted = delimited(
        multispace0,
        delimited(char('"'), is_not(r#"""#), char('"')),
        multispace0,
    );

    // detect a alphanumeric string prefixed with a -: -abc123
    let excluded = delimited(
        multispace0,
        preceded(char('-'), take_while1(is_literal_char)),
        multispace0,
    );

    let unclosed_quote = is_a(r#"""#);

    map(
        delimited(
            multispace0,
            opt(fold_many0(
                alt((
                    map(quoted, |t: &str| {
                        // our parser rule can match empty quotes, we get rid of them
                        if t.trim().is_empty() {
                            return None;
                        }
                        Some(Token::Quoted(Cow::Borrowed(t)))
                    }),
                    map(unclosed_quote, |_| None),
                    map(excluded, |t| Some(Token::Excluded(Cow::Borrowed(t)))),
                    map(literal, |t| Some(Token::Literal(Cow::Borrowed(t)))),
                )),
                Vec::new,
                |mut acc, item| {
                    if let Some(item) = item {
                        acc.push(item);
                    }
                    acc
                },
            )),
            eof,
        ),
        |t| {
            // Return an empty list of tokens if query is empty (or only contains spaces)
            t.unwrap_or_default()
        },
    )(input)
}

/// Parse a stream of tokens and retokenize them using the tantivy tokenizer.
///
/// This is done after our own grammar tokenization. We want to pass tantivy's
/// tokenizer to remove puntuation and apply the same process the index is
/// applying to the `text` field.
///
/// NOTE this depends on how the `text` field is being indexed and tokenized.
/// Any change there should be reflected here too.
fn tantivy_retokenize(tokens: Vec<Token>) -> Vec<Token> {
    let mut tantivy_tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();

    tokens
        .into_iter()
        .flat_map(|token| match token {
            Token::Literal(Cow::Borrowed(lit)) => {
                let mut stream = tantivy_tokenizer.token_stream(lit);
                let mut tokens = vec![];
                while stream.advance() {
                    let token = stream.token();
                    tokens.push(Token::Literal(Cow::Owned(token.text.clone())));
                }
                tokens
            }
            Token::Quoted(Cow::Borrowed(quoted)) => {
                let mut stream = tantivy_tokenizer.token_stream(quoted);
                let mut tokens = vec![];
                while stream.advance() {
                    let token = stream.token();
                    tokens.push(token.text.clone())
                }
                let text = tokens.into_iter().join(" ");
                // as with the nom tokenizer, empty quotes make no sense so we
                // get rid of them
                if !text.is_empty() {
                    vec![Token::Quoted(Cow::Owned(text))]
                } else {
                    vec![]
                }
            }
            Token::Excluded(Cow::Borrowed(excluded)) => {
                let mut stream = tantivy_tokenizer.token_stream(excluded);
                let mut tokens = vec![];
                while stream.advance() {
                    let token = stream.token();
                    tokens.push(Token::Excluded(Cow::Owned(token.text.clone())));
                }
                tokens
            }
            Token::Literal(Cow::Owned(_)) | Token::Quoted(Cow::Owned(_)) | Token::Excluded(Cow::Owned(_)) => {
                unreachable!("nom parser only returns borrowed strings")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use Token::*;

    #[test]
    fn test_empty_query() {
        let expected = vec![];

        let query = "";
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // whitespaces only
        let query = "    ";
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // dashes and whitespaces only
        let query = "  - - -   - - -  ";
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // quoted whitespaces
        let query = r#""  ""#;
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // only special characters (punctuation is removed)
        let query = "!@#~&/()=?";
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_simple_query() {
        let query = "This is a simple query";
        let expected = vec![
            Literal(Cow::Borrowed("this")),
            Literal(Cow::Borrowed("is")),
            Literal(Cow::Borrowed("a")),
            Literal(Cow::Borrowed("simple")),
            Literal(Cow::Borrowed("query")),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_special_characters() {
        let query = "This-is?a_less+simple*query";
        let expected = vec![
            Literal("this".into()),
            Literal("is".into()),
            Literal("a".into()),
            Literal("less".into()),
            Literal("simple".into()),
            Literal("query".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_excluded_words() {
        let query = "This is an -excluded word";
        let expected = vec![
            Literal("this".into()),
            Literal("is".into()),
            Literal("an".into()),
            Excluded("excluded".into()),
            Literal("word".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        let query = "-Everything -is -excluded";
        let expected = vec![
            Excluded("everything".into()),
            Excluded("is".into()),
            Excluded("excluded".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // We don't handle double exclude, it is considered as exclude `-Not` and the second dash is
        // removed by the tantivy tokenizer
        let query = "--Not ---everything -is -excluded";
        let expected = vec![
            Excluded("not".into()),
            Excluded("everything".into()),
            Excluded("is".into()),
            Excluded("excluded".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // hyphenated words. Due to tantivy tokenization, we remove the minus (-) and split the term
        // in two literals
        let query = "hyphenated-word";
        let expected = vec![Literal("hyphenated".into()), Literal("word".into())];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_quoted_strings() {
        let query = r#""quoted""#;
        let expected = vec![Quoted("quoted".into())];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        let query = r#"This is "really important""#;
        let expected = vec![
            Literal("this".into()),
            Literal("is".into()),
            Quoted("really important".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        let query = r#"half "quoted string"#;
        let expected = vec![
            Literal("half".into()),
            Literal("quoted".into()),
            Literal("string".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        let query = r#"half" quoted string"#;
        let expected = vec![
            Literal("half".into()),
            Literal("quoted".into()),
            Literal("string".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        let query = r#"half q"uoted string"#;
        let expected = vec![
            Literal("half".into()),
            Literal("q".into()),
            Literal("uoted".into()),
            Literal("string".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // quoted strings with special characters that tantivy tokenizer removes are removed
        let query = r#"tantivy deletes "...""#;
        let expected = vec![Literal("tantivy".into()), Literal("deletes".into())];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_excluded_inside_quotes() {
        // quotes take preference over excluding words
        let query = r#"This is "really -important""#;
        let expected = vec![
            Literal("this".into()),
            Literal("is".into()),
            // here we have removed the minus (-) due to tantivy tokenization
            Quoted("really important".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // without separation between excluded and quotes
        let query = r#""quoted"-term"#;
        let expected = vec![Quoted("quoted".into()), Excluded("term".into())];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        let query = r#"-term"quoted""#;
        let expected = vec![Excluded("term".into()), Quoted("quoted".into())];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_unicode_characters_and_language_support() {
        let query = r#"résumé "São Paulo" -tésting"#;
        let expected = vec![
            Literal("résumé".into()),
            Quoted("são paulo".into()),
            Excluded("tésting".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // japanese chars are not splitted, this may lead to wrong results. We may want to support
        // other tokenizers to consider different languages
        let query = "クジラはすごい！";
        let expected = vec![Literal("クジラはすごい".into())];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // As arabic has spaces, at least we split them
        let query = r#"الحيتان رائعة"#;
        let expected = vec![Literal("الحيتان".into()), Literal("رائعة".into())];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_complex_combinations() {
        let query = r#"This is "really" "important stuff" -except for this"#;
        let expected = vec![
            Literal("this".into()),
            Literal("is".into()),
            Quoted("really".into()),
            Quoted("important stuff".into()),
            Excluded("except".into()),
            Literal("for".into()),
            Literal("this".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // mixed whitespaces in different positions
        let query = r#"   -first "second    part"   third -fourth   "#;
        let expected = vec![
            Excluded("first".into()),
            Quoted("second part".into()),
            Literal("third".into()),
            Excluded("fourth".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // The following are more (potentially) controversial combination that may lead to
        // unexpected results. Any of these is subject to change in the future if we implement more
        // complex parsing (e.g. escaping support)

        // quoted string with escaped quotes
        let query = r#"Text with "escaped \"quote\" inside""#;
        let expected = vec![
            Literal("text".into()),
            Literal("with".into()),
            // handling escaped quotes would quote these 3 terms together
            Quoted("escaped".into()),
            Literal("quote".into()),
            Quoted("inside".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);

        // Mix punctuation with dashes and quotes. In this case, "WORLD"!-minus this is considered a
        // literal because tantivy tokenization is done *after* our grammar tokenization. So we
        // detect the `!-minus` and tantivy removed both punctuation chars
        let query = r#"Hello, "WORLD"!-minus -this!"#;
        let expected = vec![
            Literal("hello".into()),
            Quoted("world".into()),
            Literal("minus".into()),
            Excluded("this".into()),
        ];
        let tokens = tokenize_query_infallible(query);
        assert_eq!(tokens, expected);
        assert_eq!(
            nidx_grammar_tokenize(r#""WORLD"!-minus"#),
            Ok(("", vec![Quoted("WORLD".into()), Literal("!-minus".into())]))
        );
    }

    #[test]
    fn test_tantivy_retokenization() {
        let query = vec![Literal("Title".into()), Literal("No-DaShEs".into())];
        let expected = vec![Literal("title".into()), Literal("no".into()), Literal("dashes".into())];
        let retokenized = tantivy_retokenize(query);
        assert_eq!(retokenized, expected)
    }
}
