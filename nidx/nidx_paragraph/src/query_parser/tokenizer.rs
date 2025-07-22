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
pub fn tokenize_query<'a>(input: &'a str) -> Result<(&'a str, Vec<Token<'a>>), nom::error::Error<&'a str>> {
    let is_literal_char = |c: char| -> bool { c.is_ascii_graphic() && c != '"' };

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

    let r: IResult<_, _> = map(
        delimited(
            multispace0,
            opt(fold_many0(
                alt((
                    map(quoted, |t| Some(Token::Quoted(Cow::Borrowed(t)))),
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
            // REVIEW: we may want to use a different default for empty query
            let tokenized = t.unwrap_or_else(Vec::new);

            // After our grammar tokenization, we want to pass the tantivy
            // tokenizer to remove punctuation and apply the same process the
            // index is applying.
            //
            // NOTE this depends on how the `text` field is being indexed and
            // tokenized, any change there should be reflected here too
            let mut tantivy_tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .build();
            tokenized
                .into_iter()
                .flat_map(|token: Token<'a>| match token {
                    Token::Literal(Cow::Borrowed(lit)) => {
                        let mut stream = tantivy_tokenizer.token_stream(&lit);
                        let mut tokens = vec![];
                        while stream.advance() {
                            let token = stream.token();
                            tokens.push(Token::Literal(Cow::Borrowed(&lit[token.offset_from..token.offset_to])));
                        }
                        tokens
                    }
                    Token::Quoted(Cow::Borrowed(quoted)) => {
                        let mut stream = tantivy_tokenizer.token_stream(&quoted);
                        let mut tokens = vec![];
                        while stream.advance() {
                            let token = stream.token();
                            tokens.push(token.text.clone())
                        }
                        vec![Token::Quoted(tokens.into_iter().join(" ").into())]
                    }
                    Token::Excluded(Cow::Borrowed(excluded)) => {
                        let mut stream = tantivy_tokenizer.token_stream(&excluded);
                        let mut tokens = vec![];
                        while stream.advance() {
                            let token = stream.token();
                            tokens.push(Token::Excluded(Cow::Borrowed(
                                &excluded[token.offset_from..token.offset_to],
                            )));
                        }
                        tokens
                    }
                    Token::Literal(Cow::Owned(_)) | Token::Quoted(Cow::Owned(_)) | Token::Excluded(Cow::Owned(_)) => {
                        unreachable!("nom parser only returns borrowed strings")
                    }
                })
                .collect::<Vec<Token<'a>>>()
        },
    )(input);
    r.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use Token::*;

    #[test]
    fn test_empty_query() {
        let query = "";
        let expected = vec![];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_simple_query() {
        let query = "This is a simple query";
        let expected = vec![
            Literal(Cow::Borrowed("This")),
            Literal(Cow::Borrowed("is")),
            Literal(Cow::Borrowed("a")),
            Literal(Cow::Borrowed("simple")),
            Literal(Cow::Borrowed("query")),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_special_characters() {
        let query = "This-is?a_less+simple*query";
        let expected = vec![
            Literal("This".into()),
            Literal("is".into()),
            Literal("a".into()),
            Literal("less".into()),
            Literal("simple".into()),
            Literal("query".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_excluded_words() {
        let query = "This is an -excluded word";
        let expected = vec![
            Literal("This".into()),
            Literal("is".into()),
            Literal("an".into()),
            Excluded("excluded".into()),
            Literal("word".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = "-Everything -is -excluded";
        let expected = vec![
            Excluded("Everything".into()),
            Excluded("is".into()),
            Excluded("excluded".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_quoted_strings() {
        let query = r#""quoted""#;
        let expected = vec![Quoted("quoted".into())];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"This is "really important""#;
        let expected = vec![
            Literal("This".into()),
            Literal("is".into()),
            Quoted("really important".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"half "quoted string"#;
        let expected = vec![
            Literal("half".into()),
            Literal("quoted".into()),
            Literal("string".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"half" quoted string"#;
        let expected = vec![
            Literal("half".into()),
            Literal("quoted".into()),
            Literal("string".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"half q"uoted string"#;
        let expected = vec![
            Literal("half".into()),
            Literal("q".into()),
            Literal("uoted".into()),
            Literal("string".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_excluded_inside_quotes() {
        // quotes take preference over excluding words
        let query = r#"This is "really -important""#;
        let expected = vec![
            Literal("This".into()),
            Literal("is".into()),
            // here we have removed the minus (-) due to tantivy tokenization
            Quoted("really important".into()),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_complex_combinations() {
        let query = r#"This is "really" "important stuff" -except for this"#;
        let expected = vec![
            Literal("This".into()),
            Literal("is".into()),
            Quoted("really".into()),
            Quoted("important stuff".into()),
            Excluded("except".into()),
            Literal("for".into()),
            Literal("this".into()),
        ];
        let r = tokenize_query(query);
        println!("Nom result: {r:?}");
        assert_eq!(r, Ok(("", expected)));
    }
}
