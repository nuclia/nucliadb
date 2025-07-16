use std::borrow::Cow;

use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{is_a, is_not};
use nom::character::complete::{alphanumeric1, char, multispace0};
use nom::combinator::{eof, map, opt};
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Token<'a> {
    Literal(Cow<'a, str>),
    Quoted(&'a str),
    Excluded(&'a str),
}

/// Parse user query and return its tokenized version.
///
/// Currently, our query language only supports:
/// - literal terms (words)
/// - excluded terms (using a prefix -)
/// - exact match terms (using double quotes)
///
/// This is a lenient parser implementation, unclosed quotes will be removed
/// from the tokenized result and no error will be returned
pub fn tokenize_query(input: &str) -> IResult<&str, Vec<Token>> {
    // detect alphanumeric words like: abc, 123, a1b2...
    let literal = delimited(multispace0, alphanumeric1, multispace0);

    // detect a quoted string: "xxx"
    let quoted = delimited(
        multispace0,
        delimited(char('"'), is_not(r#"""#), char('"')),
        multispace0,
    );

    // detect a alphanumeric string prefixed with a -: -abc123
    let excluded = delimited(multispace0, preceded(char('-'), alphanumeric1), multispace0);

    let unclosed_quote = is_a(r#"""#);

    let r: IResult<_, _> = map(
        delimited(
            multispace0,
            opt(fold_many0(
                alt((
                    map(quoted, |t| Some(Token::Quoted(t))),
                    map(unclosed_quote, |_| None),
                    map(excluded, |t| Some(Token::Excluded(t))),
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
            // Return an empty string if query is empty (or only contains spaces)
            // REVIEW: we may want to use a different default for empty query
            t.unwrap_or_default()
        },
    )(input);
    r
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
    fn test_excluded_words() {
        let query = "This is an -excluded word";
        let expected = vec![
            Literal(Cow::Borrowed("This")),
            Literal(Cow::Borrowed("is")),
            Literal(Cow::Borrowed("an")),
            Excluded("excluded"),
            Literal(Cow::Borrowed("word")),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = "-Everything -is -excluded";
        let expected = vec![Excluded("Everything"), Excluded("is"), Excluded("excluded")];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_quoted_strings() {
        let query = r#""quoted""#;
        let expected = vec![Quoted("quoted")];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"This is "really important""#;
        let expected = vec![
            Literal(Cow::Borrowed("This")),
            Literal(Cow::Borrowed("is")),
            Quoted("really important"),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"half "quoted string"#;
        let expected = vec![
            Literal(Cow::Borrowed("half")),
            Literal(Cow::Borrowed("quoted")),
            Literal(Cow::Borrowed("string")),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"half" quoted string"#;
        let expected = vec![
            Literal(Cow::Borrowed("half")),
            Literal(Cow::Borrowed("quoted")),
            Literal(Cow::Borrowed("string")),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));

        let query = r#"half q"uoted string"#;
        let expected = vec![
            Literal(Cow::Borrowed("half")),
            Literal(Cow::Borrowed("q")),
            Literal(Cow::Borrowed("uoted")),
            Literal(Cow::Borrowed("string")),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_excluded_inside_quotes() {
        // quotes take preference over excluding words
        let query = r#"This is "really -important""#;
        let expected = vec![
            Literal(Cow::Borrowed("This")),
            Literal(Cow::Borrowed("is")),
            Quoted("really -important"),
        ];
        let r = tokenize_query(query);
        assert_eq!(r, Ok(("", expected)));
    }

    #[test]
    fn test_complex_combinations() {
        let query = r#"This is "really" "important stuff" -except for this"#;
        let expected = vec![
            Literal(Cow::Borrowed("This")),
            Literal(Cow::Borrowed("is")),
            Quoted("really"),
            Quoted("important stuff"),
            Excluded("except"),
            Literal(Cow::Borrowed("for")),
            Literal(Cow::Borrowed("this")),
        ];
        let r = tokenize_query(query);
        println!("Nom result: {r:?}");
        assert_eq!(r, Ok(("", expected)));
    }
}
