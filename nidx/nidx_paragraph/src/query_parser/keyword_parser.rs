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
use tantivy::Term;
use tantivy::query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
use tantivy::schema::IndexRecordOption;

use crate::ParagraphSchema;

use super::FallbackQuery;
use super::tokenizer::Token;

/// Convert a tokenized query into a tantivy keyword query
///
/// Empty queries will match everything.
pub fn parse_keyword_query<'a>(
    query: &'a [Token<'a>],
    schema: &ParagraphSchema,
) -> Result<Box<dyn Query>, FallbackQuery> {
    let mut errors = vec![];
    let mut subqueries = vec![];
    for item in query {
        match item {
            Token::Literal(literal) => {
                let q = parse_literal(schema, literal);
                subqueries.push((Occur::Should, q));
            }
            Token::Quoted(quoted) => match parse_quoted(schema, quoted) {
                Ok(q) => subqueries.push((Occur::Should, q)),
                Err(error) => errors.push(error),
            },
            Token::Excluded(excluded) => {
                let q = parse_excluded(schema, excluded);
                subqueries.push((Occur::Should, q));
            }
        }
    }

    let q = if subqueries.is_empty() {
        Box::new(AllQuery)
    } else if subqueries.len() == 1 {
        subqueries.pop().unwrap().1
    } else {
        Box::new(BooleanQuery::new(subqueries))
    };

    if errors.is_empty() { Ok(q) } else { Err((q, errors)) }
}

#[inline]
pub fn parse_literal(schema: &ParagraphSchema, literal: &str) -> Box<dyn Query> {
    Box::new(TermQuery::new(
        Term::from_field_text(schema.text, literal),
        IndexRecordOption::Basic,
    ))
}

pub fn parse_quoted(schema: &ParagraphSchema, quoted: &str) -> Result<Box<dyn Query>, String> {
    let mut terms: Vec<Term> = quoted
        .split_whitespace()
        .map(|word| Term::from_field_text(schema.text, word))
        .collect();

    #[allow(clippy::comparison_chain)]
    if terms.len() == 1 {
        // phrase queries must have more than one term, so we use a term query
        let term = terms.remove(0); // safe because terms.len() == 1
        Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
    } else if terms.len() > 1 {
        Ok(Box::new(PhraseQuery::new(terms)))
    } else {
        #[cfg(not(test))] // we don't want an assert on tests, we want the error
        debug_assert!(
            false,
            "Quoted content should have been validated to not only contain whitespaces"
        );
        // we return an error to protect us from tokenizer errors, but this branch should never
        // happen
        Err("Keyword tokenizer built a query with a only whitespaces Quoted token!".to_string())
    }
}

#[inline]
pub fn parse_excluded(schema: &ParagraphSchema, excluded: &str) -> Box<dyn Query> {
    Box::new(BooleanQuery::new(vec![
        (Occur::Must, Box::new(AllQuery)),
        (
            Occur::MustNot,
            Box::new(TermQuery::new(
                Term::from_field_text(schema.text, excluded),
                IndexRecordOption::Basic,
            )),
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_query_is_all_query() {
        let schema = ParagraphSchema::new();
        let query = parse_keyword_query(&[], &schema).unwrap();
        assert!(query.is::<AllQuery>());
    }

    #[test]
    fn test_one_clause_simplification() {
        let schema = ParagraphSchema::new();

        let query = parse_keyword_query(&[Token::Literal("nucliadb".into())], &schema).unwrap();
        let term = extract_term_from(&query);
        assert_eq!(*term, Term::from_field_text(schema.text, "nucliadb"));
    }

    #[test]
    fn test_tantivy_query_conversion() {
        let schema = ParagraphSchema::new();

        // nucliadb -is a "RAG database" with "superpowers"
        let query = vec![
            Token::Literal("nucliadb".into()),
            Token::Excluded("is".into()),
            Token::Literal("a".into()),
            Token::Quoted("RAG database".into()),
            Token::Literal("with".into()),
            Token::Quoted("superpowers".into()),
        ];
        let r = downcast_boolean_query(parse_keyword_query(&query, &schema).unwrap());
        let clauses = r.clauses();
        assert_eq!(clauses.len(), 6);

        // term: nucliadb
        assert_eq!(clauses[0].0, Occur::Should);
        let term = extract_term_from(&clauses[0].1);
        assert_eq!(*term, Term::from_field_text(schema.text, "nucliadb"));

        // excluded term: is
        assert_eq!(clauses[1].0, Occur::Should);
        let subquery = clauses[1]
            .1
            .downcast_ref::<BooleanQuery>()
            .expect("BooleanQuery expected");
        let subclauses = subquery.clauses();
        assert_eq!(subclauses.len(), 2);
        assert_eq!(subclauses[0].0, Occur::Must);
        assert!(subclauses[0].1.downcast_ref::<AllQuery>().is_some());
        assert_eq!(subclauses[1].0, Occur::MustNot);
        let term = extract_term_from(&subclauses[1].1);
        assert_eq!(*term, Term::from_field_text(schema.text, "is"));

        // term: a
        assert_eq!(clauses[2].0, Occur::Should);
        let term = extract_term_from(&clauses[2].1);
        assert_eq!(*term, Term::from_field_text(schema.text, "a"));

        // exact: RAG database
        assert_eq!(clauses[3].0, Occur::Should);
        let terms = extract_phrase_terms_from(&clauses[3].1);
        assert_eq!(
            *terms,
            vec![
                Term::from_field_text(schema.text, "RAG"),
                Term::from_field_text(schema.text, "database"),
            ]
        );

        // term: with
        assert_eq!(clauses[4].0, Occur::Should);
        let term = extract_term_from(&clauses[4].1);
        assert_eq!(*term, Term::from_field_text(schema.text, "with"));

        // exact: superpowers.
        // An exact match of 1 word is converted into a term query
        assert_eq!(clauses[5].0, Occur::Should);
        let term = extract_term_from(&clauses[5].1);
        assert_eq!(*term, Term::from_field_text(schema.text, "superpowers"));
    }

    #[test]
    /// The tokenizer should not allow an empty (whitespaces) Quoted token
    fn test_parse_quoted_error() {
        let schema = ParagraphSchema::new();

        let quoted = "   ";
        let r = parse_quoted(&schema, quoted);
        assert!(r.is_err());

        let r = parse_keyword_query(&[Token::Quoted("   ".into())], &schema);
        assert!(r.is_err());
    }

    fn downcast_boolean_query(q: Box<dyn Query>) -> Box<BooleanQuery> {
        let q = q.downcast::<BooleanQuery>();
        assert!(q.is_ok(), "BooleanQuery expected");
        q.unwrap()
    }

    #[allow(clippy::borrowed_box)]
    fn extract_term_from(query: &Box<dyn Query>) -> &Term {
        let q = query.downcast_ref::<TermQuery>();
        assert!(q.is_some(), "TermQuery expected");
        let q = q.unwrap();
        q.term()
    }

    #[allow(clippy::borrowed_box)]
    fn extract_phrase_terms_from(query: &Box<dyn Query>) -> Vec<Term> {
        let q = query.downcast_ref::<PhraseQuery>();
        assert!(q.is_some(), "PhraseQuery expected");
        let q = q.unwrap();
        q.phrase_terms()
    }
}
