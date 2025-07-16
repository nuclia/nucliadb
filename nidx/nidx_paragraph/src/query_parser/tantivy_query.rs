use tantivy::Term;
use tantivy::query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
use tantivy::schema::IndexRecordOption;

use crate::ParagraphSchema;

use super::tokenizer::Token;

// Convert a list of tokens into a tantivy keyword query
pub fn build_keyword_query(query: &[Token]) -> Box<dyn Query> {
    let mut subqueries = vec![];
    let schema = ParagraphSchema::new();

    for item in query {
        match item {
            Token::Literal(literal) => {
                let q: Box<dyn Query> = Box::new(TermQuery::new(
                    Term::from_field_text(schema.text, literal),
                    IndexRecordOption::Basic,
                ));
                subqueries.push((Occur::Should, q));
            }
            Token::Quoted(quoted) => {
                let mut terms: Vec<Term> = quoted
                    .split_whitespace()
                    .map(|word| Term::from_field_text(schema.text, word))
                    .collect();

                if terms.len() == 1 {
                    // phrase queries must have more than one term, so we use a term query
                    let term = terms.remove(0); // safe because terms.len() == 1
                    let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                    subqueries.push((Occur::Should, q));
                } else if terms.len() > 1 {
                    let q: Box<dyn Query> = Box::new(PhraseQuery::new(terms));
                    subqueries.push((Occur::Should, q));
                }
            }
            Token::Excluded(excluded) => {
                let q: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                    (Occur::Must, Box::new(AllQuery)),
                    (
                        Occur::MustNot,
                        Box::new(TermQuery::new(
                            Term::from_field_text(schema.text, excluded),
                            IndexRecordOption::Basic,
                        )),
                    ),
                ]));
                subqueries.push((Occur::Should, q));
            }
        }
    }

    Box::new(BooleanQuery::new(subqueries))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let r = downcast_boolean_query(build_keyword_query(&query));
        let clauses = r.clauses();
        assert_eq!(clauses.len(), 6);

        // term: nucliadb
        assert_eq!(clauses[0].0, Occur::Should);
        let term = extract_term_from(&clauses[0].1);
        assert_eq!(*term, Term::from_field_text(schema.text, "nucliadb"));

        // excluded term: is
        assert_eq!(clauses[1].0, Occur::Should);
        let subquery = clauses[1].1.downcast_ref::<BooleanQuery>().expect("BooleanQuery expected");
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
        assert_eq!(*terms, vec![
            Term::from_field_text(schema.text, "RAG"),
            Term::from_field_text(schema.text, "database"),
        ]);

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

    fn downcast_boolean_query(q: Box<dyn Query>) -> Box<BooleanQuery> {
        let q = q.downcast::<BooleanQuery>();
        assert!(q.is_ok(), "BooleanQuery expected");
        let q = q.unwrap();
        q
    }

    fn extract_term_from(query: &Box<dyn Query>) -> &Term {
        let q = query.downcast_ref::<TermQuery>();
        assert!(q.is_some(), "TermQuery expected");
        let q = q.unwrap();
        q.term()
    }

    fn extract_phrase_terms_from(query: &Box<dyn Query>) -> Vec<Term> {
        let q = query.downcast_ref::<PhraseQuery>();
        assert!(q.is_some(), "PhraseQuery expected");
        let q = q.unwrap();
        q.phrase_terms()
    }
}
