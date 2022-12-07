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

use std::path::Path;

use itertools::Itertools;
use tantivy::collector::TopDocs;
use tantivy::query::RegexQuery;
use tantivy::schema::{Field, Schema, Term, TextFieldIndexing, TextOptions, STORED, STRING};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy};

use super::errors::*;
use super::relations_io::IoNode;

pub type DReader = IndexReader;
pub type DWriter = IndexWriter;

pub struct NodeDictionary {
    node_value: Field,
    node_hash: Field,
    #[allow(unused)]
    index: Index,
}
impl NodeDictionary {
    const NODE_VALUE: &str = "value";
    const NODE_HASH: &str = "hash";
    const NUM_THREADS: usize = 1;
    const MEM_LIMIT: usize = 6_000_000;

    fn adapt_text(&self, text: &str) -> String {
        deunicode::deunicode(text).to_lowercase()
    }
    fn build_query(&self, text: &str) -> String {
        let query = text
            .split(' ')
            .filter(|s| !s.is_empty())
            .map(|s| regex::escape(s.trim()))
            .join(r"\s+");
        format!("(?im){query}.*")
    }
    fn new(path: &Path) -> RResult<NodeDictionary> {
        let text_options = TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw"))
            .set_stored();
        let mut schema_builder = Schema::builder();
        let node_hash = schema_builder.add_text_field(Self::NODE_HASH, STRING | STORED);
        let node_value = schema_builder.add_text_field(Self::NODE_VALUE, text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path, schema).or_else(|_| Index::open_in_dir(path))?;
        Ok(NodeDictionary {
            index,
            node_hash,
            node_value,
        })
    }
    pub fn new_writer(path: &Path) -> RResult<(NodeDictionary, IndexWriter)> {
        let dictionary = Self::new(path)?;
        let writer = dictionary
            .index
            .writer_with_num_threads(Self::NUM_THREADS, Self::MEM_LIMIT)?;
        Ok((dictionary, writer))
    }
    pub fn new_reader(path: &Path) -> RResult<(NodeDictionary, IndexReader)> {
        let dictionary = Self::new(path)?;
        let reader = dictionary
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        Ok((dictionary, reader))
    }
    pub fn search(
        &self,
        reader: &IndexReader,
        no_results: usize,
        query: &str,
    ) -> RResult<Vec<String>> {
        let query = self.adapt_text(query);
        let query = self.build_query(&query);
        let termq = Box::new(RegexQuery::from_pattern(&query, self.node_value)?);
        let collector = TopDocs::with_limit(no_results);
        let searcher = reader.searcher();
        let results = searcher
            .search(termq.as_ref(), &collector)?
            .into_iter()
            .flat_map(|(_, d)| searcher.doc(d).ok())
            .flat_map(|d| {
                d.get_first(self.node_hash)
                    .and_then(|v| v.as_text())
                    .map(|v| v.to_string())
            })
            .collect();
        Ok(results)
    }
    pub fn add_node(&self, writer: &IndexWriter, node: &IoNode) -> RResult<()> {
        let document = doc!(
            self.node_hash => node.hash(),
            self.node_value => self.adapt_text(node.name())
        );
        self.delete_node(writer, node);
        writer.add_document(document)?;
        Ok(())
    }
    pub fn delete_node(&self, writer: &IndexWriter, node: &IoNode) {
        writer.delete_term(Term::from_field_text(self.node_hash, node.hash()));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn search_test() {
        let dir = tempfile::tempdir().unwrap();
        let (index, mut writer) = NodeDictionary::new_writer(dir.path()).unwrap();
        let node = IoNode::new("New york".to_string(), "Untyped".to_string(), None);
        index.add_node(&writer, &node).unwrap();
        let node = IoNode::new("Barcelona".to_string(), "Untyped".to_string(), None);
        index.add_node(&writer, &node).unwrap();
        writer.commit().unwrap();
        let (index, reader) = NodeDictionary::new_reader(dir.path()).unwrap();
        let r1 = index.search(&reader, 10, "new york").unwrap();
        let r2 = index.search(&reader, 10, "new York").unwrap();
        let r3 = index.search(&reader, 10, "new").unwrap();
        let r4 = index.search(&reader, 10, "york").unwrap();
        let r5 = index.search(&reader, 10, "bár").unwrap();

        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r3.len(), 1);
        assert_eq!(r4.len(), 0);
        assert_eq!(r5.len(), 1);
    }
    #[test]
    fn open_reader() {
        let dir = tempfile::tempdir().unwrap();
        NodeDictionary::new_reader(dir.path()).unwrap();
    }
    #[test]
    fn open_writer() {
        let dir = tempfile::tempdir().unwrap();
        NodeDictionary::new_writer(dir.path()).unwrap();
    }
}
