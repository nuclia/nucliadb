// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use nidx_protos::{LpgDirection, relation_node::NodeType};
use tantivy::{
    DocAddress, DocId, Score, SegmentOrdinal, SegmentReader, TantivyDocument, TantivyError, Term,
    collector::{Collector, SegmentCollector},
    columnar::Column,
    query::{BooleanQuery, Query, TermQuery, TermSetQuery},
    schema::{Facet, Field, IndexRecordOption, Value},
    store::StoreReader,
};
use uuid::Uuid;

use crate::{RelationSearcher, io_maps, schema::encode_node};

#[derive(Clone, Debug)]
pub struct LpgRelation {
    pub resource_id: Uuid,
    pub source: String,
    pub target: String,
    pub edge_type: String,
}

struct EdgeCollector {
    starts: Arc<HashSet<Vec<u64>>>,
    direction: LpgDirection,
    field: Field,
    limit: usize,
}

struct EdgeSegmentCollector {
    starts: Arc<HashSet<Vec<u64>>>,
    direction: LpgDirection,
    source: Column<u64>,
    target: Column<u64>,
    store: StoreReader,
    field: Field,
    segment: SegmentOrdinal,
    limit: usize,
    edges: BTreeMap<Uuid, DocAddress>,
    error: Option<TantivyError>,
}

impl Collector for EdgeCollector {
    type Fruit = BTreeMap<Uuid, DocAddress>;
    type Child = EdgeSegmentCollector;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn for_segment(&self, segment: SegmentOrdinal, reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        Ok(EdgeSegmentCollector {
            starts: Arc::clone(&self.starts),
            direction: self.direction,
            source: reader.fast_fields().u64("encoded_source_id")?,
            target: reader.fast_fields().u64("encoded_target_id")?,
            store: reader.get_store_reader(1)?,
            field: self.field,
            segment,
            limit: self.limit,
            edges: BTreeMap::new(),
            error: None,
        })
    }

    fn merge_fruits(&self, fruits: Vec<tantivy::Result<BTreeMap<Uuid, DocAddress>>>) -> tantivy::Result<Self::Fruit> {
        let mut edges = BTreeMap::new();
        for fruit in fruits {
            for (id, address) in fruit? {
                edges.insert(id, address);
                if edges.len() > self.limit {
                    edges.pop_last();
                }
            }
        }
        Ok(edges)
    }
}

impl EdgeSegmentCollector {
    fn add(&mut self, doc: DocId) -> tantivy::Result<()> {
        // Indexed node values are normalized; fast-field IDs retain exact case and Unicode.
        let outgoing = matches!(self.direction, LpgDirection::LpgOutgoing | LpgDirection::LpgBoth)
            && self
                .starts
                .contains(&self.source.values_for_doc(doc).collect::<Vec<_>>());
        let incoming = matches!(self.direction, LpgDirection::LpgIncoming | LpgDirection::LpgBoth)
            && self
                .starts
                .contains(&self.target.values_for_doc(doc).collect::<Vec<_>>());
        if !outgoing && !incoming {
            return Ok(());
        }
        let record: TantivyDocument = self.store.get(doc)?;
        let bytes = record
            .get_first(self.field)
            .and_then(|value| value.as_bytes())
            .ok_or_else(|| TantivyError::SystemError("LPG relation field identity missing".into()))?;
        if bytes.len() < 16 {
            return Err(TantivyError::SystemError("invalid LPG relation field identity".into()));
        }
        if &bytes[16..] != b"a/metadata" {
            return Ok(());
        }
        let id = Uuid::from_slice(&bytes[..16]).map_err(|error| TantivyError::SystemError(error.to_string()))?;
        self.edges.insert(id, DocAddress::new(self.segment, doc));
        Ok(())
    }
}

impl SegmentCollector for EdgeSegmentCollector {
    type Fruit = tantivy::Result<BTreeMap<Uuid, DocAddress>>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        // A full segment already proves the bound; avoid further stored-document reads.
        if self.error.is_none() && self.edges.len() < self.limit {
            self.error = self.add(doc).err();
        }
    }

    fn harvest(self) -> Self::Fruit {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.edges),
        }
    }
}

impl RelationSearcher {
    #[tracing::instrument(name = "relation::lpg_expand", skip_all)]
    pub fn lpg_expand(
        &self,
        node_ids: &[String],
        direction: LpgDirection,
        edge_types: &[String],
        resource_ids: Option<&HashSet<Uuid>>,
        limit: usize,
    ) -> anyhow::Result<Vec<LpgRelation>> {
        anyhow::ensure!(limit > 0, "LPG relation limit must be positive");
        anyhow::ensure!(
            direction != LpgDirection::Unspecified,
            "LPG expansion direction required"
        );
        if node_ids.is_empty() || resource_ids.is_some_and(HashSet::is_empty) {
            return Ok(vec![]);
        }
        let schema = &self.reader.schema;
        let mut endpoints: Vec<Box<dyn Query>> = Vec::new();
        for field in [
            matches!(direction, LpgDirection::LpgOutgoing | LpgDirection::LpgBoth)
                .then_some(schema.normalized_source_value),
            matches!(direction, LpgDirection::LpgIncoming | LpgDirection::LpgBoth)
                .then_some(schema.normalized_target_value),
        ]
        .into_iter()
        .flatten()
        {
            endpoints.push(Box::new(TermSetQuery::new(
                node_ids
                    .iter()
                    .map(|id| Term::from_field_text(field, &schema.normalize(id))),
            )));
        }
        let mut queries: Vec<Box<dyn Query>> = vec![
            Box::new(BooleanQuery::union(endpoints)),
            Box::new(TermQuery::new(
                Term::from_facet(schema.facets, &Facet::from("/lpg/edge")),
                IndexRecordOption::Basic,
            )),
        ];
        if !edge_types.is_empty() {
            queries.push(Box::new(TermSetQuery::new(
                edge_types.iter().map(|kind| Term::from_field_text(schema.label, kind)),
            )));
        }
        if let Some(ids) = resource_ids {
            queries.push(Box::new(TermSetQuery::new(
                ids.iter()
                    .map(|id| Term::from_field_bytes(schema.resource_id, id.as_bytes())),
            )));
        }
        let collector = EdgeCollector {
            starts: Arc::new(
                node_ids
                    .iter()
                    .map(|id| encode_node(id, io_maps::node_type_to_u64(NodeType::Resource), ""))
                    .collect(),
            ),
            direction,
            field: schema.resource_field_id,
            limit,
        };
        let searcher = self.reader.reader.searcher();
        let edges = searcher.search(&BooleanQuery::intersection(queries), &collector)?;
        edges
            .into_iter()
            .map(|(resource_id, address)| {
                let doc: TantivyDocument = searcher.doc(address)?;
                let text = |field| -> anyhow::Result<String> {
                    doc.get_first(field)
                        .and_then(|value| value.as_str())
                        .map(str::to_owned)
                        .ok_or_else(|| anyhow::anyhow!("LPG relation value missing"))
                };
                Ok(LpgRelation {
                    resource_id,
                    source: text(schema.source_value)?,
                    target: text(schema.target_value)?,
                    edge_type: text(schema.label)?,
                })
            })
            .collect()
    }
}
