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
use std::path::PathBuf;

use nucliadb_protos::nodereader;
use nucliadb_protos::utils;
use uuid::Uuid;

use crate::metrics::vectors::MergeSource;
use crate::prelude::*;
use crate::query_language::BooleanExpression;
use crate::IndexFiles;
use nucliadb_protos::noderesources;

pub type VectorsReaderPointer = Box<dyn VectorReader>;
pub type VectorsWriterPointer = Box<dyn VectorWriter>;
pub type ProtosRequest = nodereader::VectorSearchRequest;
pub type ProtosResponse = nodereader::VectorSearchResponse;

#[derive(Debug, Clone, Copy)]
pub struct MergeParameters {
    pub max_nodes_in_merge: usize,
    pub segments_before_merge: usize,
    pub maximum_deleted_entries: usize,
}

pub struct MergeContext {
    pub parameters: MergeParameters,
    pub source: MergeSource,
}

#[derive(Clone)]
pub struct VectorIndexConfig {
    pub similarity: utils::VectorSimilarity,
    pub path: PathBuf,
    pub shard_id: String,
    pub normalize_vectors: bool,
}

// In an ideal world this should be part of the actual request, but since
// we use protos all the way down the stack here we are. Once the protos use
// is restricted to only the upper layer, this type won't be needed anymore.
#[derive(Clone, Default)]
pub struct VectorsContext {
    pub filtering_formula: Option<BooleanExpression>,
}

pub struct MergeMetrics {
    pub merged: usize,
    pub left: usize,
}

pub trait MergeResults {
    fn inputs(&self) -> &HashSet<Uuid>;
    fn output(&self) -> Uuid;
    fn record_metrics(&self, source: MergeSource);
    fn get_metrics(&self) -> MergeMetrics;
}

pub trait MergeRunner {
    fn run(&mut self) -> NodeResult<Box<dyn MergeResults>>;
}

pub trait VectorReader: std::fmt::Debug + Send + Sync {
    fn search(&self, request: &ProtosRequest, context: &VectorsContext) -> NodeResult<ProtosResponse>;
    fn stored_ids(&self) -> NodeResult<Vec<String>>;
    fn count(&self) -> NodeResult<usize>;
    fn needs_update(&self) -> NodeResult<bool>;
}

pub trait VectorWriter: std::fmt::Debug + Send + Sync {
    fn count(&self) -> NodeResult<usize>;
    fn get_segment_ids(&self) -> NodeResult<Vec<String>>;
    fn get_index_files(&self, prefix: &str, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles>;

    fn prepare_merge(&self, parameters: MergeParameters) -> NodeResult<Option<Box<dyn MergeRunner>>>;
    fn record_merge(&mut self, merge_result: Box<dyn MergeResults>, source: MergeSource) -> NodeResult<MergeMetrics>;
    fn set_resource(&mut self, resource: ResourceWrapper) -> NodeResult<()>;
    fn delete_resource(&mut self, resource_id: &noderesources::ResourceId) -> NodeResult<()>;
    fn garbage_collection(&mut self) -> NodeResult<()>;
    fn force_garbage_collection(&mut self) -> NodeResult<()>;
    fn reload(&mut self) -> NodeResult<()>;
}

pub struct ResourceWrapper<'a> {
    resource: &'a noderesources::Resource,
    vectorset: Option<String>,
    fallback_to_default_vectorset: bool,
}

impl<'a> From<&'a noderesources::Resource> for ResourceWrapper<'a> {
    fn from(value: &'a noderesources::Resource) -> Self {
        Self {
            resource: value,
            vectorset: None,
            fallback_to_default_vectorset: false,
        }
    }
}

impl<'a> ResourceWrapper<'a> {
    pub fn new_vectorset_resource(
        resource: &'a noderesources::Resource,
        vectorset: &str,
        fallback_to_default_vectorset: bool,
    ) -> Self {
        Self {
            resource,
            vectorset: Some(vectorset.to_string()),
            fallback_to_default_vectorset,
        }
    }

    pub fn id(&self) -> &String {
        &self.resource.shard_id
    }

    pub fn fields(&self) -> impl Iterator<Item = (&String, impl Iterator<Item = ParagraphVectors>)> {
        self.resource.paragraphs.iter().map(|(field_id, paragraphs_wrapper)| {
            let sentences_iterator = paragraphs_wrapper.paragraphs.iter().filter_map(|(_paragraph_id, paragraph)| {
                let sentences = if let Some(vectorset) = &self.vectorset {
                    // indexing a vectorset, we should return only paragraphs from this vectorset.
                    // If vectorset is not found, we'll skip this paragraph
                    if let Some(vectorset_sentences) = paragraph.vectorsets_sentences.get(vectorset) {
                        Some(&vectorset_sentences.sentences)
                    } else if self.fallback_to_default_vectorset {
                        Some(&paragraph.sentences)
                    } else {
                        None
                    }
                } else {
                    // Default vectors index (no vectorset)
                    Some(&paragraph.sentences)
                };
                sentences.map(|s| ParagraphVectors {
                    vectors: s,
                    labels: &paragraph.labels,
                })
            });
            (field_id, sentences_iterator)
        })
    }

    pub fn sentences_to_delete(&self) -> impl Iterator<Item = &str> {
        self.resource.sentences_to_delete.iter().map(String::as_str)
    }
}

pub struct ParagraphVectors<'a> {
    pub vectors: &'a HashMap<String, noderesources::VectorSentence>,
    pub labels: &'a Vec<String>,
}
