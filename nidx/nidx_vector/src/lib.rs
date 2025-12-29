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

pub mod config;
mod data_store;
mod data_types;
mod field_list_metadata;
pub mod formula;
mod hnsw;
mod indexer;
mod inverted_index;
mod multivector;
mod query_io;
mod request_types;
mod searcher;
pub mod segment;
mod utils;
pub mod vector_types;

use config::VectorConfig;
use indexer::{ResourceWrapper, index_resource};
use nidx_protos::{Resource, VectorSearchResponse};
use nidx_types::prefilter::PrefilterResult;
use nidx_types::{OpenIndexMetadata, SegmentMetadata};
use searcher::Searcher;
use segment::OpenSegment;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::iter::Peekable;
use std::path::Path;
use thiserror::Error;
use tracing::instrument;

pub use indexer::SEGMENT_TAGS;
pub use request_types::VectorSearchRequest;

use crate::config::IndexEntity;
use crate::indexer::index_relations;
use crate::utils::FieldKey;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct ParagraphAddr(u32);
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct VectorAddr(pub u32);

#[derive(Clone, Serialize, Deserialize)]
pub struct VectorSegmentMeta {
    pub tags: HashSet<String>,
}

type VectorSegmentMetadata = SegmentMetadata<VectorSegmentMeta>;

pub struct VectorIndexer;

impl VectorIndexer {
    #[instrument(name = "vector::index_resource", skip_all)]
    pub fn index_resource(
        &self,
        output_dir: &Path,
        config: &VectorConfig,
        resource: &Resource,
        index_name: &str,
        use_default_vectorset: bool,
    ) -> anyhow::Result<Option<VectorSegmentMetadata>> {
        if matches!(config.entity, IndexEntity::Paragraph) {
            let vectorset_resource =
                ResourceWrapper::new_vectorset_resource(resource, index_name, use_default_vectorset);
            index_resource(vectorset_resource, output_dir, config)
        } else {
            index_relations(resource, output_dir, config)
        }
    }

    pub fn deletions_for_resource(&self, resource: &Resource, index_name: &str) -> Vec<String> {
        if let Some(prefixes) = resource.vector_prefixes_to_delete.get(index_name) {
            prefixes.items.clone()
        } else {
            resource.vectors_to_delete_in_all_vectorsets.clone()
        }
    }

    #[instrument(name = "vector::merge", skip_all)]
    pub fn merge(
        &self,
        work_dir: &Path,
        config: VectorConfig,
        open_index: impl OpenIndexMetadata<VectorSegmentMeta>,
    ) -> anyhow::Result<VectorSegmentMetadata> {
        let mut open_segments: Vec<(OpenSegment, HashSet<FieldKey>)> = Vec::new();

        let mut segment_deletions = segment_deletions(&open_index);
        while let Some((segment, deletions)) = segment_deletions.next() {
            let mut open_segment = segment::open(segment, &config)?;
            open_segment.apply_deletions(deletions);
            open_segments.push((open_segment, deletions.clone()));
        }

        // Do the merge
        let open_segments_ref = open_segments.iter().map(|(s, d)| (s, d)).collect();
        let open_destination = segment::merge(work_dir, open_segments_ref, &config)?;

        Ok(open_destination.into_metadata())
    }
}

pub struct VectorSearcher {
    searcher: Searcher,
}

impl VectorSearcher {
    #[instrument(name = "vector::open", skip_all)]
    pub fn open(config: VectorConfig, open_index: impl OpenIndexMetadata<VectorSegmentMeta>) -> anyhow::Result<Self> {
        Ok(VectorSearcher {
            searcher: Searcher::open(open_segments(open_index, &config)?, config)?,
        })
    }

    #[instrument(name = "vector::search", skip_all)]
    pub fn search(
        &self,
        request: &VectorSearchRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<VectorSearchResponse> {
        self.searcher.search(request, prefilter)
    }

    pub fn space_usage(&self) -> usize {
        self.searcher.space_usage()
    }
}

fn open_segments(
    open_index: impl OpenIndexMetadata<VectorSegmentMeta>,
    config: &VectorConfig,
) -> VectorR<Vec<OpenSegment>> {
    let mut open_segments = Vec::new();

    let mut segment_deletions = segment_deletions(&open_index);
    while let Some((segment, deletions)) = segment_deletions.next() {
        let mut open_segment = segment::open(segment, config)?;
        open_segment.apply_deletions(deletions);
        open_segments.push(open_segment);
    }

    Ok(open_segments)
}

struct SegmentDeletions<'a, S, D>
where
    S: Iterator,
    D: Iterator,
{
    segments: S,
    deletions: Peekable<D>,
    deletions_so_far: HashSet<FieldKey<'a>>,
}

/// Should be SegmentDeletions::new but this runs into less problems with type inference
fn segment_deletions<'a>(
    open_index: &'a impl OpenIndexMetadata<VectorSegmentMeta>,
) -> SegmentDeletions<
    'a,
    impl Iterator<Item = (VectorSegmentMetadata, nidx_types::Seq)>,
    impl Iterator<Item = (&'a String, nidx_types::Seq)>,
> {
    SegmentDeletions {
        segments: open_index.segments().rev(),
        deletions: open_index.deletions().rev().peekable(),
        deletions_so_far: HashSet::new(),
    }
}

impl<'a: 'b, 'b, S, D> SegmentDeletions<'a, S, D>
where
    S: Iterator<Item = (VectorSegmentMetadata, nidx_types::Seq)>,
    D: Iterator<Item = (&'a String, nidx_types::Seq)>,
{
    fn next(&mut self) -> Option<(VectorSegmentMetadata, &HashSet<FieldKey<'b>>)> {
        let (segment, segment_seq) = self.segments.next()?;
        while let Some(d) = self.deletions.peek()
            && d.1 > segment_seq
        {
            if let Some(key) = FieldKey::from_field_id(self.deletions.next().unwrap().0) {
                self.deletions_so_far.insert(key);
            }
        }
        Some((segment, &self.deletions_so_far))
    }
}

#[derive(Debug, Error)]
pub enum VectorErr {
    #[error("IO error: {0}")]
    IoErr(#[from] std::io::Error),
    #[error("This index does not have an alive writer")]
    NoWriterError,
    #[error("Only one writer can be open at the same time")]
    MultipleWritersError,
    #[error("Writer has uncommitted changes, please commit or abort")]
    UncommittedChangesError,
    #[error("Merger is already initialized")]
    MergerAlreadyInitialized,
    #[error("Can not merge zero segments")]
    EmptyMerge,
    #[error("Inconsistent dimensions. Index={index_config} Vector={vector}")]
    InconsistentDimensions { index_config: usize, vector: usize },
    #[error("UTF8 decoding error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Some of the merged segments were not found")]
    MissingMergedSegments,
    #[error("Not all of the merged segments have the same tags")]
    InconsistentMergeSegmentTags,
    #[error("Not all of the merged segments have the same data store version")]
    InconsistentMergeDataStore,
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("FST error: {0}")]
    FstError(#[from] fst::Error),
    #[error("bincode error: {0}")]
    SerializationError(#[from] bincode::error::EncodeError),
}

pub type VectorR<O> = Result<O, VectorErr>;
