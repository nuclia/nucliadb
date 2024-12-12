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
mod data_point;
mod data_point_provider;
mod data_types;
mod formula;
mod indexer;
mod query_io;
mod utils;
mod vector_types;

use config::VectorConfig;
use data_point::open;
use data_point_provider::reader::{Reader, TimeSensitiveDLog};
use data_point_provider::DTrie;
use indexer::{index_resource, ResourceWrapper};
use nidx_protos::{Resource, VectorSearchRequest, VectorSearchResponse};
use nidx_types::{OpenIndexMetadata, SegmentMetadata};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use thiserror::Error;
use tracing::instrument;

pub use data_point_provider::reader::VectorsContext;
pub use indexer::SEGMENT_TAGS;

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
        let vectorset_resource = ResourceWrapper::new_vectorset_resource(resource, index_name, use_default_vectorset);
        index_resource(vectorset_resource, output_dir, config)
    }

    pub fn deletions_for_resource(&self, resource: &Resource) -> Vec<String> {
        resource.sentences_to_delete.clone()
    }

    #[instrument(name = "vector::merge", skip_all)]
    pub fn merge(
        &self,
        work_dir: &Path,
        config: VectorConfig,
        open_index: impl OpenIndexMetadata<VectorSegmentMeta>,
    ) -> anyhow::Result<VectorSegmentMetadata> {
        let mut delete_log = data_point_provider::DTrie::new();
        for (key, seq) in open_index.deletions() {
            delete_log.insert(key.as_bytes(), seq);
        }

        let segment_ids: Vec<_> = open_index
            .segments()
            .map(|(meta, seq)| {
                let open_dp = open(meta).unwrap();
                (
                    TimeSensitiveDLog {
                        dlog: &delete_log,
                        time: seq,
                    },
                    open_dp,
                )
            })
            .collect();

        // Do the merge
        let open_destination =
            data_point::merge(work_dir, &segment_ids.iter().map(|(a, b)| (a, b)).collect::<Vec<_>>(), &config)?;

        Ok(VectorSegmentMetadata {
            path: work_dir.to_path_buf(),
            records: open_destination.no_nodes(),
            index_metadata: VectorSegmentMeta {
                tags: open_destination.tags().clone(),
            },
        })
    }
}

pub struct VectorSearcher {
    reader: Reader,
}

impl VectorSearcher {
    #[instrument(name = "vector::open", skip_all)]
    pub fn open(config: VectorConfig, open_index: impl OpenIndexMetadata<VectorSegmentMeta>) -> anyhow::Result<Self> {
        let mut delete_log = DTrie::new();

        for (key, seq) in open_index.deletions() {
            delete_log.insert(key.as_bytes(), seq);
        }

        Ok(VectorSearcher {
            reader: Reader::open(open_index.segments().collect(), config, delete_log)?,
        })
    }

    #[instrument(name = "vector::search", skip_all)]
    pub fn search(
        &self,
        request: &VectorSearchRequest,
        context: &VectorsContext,
    ) -> anyhow::Result<VectorSearchResponse> {
        self.reader.search(request, context)
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
    #[error("Can not merge zero datapoints")]
    EmptyMerge,
    #[error("Inconsistent dimensions. Index={index_config} Vector={vector}")]
    InconsistentDimensions {
        index_config: usize,
        vector: usize,
    },
    #[error("UTF8 decoding error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Some of the merged segments were not found")]
    MissingMergedSegments,
    #[error("Not all of the merged segments have the same tags")]
    InconsistentMergeSegmentTags,
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(&'static str),
}

pub type VectorR<O> = Result<O, VectorErr>;
