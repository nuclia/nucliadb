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

use config::VectorConfig;
use data_point::open;
use data_point_provider::reader::{Reader, TimeSensitiveDLog, VectorsContext};
use data_point_provider::DTrie;
use indexer::index_resource;
use nidx_protos::{Resource, VectorSearchRequest, VectorSearchResponse};
use nidx_types::{OpenIndexMetadata, SegmentMetadata};
use std::collections::HashSet;
use std::path::Path;

type VectorSegmentMetadata = SegmentMetadata<()>;

pub struct VectorIndexer;

impl VectorIndexer {
    pub fn index_resource(
        &self,
        output_dir: &Path,
        config: VectorConfig,
        resource: &Resource,
    ) -> anyhow::Result<Option<VectorSegmentMetadata>> {
        index_resource(resource.into(), output_dir, &config)
    }

    pub fn deletions_for_resource(&self, resource: &Resource) -> Vec<String> {
        resource.sentences_to_delete.clone()
    }

    pub fn merge(
        &self,
        work_dir: &Path,
        config: VectorConfig,
        open_index: impl OpenIndexMetadata<()>,
    ) -> anyhow::Result<VectorSegmentMetadata> {
        // TODO: Maybe segments should not get a DTrie of deletions and just a hashset of them, and we can handle building that here?
        // Wait and see how the Tantivy indexes turn out
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
            tags: HashSet::new(),
            index_metadata: (),
        })
    }
}

pub struct VectorSearcher {
    reader: Reader,
}

impl VectorSearcher {
    pub fn open(config: VectorConfig, open_index: impl OpenIndexMetadata<()>) -> anyhow::Result<Self> {
        let mut delete_log = DTrie::new();

        for (key, seq) in open_index.deletions() {
            delete_log.insert(key.as_bytes(), seq);
        }

        Ok(VectorSearcher {
            reader: Reader::open(open_index.segments().collect(), config, delete_log)?,
        })
    }

    pub fn search(
        &self,
        request: &VectorSearchRequest,
        context: &VectorsContext,
    ) -> anyhow::Result<VectorSearchResponse> {
        self.reader.search(request, context)
    }
}

//
// nidx_vector code
//
pub mod config;
pub mod data_point;
pub mod data_point_provider;
mod data_types;
pub mod formula;
pub mod indexer;
mod query_io;
mod utils;
mod vector_types;

use thiserror::Error;
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
    #[error("Inconsistent dimensions")]
    InconsistentDimensions,
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
