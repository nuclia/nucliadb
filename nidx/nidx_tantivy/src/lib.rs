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

//! This module provides an adapter between Tantivy and nidx in the following ways:
//!
//! - Indexing: Provides an index writer that can be used by the index to write to a single segment and have it return the appropriate metadata.
//! - Merging: A generic merger that can be used by any Tantivy index and reads/write the segments from where nidx expects to find them.
//! - Searching: Provides a `tantivy::Searcher` that can be used with the file format that the nidx uses, without moving files.
//!
//! The indexes for merging and searching also take charge of applying the deletions, so each individual index does not have to worry about it.

pub mod index_reader;
pub mod utils;

use nidx_types::SegmentMetadata;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tantivy::{Index, SingleSegmentIndexWriter, TantivyDocument, directory::MmapDirectory, schema::Schema};

#[derive(Clone, Serialize, Deserialize)]
pub struct TantivyMeta {
    pub segment_id: String,
}

pub type TantivySegmentMetadata = SegmentMetadata<TantivyMeta>;

pub struct TantivyIndexer {
    writer: SingleSegmentIndexWriter,
    output_path: PathBuf,
    has_documents: bool,
}

impl TantivyIndexer {
    pub fn new(output_dir: PathBuf, schema: Schema) -> anyhow::Result<Self> {
        let index_builder = Index::builder().schema(schema);
        let writer = index_builder.single_segment_index_writer(MmapDirectory::open(&output_dir)?, 15_000_000)?;
        Ok(Self {
            writer,
            output_path: output_dir,
            has_documents: false,
        })
    }

    pub fn add_document(&mut self, doc: TantivyDocument) -> tantivy::Result<()> {
        self.has_documents = true;
        self.writer.add_document(doc)
    }

    pub fn finalize(self) -> anyhow::Result<Option<SegmentMetadata<TantivyMeta>>> {
        if !self.has_documents {
            return Ok(None);
        }
        let index = self.writer.finalize()?;
        let segments = index.searchable_segment_metas()?;
        assert_eq!(segments.len(), 1);
        let segment = &segments[0];

        Ok(Some(SegmentMetadata {
            path: self.output_path,
            records: segment.max_doc() as usize,
            index_metadata: TantivyMeta {
                segment_id: segment.id().uuid_string(),
            },
        }))
    }
}
