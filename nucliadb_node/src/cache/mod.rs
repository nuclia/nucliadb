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

//! Shard provider using an unbounded cache.
//!
//! It can contain all shard readers/writers from its index node, so it could be
//! memory expensive. However, it's an easy implementation that speeds up
//! operations.
//!
//! For faster reads at cost of slower initialization and memory consumption,
//! all shards can be loaded at initialization time.

mod entity_cache;
mod reader_cache;
mod resource_cache;
mod writer_cache;

use crate::metadata_entity::Metadata;
use crate::paragraph_entity::reader::ParagraphRPointer;
use crate::paragraph_entity::writer::ParagraphWPointer;
use crate::relation_entity::reader::RelationRPointer;
use crate::relation_entity::writer::RelationWPointer;
use crate::text_entity::reader::TextRPointer;
use crate::text_entity::writer::TextWPointer;
use crate::vector_entity::reader::VectorRPointer;
use crate::vector_entity::writer::VectorWPointer;

pub enum WriteEntity {
    VectorEntity(VectorWPointer),
    ParagraphEntity(ParagraphWPointer),
    TextEntity(TextWPointer),
    RelationEntity(RelationWPointer),
    MetadataEntity(Metadata),
}

pub enum ReadEntity {
    VectorEntity(VectorRPointer),
    ParagraphEntity(ParagraphRPointer),
    TextEntity(TextRPointer),
    RelationEntity(RelationRPointer),
    MetadataEntity(Metadata),
}

pub type ReadersCache = entity_cache::Cache<ReadEntity>;
pub type WritersCache = entity_cache::Cache<WriteEntity>;

// This needs to go
pub use reader_cache::ShardReaderCache;
pub use writer_cache::ShardWriterCache;
