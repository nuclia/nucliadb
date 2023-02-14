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
pub mod fs_state;
pub mod paragraphs;
pub mod relations;
pub mod texts;
pub mod vectors;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub use anyhow::{anyhow as node_error, Context};
use nucliadb_protos::{Resource, ResourceId};
pub type NodeResult<O> = anyhow::Result<O>;

pub fn paragraph_write(
    x: &paragraphs::ParagraphsWriterPointer,
) -> RwLockWriteGuard<'_, dyn paragraphs::ParagraphWriter + 'static> {
    x.write().unwrap_or_else(|l| l.into_inner())
}

pub fn text_write(
    x: &texts::TextsWriterPointer,
) -> RwLockWriteGuard<'_, dyn texts::FieldWriter + 'static> {
    x.write().unwrap_or_else(|l| l.into_inner())
}

pub fn vector_write(
    x: &vectors::VectorsWriterPointer,
) -> RwLockWriteGuard<'_, dyn vectors::VectorWriter + 'static> {
    x.write().unwrap_or_else(|l| l.into_inner())
}

pub fn relation_write(
    x: &relations::RelationsWriterPointer,
) -> RwLockWriteGuard<'_, dyn relations::RelationWriter + 'static> {
    x.write().unwrap_or_else(|l| l.into_inner())
}

pub fn paragraph_read(
    x: &paragraphs::ParagraphsWriterPointer,
) -> RwLockReadGuard<'_, dyn paragraphs::ParagraphWriter + 'static> {
    x.read().unwrap_or_else(|l| l.into_inner())
}

pub fn text_read(
    x: &texts::TextsWriterPointer,
) -> RwLockReadGuard<'_, dyn texts::FieldWriter + 'static> {
    x.read().unwrap_or_else(|l| l.into_inner())
}

pub fn vector_read(
    x: &vectors::VectorsWriterPointer,
) -> RwLockReadGuard<'_, dyn vectors::VectorWriter + 'static> {
    x.read().unwrap_or_else(|l| l.into_inner())
}

pub fn relation_read(
    x: &relations::RelationsWriterPointer,
) -> RwLockReadGuard<'_, dyn relations::RelationWriter + 'static> {
    x.read().unwrap_or_else(|l| l.into_inner())
}

pub mod protos {
    pub use nucliadb_protos::*;
    pub use {prost, prost_types};
}

pub mod tracing {
    pub use tracing::*;
}

pub mod thread {
    pub use rayon::prelude::*;
    pub use rayon::*;
}

pub mod prelude {
    pub use crate::paragraphs::{self, *};
    pub use crate::relations::{self, *};
    pub use crate::texts::{self, *};
    pub use crate::vectors::{self, *};
    pub use crate::{
        encapsulate_reader, encapsulate_writer, node_error, paragraph_read, paragraph_write,
        relation_read, relation_write, text_read, text_write, vector_read, vector_write, Context,
        NodeResult, ReaderChild, WriterChild,
    };
}

pub fn encapsulate_reader<T>(reader: T) -> Arc<T> {
    Arc::new(reader)
}

pub fn encapsulate_writer<T>(writer: T) -> Arc<RwLock<T>> {
    Arc::new(RwLock::new(writer))
}

pub trait WriterChild: std::fmt::Debug + Send + Sync {
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()>;
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()>;
    fn garbage_collection(&mut self) -> NodeResult<()>;
    fn stop(&mut self) -> NodeResult<()>;
    fn count(&self) -> NodeResult<usize>;
}

pub trait ReaderChild: std::fmt::Debug + Send + Sync {
    type Request;
    type Response;
    fn search(&self, request: &Self::Request) -> NodeResult<Self::Response>;
    fn reload(&self);
    fn stored_ids(&self) -> NodeResult<Vec<String>>;
    fn stop(&self) -> NodeResult<()>;
}
