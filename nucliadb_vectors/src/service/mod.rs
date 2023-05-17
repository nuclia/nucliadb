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

pub mod reader;
pub mod writer;

use std::fs::File;

use fs2::FileExt;
use nucliadb_core::protos::VectorSimilarity as GrpcSimilarity;
pub use reader::*;
pub use writer::*;

use crate::data_point::Similarity;
use crate::VectorR;

const SET_LOCK: &str = "rest.lock";

enum MaybeLocked<'service, V> {
    NoLock(&'service V),
    WithLock(&'service File, V),
}
impl<'service, V> Drop for MaybeLocked<'service, V> {
    fn drop(&mut self) {
        let MaybeLocked::WithLock(lock, _) = self else {
            return;
        };
        let _ = lock.unlock();
    }
}
impl<'service, V> MaybeLocked<'service, V> {
    pub fn no_lock(inner: &'service V) -> MaybeLocked<'service, V> {
        Self::NoLock(inner)
    }
    pub fn with_shared_lock(inner: V, lock: &'service File) -> VectorR<MaybeLocked<'service, V>> {
        lock.lock_shared()?;
        Ok(Self::WithLock(lock, inner))
    }
    pub fn inner(&self) -> &V {
        match self {
            Self::WithLock(_, inner) => inner,
            Self::NoLock(inner) => inner,
        }
    }
}

impl From<GrpcSimilarity> for Similarity {
    fn from(value: GrpcSimilarity) -> Self {
        match value {
            GrpcSimilarity::Cosine => Similarity::Cosine,
            GrpcSimilarity::Dot => Similarity::Dot,
        }
    }
}
