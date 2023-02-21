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

use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::{VectorErr, VectorR};

#[derive(Clone)]
pub struct MergerWriterSync(Arc<Mutex<()>>);
impl MergerWriterSync {
    pub fn new() -> MergerWriterSync {
        MergerWriterSync(Arc::new(Mutex::new(())))
    }
    pub fn try_to_start_working(&self) -> VectorR<MutexGuard<'_, ()>> {
        match self.0.try_lock() {
            Ok(lock) => Ok(lock),
            Err(TryLockError::Poisoned(poisoned)) => Ok(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => Err(VectorErr::WorkDelayed),
        }
    }
}
