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

use uuid::Uuid;

/// Represents a field that has met all of the
/// pre-filtering requirements.
#[derive(Debug, Clone)]
pub struct FieldId {
    pub resource_id: Uuid,
    pub field_id: String,
}

/// Utility type to identify and allow optimizations in filtering edge cases
#[derive(Debug, Default, Clone)]
pub enum PrefilterResult {
    #[default]
    None,
    All,
    Some(Vec<FieldId>),
}
