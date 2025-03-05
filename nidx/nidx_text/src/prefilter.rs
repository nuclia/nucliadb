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

use nidx_protos::{FilterExpression, Security, prost_types::Timestamp};

/// A field has two dates
#[derive(Debug, Clone, Copy)]
pub enum FieldDateType {
    /// When the field was created
    Created,
    /// When the field was modified
    Modified,
}

/// Used to define the time range of interest
#[derive(Debug, Clone)]
pub struct TimestampFilter {
    pub applies_to: FieldDateType,
    pub from: Option<Timestamp>,
    pub to: Option<Timestamp>,
}

/// A query plan pre-filtering stage.
/// It is useful for reducing the space of results
/// for the rest of the plan.
#[derive(Debug, Clone)]
pub struct PreFilterRequest {
    pub security: Option<Security>,
    pub filter_expression: Option<FilterExpression>,
}
