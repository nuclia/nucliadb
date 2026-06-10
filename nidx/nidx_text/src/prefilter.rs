// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
