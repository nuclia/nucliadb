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

use nidx_protos::{FilterExpression, Security};

#[derive(Clone, Default)]
pub struct DocumentSearchRequest {
    pub body: String,
    pub order: Option<nidx_protos::OrderBy>,
    pub faceted: Option<nidx_protos::Faceted>,
    pub result_per_page: i32,
    pub only_faceted: bool,
    pub advanced_query: Option<String>,
    pub min_score: f32,
    pub filter_expression: Option<FilterExpression>,
    pub security: Option<Security>,
}
