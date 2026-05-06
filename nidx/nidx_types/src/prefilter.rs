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

use std::collections::HashSet;

use uuid::Uuid;

/// Represents a field that has met all of the
/// pre-filtering requirements.
#[derive(Debug, Clone)]
pub struct FieldId {
    pub resource_id: Uuid,
    /// `None` means all fields on this resource match (resource-granular).
    /// `Some(field_id)` means a specific field matches (field-granular).
    pub field_id: Option<String>,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum FilterOperator {
    #[default]
    And,
    Or,
}

/// Utility type to identify and allow optimizations in filtering edge cases
#[derive(Debug, Default, Clone)]
pub enum PrefilterResult {
    #[default]
    None,
    All,
    Some(Vec<FieldId>),
}

impl PrefilterResult {
    /// Combine `self` (the text/security prefilter result) with a set of
    /// resource UUIDs from the JSON prefilter using OR or AND semantics.
    pub fn combine(self, resource_uuids: HashSet<Uuid>, operator: FilterOperator) -> Self {
        if resource_uuids.is_empty() {
            return match operator {
                FilterOperator::Or => self,
                FilterOperator::And => PrefilterResult::None,
            };
        }

        let to_field_ids = |uuids: &HashSet<Uuid>| -> Vec<FieldId> {
            uuids
                .iter()
                .map(|&uuid| FieldId {
                    resource_id: uuid,
                    field_id: None,
                })
                .collect()
        };

        match (self, operator) {
            (PrefilterResult::All, FilterOperator::Or) => PrefilterResult::All,
            (PrefilterResult::None, FilterOperator::Or) => PrefilterResult::Some(to_field_ids(&resource_uuids)),
            (PrefilterResult::Some(fields), FilterOperator::Or) => {
                // All resources from the JSON prefilter pass at resource-level.
                // Fields only in the text prefilter (not covered by resource_uuids)
                // are kept at their original field-level granularity.
                let mut merged = to_field_ids(&resource_uuids);
                merged.extend(fields.into_iter().filter(|f| !resource_uuids.contains(&f.resource_id)));
                PrefilterResult::Some(merged)
            }
            (PrefilterResult::None, FilterOperator::And) => PrefilterResult::None,
            (PrefilterResult::All, FilterOperator::And) => PrefilterResult::Some(to_field_ids(&resource_uuids)),
            (PrefilterResult::Some(fields), FilterOperator::And) => {
                let filtered: Vec<FieldId> = fields
                    .into_iter()
                    .filter(|f| resource_uuids.contains(&f.resource_id))
                    .collect();
                if filtered.is_empty() {
                    PrefilterResult::None
                } else {
                    PrefilterResult::Some(filtered)
                }
            }
        }
    }
}
