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
// Graph traversal needs resource documents, not processed-field or paragraph
// matches. Python translates supported resource/facet expressions into this
// restriction; unsupported field-only leaves become false predicates there.
// The separate scope allows graph results when the field prefilter is empty.
// Security is the legacy group predicate if supplied, but the hybrid Python
// caller currently disables it. No native-permission policy is implemented here.
const graphResourceQuery = cts.andQuery([
  cts.collectionQuery(COLLECTIONS.resources),
  ${security},
  ${restriction}
]);