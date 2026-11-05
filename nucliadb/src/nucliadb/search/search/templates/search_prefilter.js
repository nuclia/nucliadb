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
// First restrict processed fields using the legacy access-group predicate.
// null groups disables it; an empty group list would admit only public fields.
// The Python hybrid caller currently forces null: HYBRID SECURITY IS DISABLED.
// This is path-index filtering, not an implementation of native permissions.
const groups = ${groups};
const fieldSecurityQuery = groups === null ? cts.trueQuery() : cts.notQuery(deniedQuery(groups));
// Lexical/graph branches need directory scopes; vector-only searches leave
// those unmaterialized and use the eligibility query instead. When materialized,
// null means unrestricted and [] means no processed fields matched.
const fieldEligibilityQuery = ${field_query};
const fieldEligibilityFiltered = ${field_filtered};
const materializeFieldDirs = ${materialize_fields};
const fieldDirs = materializeFieldDirs ? ${field_dirs} : null;
const fieldsUnrestricted = fieldEligibilityQuery === null || (materializeFieldDirs && fieldDirs === null);
// Explicit field directories already exclude denied fields in an AND search.
// With no directory restriction, or with an OR paragraph branch that can bypass
// it, exclude denied resource subtrees separately. securityQuery is only the
// remaining restriction, not necessarily the complete security decision alone.
const denied = groups !== null && (fieldDirs === null || ${paragraph_or})
  ? deniedResources(groups) : [];
const securityQuery = denied.length
  ? cts.notQuery(cts.directoryQuery(denied, 'infinity')) : cts.trueQuery();