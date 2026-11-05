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
(() => {
  // Evaluate field/resource predicates on processed fields, which carry field
  // metadata and copied resource metadata, rather than on every child chunk.
  const scope = cts.andQuery([
    cts.collectionQuery(COLLECTIONS.fieldsProcessed),
    fieldSecurityQuery
  ]);
  // Predicates such as text/phrases need filtered document verification; exact
  // index-resolvable predicates can enumerate matches through the URI lexicon.
  const uris = matchingUris(cts.andQuery([scope, ${query}]), ${filtered});
  // Avoid a large directory list when all allowed fields appear to match.
  // cts.estimate is an index estimate, not a verified document count. Preserve
  // [] for no matches; null is the sentinel for no remaining field restriction.
  if (uris.length && uris.length >= cts.estimate(scope)) return null;
  // Removing the processed-document suffix leaves a trailing-slash field URI
  // prefix. Recursive directory queries then include its chunks and embeddings.
  return uris.map(uri => uri.replace(/processed\.json$$/, ''));
})()