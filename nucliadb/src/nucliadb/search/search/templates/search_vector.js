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
  const fieldPlan = fieldsUnrestricted ? null : matchingKeyPlan(cts.andQuery([
    cts.collectionQuery(COLLECTIONS.fieldsProcessed), fieldSecurityQuery, fieldEligibilityQuery
  ]), fieldEligibilityFiltered, 'field_key');
  let paragraphPlan = null;
  if (${filter_limit} && paragraphQuery !== null && !(paragraphOr && fieldsUnrestricted)) {
    const chunkQuery = cts.andQuery([
      cts.collectionQuery(COLLECTIONS.chunks),
      securityQuery,
      paragraphQuery
    ]);
    paragraphPlan = matchingKeyPlan(chunkQuery, ${paragraph_filtered}, 'paragraph_key');
  }
  const model = ${model};
  const scope = cts.andQuery([
    cts.collectionQuery(COLLECTIONS.embeddings),
    embeddingScope,
    cts.jsonPropertyValueQuery('model_id', model, ['exact'])
  ]);
  return semanticSearch({
    limit: ${limit},
    scope,
    fieldPlan,
    paragraphPlan,
    paragraphOr,
    queryVector: vec.vector(${vector}),
    minScore: ${min_score},
    exactMax: ${exact_max},
    view: ${view}
  });
})()