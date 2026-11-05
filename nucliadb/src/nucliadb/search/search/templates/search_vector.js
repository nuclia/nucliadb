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
  let paragraphKeys = null;
  if (${filter_limit} && paragraphQuery !== null && !(paragraphOr && fieldQuery === null)) {
    // AND can narrow chunks by field first. OR must find paragraph matches
    // outside those fields too; security remains common to both alternatives.
    const chunkQuery = cts.andQuery([
      cts.collectionQuery(COLLECTIONS.chunks),
      securityQuery,
      ...(!paragraphOr && fieldQuery !== null ? [fieldQuery] : []),
      paragraphQuery
    ]);
    paragraphKeys = matchingUris(chunkQuery, ${paragraph_filtered}).map(chunkKey).filter(key => key !== null);
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
    fieldKeys: fieldDirs,
    paragraphKeys,
    paragraphOr,
    unrestricted: fieldDirs === null && paragraphKeys === null && groups === null,
    queryVector: vec.vector(${vector}),
    minScore: ${min_score},
    view: ${view},
    exactMax: ${exact_max},
    oversampling: ${oversampling}
  });
})()