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
  // Stage 1: Install the shared index names and helpers in this eval's scope.
  // The lexical, vector and optional graph branches reuse the same prefilter work.
  ${constants}
  ${helpers}

  // Stage 2: Resolve field matches and any security restriction not already
  // represented by those matches. Graph retrieval gets a resource-level scope.
  // HYBRID SECURITY IS CURRENTLY DISABLED by the Python caller, which supplies
  // null groups; these templates retain the legacy group-filtering machinery.
  ${prefilter}
  ${graph_resource_scope}

  // Stage 3: An empty field selection makes an AND search impossible. Do not
  // short-circuit OR searches, whose paragraph side may still match, or graph
  // searches, whose resource scope can match even when no processed fields do.
  const paragraphOr = ${paragraph_or};
  if (fieldDirs !== null && !fieldDirs.length && !paragraphOr && ${no_graph}) {
    return {chunks: [], embeddings: []};
  }
  // Stage 4: null fieldDirs means no field restriction; [] means match nothing.
  // A null fieldQuery is thus an absent predicate, not a false query. For AND,
  // intersect field and paragraph matches; for OR, union them. An unrestricted
  // field side makes that union unrestricted, even when paragraphs are filtered.
  const paragraphQuery = ${paragraph_query};
  const fieldQuery = fieldDirs === null ? null : dirsQuery(fieldDirs);
  const filterQuery = paragraphQuery === null ? fieldQuery :
    fieldQuery === null ? (paragraphOr ? null : paragraphQuery) :
    paragraphOr ? cts.orQuery([fieldQuery, paragraphQuery]) : cts.andQuery([fieldQuery, paragraphQuery]);
  // Chunk predicates apply directly to chunk documents. Embeddings cannot use
  // paragraph text/facet predicates directly: the vector branch joins matching
  // fields and chunks to embedding keys before scoring.
  const chunkScope = filterQuery === null ? securityQuery : cts.andQuery([securityQuery, filterQuery]);
  const embeddingScope = securityQuery;

  // Stage 5: Execute the enabled branches and return separate result families.
  // Scores are not fused here. The vector helper sets semanticMode to report
  // whether the returned semantic candidates came from ANN or an exact scan.
  return {
    chunks: ${chunks},
    embeddings: ${embeddings},
    graph: ${graph},
    semantic_mode: semanticMode
  };
})()