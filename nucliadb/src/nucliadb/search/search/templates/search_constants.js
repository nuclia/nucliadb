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
// Shared names come from Python's schema definitions, not request text. Each
// collection selects a document family: resources for resource/graph scoping,
// processed fields for prefilters, chunks for lexical/paragraph matching, and
// embeddings for semantic retrieval. Collection scoping is not authorization.
const COLLECTIONS = ${collections};
// Range-index paths address facets, copied resource facets and legacy access
// groups. They must agree with the indexed JSON paths used during ingestion.
const PATHS = ${paths};
// ANN views live in this schema; the vector branch selects a model/dimension
// view within it. Facet lexicon expansion uses the configured string collation.
const VECTOR_SCHEMA = ${vector_schema};
const COLLATION = ${collation};