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

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

pub type OpenShardsMetric = Family<(), Gauge>;

pub fn register_open_shards_metric(registry: &mut Registry) -> OpenShardsMetric {
    let open_shards = OpenShardsMetric::default();
    registry.register("nucliadb_shard_cache_open", "Open shards", open_shards.clone());
    open_shards
}

pub type EvictedShardsMetric = Family<(), Counter>;

pub fn register_evicted_shards_metric(registry: &mut Registry) -> EvictedShardsMetric {
    let evicted_shards = EvictedShardsMetric::default();
    registry.register("nucliadb_shard_cache_evicted", "Evicted shards", evicted_shards.clone());
    evicted_shards
}
