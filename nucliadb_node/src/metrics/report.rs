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

use std::borrow::Cow;
use std::collections::HashMap;

use prometheus::proto::MetricFamily;
use prometheus::{labels, opts, IntGauge, Registry};

use super::Error;

/// A trait to provide mandatory report data to the `Publisher`.
pub trait Report {
    fn metrics(&self) -> Vec<MetricFamily>;
    fn labels(&self) -> HashMap<String, String>;
}

/// `NodeReport`, a structured representation of a node metrics.
pub struct NodeReport {
    id: Cow<'static, str>,
    registry: Registry,
    pub shard_count: IntGauge,
    pub paragraph_count: IntGauge,
}

impl NodeReport {
    pub fn new(id: impl Into<Cow<'static, str>>) -> Result<Self, Error> {
        let id = id.into();
        let registry = Registry::new();
        let shard_count =
            IntGauge::with_opts(opts!("shard_count", "Number of shards in the node"))?;
        let paragraph_count = IntGauge::with_opts(opts!(
            "paragraph_count",
            "The sum of all shard paragraphs in the node"
        ))?;

        registry.register(Box::new(shard_count.clone()))?;
        registry.register(Box::new(paragraph_count.clone()))?;

        Ok(Self {
            id,
            registry,
            shard_count,
            paragraph_count,
        })
    }

    pub fn score(&self) -> f32 {
        self.shard_count
            .get()
            .saturating_mul(self.paragraph_count.get()) as f32
            / 100.0
    }
}

impl Report for NodeReport {
    fn metrics(&self) -> Vec<MetricFamily> {
        self.registry.gather()
    }

    fn labels(&self) -> HashMap<String, String> {
        labels! { "node_id".to_string() => self.id.to_string() }
    }
}
