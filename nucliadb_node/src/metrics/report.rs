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
}

impl Report for NodeReport {
    fn metrics(&self) -> Vec<MetricFamily> {
        self.registry.gather()
    }

    fn labels(&self) -> HashMap<String, String> {
        labels! { "node_id".to_string() => self.id.to_string() }
    }
}
