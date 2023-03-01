use itertools::Itertools;
use reqwest::Client as HttpClient;
use serde::Deserialize;

use crate::index::{Indexable, ReversedIndex};
use crate::node::Node;
use crate::views::KnowledgeBox as KnowledgeBoxView;
use crate::Error;

#[derive(Debug)]
pub struct Index;

impl Indexable for Index {
    type Item = String;
}

/// A reversed index storing information about shard replicas.
pub type ShardIndex = ReversedIndex<Index>;

impl ShardIndex {
    /// Get all shard replicas from Nuclia's API for the given list of nodes.
    ///
    /// # Errors
    /// This associated function will return an error if:
    /// - The Nuclia's API is not reachable
    /// - The JSON response is malformed
    pub async fn from_api(url: &str, nodes: &[Node]) -> Result<Self, Error> {
        let mut shard_replicas = Vec::default();
        let http_client = HttpClient::new();

        // iterates over all unique KBs only
        for kb_id in nodes
            .iter()
            .flat_map(Node::shards)
            .map(Shard::kb_id)
            .unique()
        {
            let url = format!("http://search.{url}/api/v1/kb/{kb_id}/shards");

            let kb = http_client
                .get(url)
                .header("X-NUCLIADB-ROLES", "MANAGER")
                .send()
                .await?
                .json::<KnowledgeBoxView>()
                .await?;

            for shard in kb.shards {
                shard_replicas.push(
                    shard
                        .replicas
                        .into_iter()
                        .map(|replica| replica.shard.id)
                        .collect::<Vec<_>>(),
                );
            }
        }

        Ok(Self::new(shard_replicas))
    }
}

/// The internal shard representation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Shard {
    /// The shard identifier.
    id: String,
    /// The shard load score.
    load_score: u64,
    /// The knowledge box the shard belongs to.
    kb_id: String,
}

impl Shard {
    /// Creates a new shard.
    pub fn new(id: String, load_score: u64, kb_id: String) -> Self {
        Self {
            id,
            load_score,
            kb_id,
        }
    }

    #[inline]
    pub fn kb_id(&self) -> &str {
        self.kb_id.as_str()
    }

    /// Returns the shard identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Indicates if the shard is active or not.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.load_score > 0
    }

    /// Returns the shard load score.
    #[inline]
    pub fn load_score(&self) -> u64 {
        self.load_score
    }
}
