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

use std::fmt;
use std::net::SocketAddr;

use derive_more::{Deref, DerefMut};
use futures::stream::{StreamExt, TryStreamExt};
use nucliadb_protos::fdbwriter::member::Type as NodeType;
use nucliadb_protos::node_reader_client::NodeReaderClient as GrpcClient;
use nucliadb_protos::{EmptyQuery, ShardList};
use reqwest::Client as HttpClient;
use serde::{de, Deserialize};
use tonic::Request;
use url::Url;

use crate::Error;

fn deserialize_protobuf_node_type<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<NodeType, D::Error> {
    struct Visitor;

    impl<'de> de::Visitor<'de> for Visitor {
        type Value = NodeType;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string containing 'IO', 'SEARCH', 'INGEST', 'TRAIN', 'UNKNOWN'")
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where E: de::Error {
            // deals with incompatiblities between API and protobuf representation
            match s {
                "IO" => Ok(NodeType::Io),
                "SEARCH" => Ok(NodeType::Search),
                "INGEST" => Ok(NodeType::Ingest),
                "TRAIN" => Ok(NodeType::Train),
                "UNKNOWN" => Ok(NodeType::Unknown),
                _ => Err(E::custom("Invalid '{s}' node type")),
            }
        }
    }

    deserializer.deserialize_any(Visitor)
}

async fn fetch_all_nodes(url: Url) -> Result<Vec<RawNode>, Error> {
    let http_client = HttpClient::new();

    Ok(http_client
        .get(url)
        .send()
        .await?
        .json::<Vec<RawNode>>()
        .await?)
}

async fn load_node(mut node: Node) -> Result<Node, Error> {
    let mut grpc_client = GrpcClient::connect(format!("http://{}", node.listen_address)).await?;
    let response = grpc_client.get_shards(Request::new(EmptyQuery {})).await?;

    let ShardList { shards } = response.into_inner();

    node.shards = shards.into_iter().map(Shard::from).collect();

    Ok(node)
}

/// The Nuclia's API node representation.
#[derive(Deserialize)]
pub struct RawNode {
    /// The node identifier.
    id: String,
    /// The `gRPC` listen address.
    listen_address: SocketAddr,
    /// The node type.
    #[serde(deserialize_with = "deserialize_protobuf_node_type")]
    r#type: NodeType,
    /// The last known node score.
    #[allow(dead_code)]
    load_score: f32,
    /// The last known number of shards in the node.
    #[allow(dead_code)]
    shard_count: u64,
    /// Indicates if the node is a dummy one.
    dummy: bool,
}

/// The internal shard representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shard {
    /// The shard identifier.
    id: String,
    /// The shard load score.
    load_score: u64,
}

impl Shard {
    /// Creates an empty shard, a.k.a a shard with a load score equals to zero.
    pub fn empty(id: String) -> Self {
        Self { id, load_score: 0 }
    }

    /// Creates a new shard.
    pub fn new(id: String, load_score: u64) -> Self {
        Self { id, load_score }
    }

    /// Returns the shard identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Indicates if the shard is empty or not.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.load_score == 0
    }

    /// Returns the shard load score.
    #[inline]
    pub fn load_score(&self) -> u64 {
        self.load_score
    }
}

impl From<nucliadb_protos::Shard> for Shard {
    fn from(shard: nucliadb_protos::Shard) -> Self {
        Self {
            id: shard.shard_id,
            load_score: shard.paragraphs,
        }
    }
}

/// The internal node representation.
#[derive(Debug, Clone)]
pub struct Node {
    /// The node identifier.
    id: String,
    /// The `gRPC` listen address.
    listen_address: SocketAddr,
    /// The list of shards linked to the node.
    shards: Vec<Shard>,
}

impl From<RawNode> for Node {
    fn from(node: RawNode) -> Self {
        Self {
            id: node.id,
            listen_address: node.listen_address,
            shards: Vec::default(),
        }
    }
}

impl Node {
    /// Fetchs all nodes known by Nuclia's API.
    ///
    /// Note that this associated function will remove all dummy and non IO nodes
    /// plus load the list of shards for each selected nodes.
    ///
    /// # Errors
    /// This associated function will returns an error if:
    /// - the Nuclia's API is not reachable
    /// - The JSON response is mal-formed
    pub async fn fetch_all(url: Url) -> Result<Vec<Self>, Error> {
        futures::stream::iter(fetch_all_nodes(url).await?)
            .filter(|node| futures::future::ready(node.r#type == NodeType::Io && !node.dummy))
            .map(|node| Ok(Node::from(node)))
            .and_then(load_node)
            .try_collect()
            .await
    }

    /// Creates a new node.
    pub fn new(id: String, listen_address: SocketAddr, shards: Vec<Shard>) -> Self {
        Self {
            id,
            listen_address,
            shards,
        }
    }

    /// Returns the node identifier.
    #[inline]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Returns the `gRPC` listen address.
    #[inline]
    pub fn listen_address(&self) -> SocketAddr {
        self.listen_address
    }

    /// Returns the node load score.
    ///
    /// Note that the node load score is basically the sum of all shard load scores.
    #[inline]
    pub fn load_score(&self) -> u64 {
        self.shards.iter().map(Shard::load_score).sum()
    }

    /// Returns an iterator over the node shards.
    #[inline]
    pub fn shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.iter()
    }

    /// Returns an iterator over the node active shards.
    ///
    /// Note that an active shard is a shard with a load score strictly superiors to zero.
    #[inline]
    pub fn active_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.iter().filter(|shard| !shard.is_empty())
    }

    /// Returns an iterator over the node empty shards.
    ///
    /// Note that an empty shard is a shard with a load score equals to zero.
    #[inline]
    pub fn empty_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.iter().filter(|shard| shard.is_empty())
    }

    /// Indicates if the node contains a shard replica of the given shard.
    #[inline]
    pub fn contains_shard_replica(&self, _shard: &Shard) -> bool {
        // TODO: load shard replicas for each shard
        self.shards.iter().any(|_shard| false)
    }

    /// Remove a shard from the node using the given shard identifier.
    pub fn remove_shard(&mut self, id: &str) -> Option<Shard> {
        self.shards
            .iter()
            .position(|shard| shard.id() == id)
            .map(|i| self.shards.remove(i))
    }

    /// Add the given shard to the node.
    pub fn add_shard(&mut self, shard: Shard) {
        self.shards.push(shard);
    }
}

/// A newtype over [`Node`] with a weigh metadata.
#[must_use]
#[derive(Debug, Copy, Clone, Deref, DerefMut)]
pub struct WeightedNode<'a> {
    /// The concrete node.
    #[deref]
    #[deref_mut]
    node: &'a Node,
    /// The node weight calculated by the [`Balancer`]
    weight: u64,
    /// The node position in the list of nodes.
    ///
    /// Note that position is only use to get rid of borrow checker restriction
    /// and modify the list of shards after the shard balancing.
    position: usize,
}

impl<'a> WeightedNode<'a> {
    /// Creates a new weighted node.
    pub fn new(node: &'a Node, weight: u64, position: usize) -> Self {
        Self {
            node,
            weight,
            position,
        }
    }

    /// Returns the node weight.
    #[inline]
    pub fn weight(&self) -> u64 {
        self.weight
    }

    /// Returns the node position.
    #[inline]
    pub fn position(&self) -> usize {
        self.position
    }
}
