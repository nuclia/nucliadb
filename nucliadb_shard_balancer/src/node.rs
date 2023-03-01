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

use std::net::SocketAddr;

use derive_more::{Deref, DerefMut};
use futures::stream::{StreamExt, TryStreamExt};
use nucliadb_protos::fdbwriter::member::Type as NodeType;
use nucliadb_protos::node_writer_client::NodeWriterClient as GrpcClient;
use nucliadb_protos::EmptyQuery;
use reqwest::Client as HttpClient;
use tonic::Request;

use crate::shard::Shard;
use crate::views::Node as NodeView;
use crate::Error;

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

impl Node {
    /// Get all nodes from Nuclia's API.
    ///
    /// Note that this associated function will remove all dummy and non IO nodes
    /// plus get the shards of all nodes.
    ///
    /// # Errors
    /// This associated function will returns an error if:
    /// - the Nuclia's API is not reachable
    /// - The JSON response is malformed
    pub async fn from_api(url: &str) -> Result<Vec<Self>, Error> {
        let http_client = HttpClient::new();

        let nodes = http_client
            .get(format!("http://search.{url}/chitchat/members"))
            .send()
            .await?
            .json::<Vec<NodeView>>()
            .await?;

        futures::stream::iter(nodes)
            .filter(|node| futures::future::ready(node.r#type == NodeType::Io && !node.dummy))
            .map(Ok)
            .and_then(Node::try_load)
            .try_collect::<Vec<_>>()
            .await
    }

    /// Loads the node from its view.
    async fn try_load(node: NodeView) -> Result<Node, Error> {
        todo!()

        // let url = format!("http://{}", node.listen_address);
        // let mut grpc_client = GrpcClient::connect(url).await?;

        // let metadata = grpc_client
        //     .get_metadata(Request::new(EmptyQuery {}))
        //     .await?
        //     .into_inner();

        // Ok(Node {
        //     id: node.id,
        //     listen_address: node.listen_address,
        //     shards: metadata
        //         .into_iter()
        //         .map(|(id, value)| Shard::new(id, value.load_score, value.kbid))
        //         .collect(),
        // })
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
        self.shards.iter().filter(|shard| shard.is_active())
    }

    /// Returns an iterator over the idle shards.
    ///
    /// Note that an idle shard is a shard with a load score equals to zero.
    #[inline]
    pub fn idle_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.iter().filter(|shard| !shard.is_active())
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
