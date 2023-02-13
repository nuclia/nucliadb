use std::fmt;
use std::net::SocketAddr;

use derive_more::{Deref, DerefMut};
use futures::stream::{StreamExt, TryStreamExt};
use futures::{future, stream};
use nucliadb_protos::fdbwriter::member::Type as NodeType;
use nucliadb_protos::node_reader_client::NodeReaderClient as GrpcClient;
use nucliadb_protos::{EmptyQuery, Shard, ShardList};
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
        where
            E: de::Error,
        {
            // deals with incompatiblity between API and protobuf representation
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

#[derive(Deserialize)]
pub struct RawNode {
    id: String,
    listen_address: SocketAddr,
    #[serde(deserialize_with = "deserialize_protobuf_node_type")]
    r#type: NodeType,
    load_score: f32,
    #[allow(dead_code)]
    shard_count: u64,
    dummy: bool,
}

#[derive(Deref, DerefMut)]
pub struct WeightedNode<'a> {
    #[deref]
    #[deref_mut]
    node: &'a Node,
    pub(crate) weight: u64,
}

impl<'a> WeightedNode<'a> {
    pub fn new(node: &'a Node, weight: u64) -> Self {
        Self { node, weight }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub(crate) id: String,
    pub(crate) listen_address: SocketAddr,
    pub(crate) load_score: f32,
    pub(crate) shards: Vec<Shard>,
}

impl From<RawNode> for Node {
    fn from(node: RawNode) -> Self {
        Self {
            id: node.id,
            listen_address: node.listen_address,
            load_score: node.load_score,
            shards: Vec::default(),
        }
    }
}

impl Node {
    pub async fn fetch_all(base_url: Url) -> Result<Vec<Self>, Error> {
        stream::iter(fetch_all_nodes(base_url).await?)
            .filter(|node| future::ready(node.r#type == NodeType::Io && !node.dummy))
            .map(|node| Ok(Node::from(node)))
            .and_then(load_node)
            .try_collect()
            .await
    }

    pub fn load_score(&self) -> u64 {
        self.load_score as u64
    }

    pub fn active_shards(&self) -> u64 {
        self.shards
            .iter()
            .filter(|shard| {
                !(shard.resources == 0 && shard.paragraphs == 0 && shard.sentences == 0)
            })
            .count() as u64
    }
}

async fn fetch_all_nodes(base_url: Url) -> Result<Vec<RawNode>, Error> {
    // this `unwrap` call is safe since we provide a well-defined URL path
    // and `base_url` has been validated upfront.
    let url = base_url.join("/chitchat/members").unwrap();
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

    node.shards = shards;

    Ok(node)
}
