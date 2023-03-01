use std::fmt;
use std::net::SocketAddr;

use nucliadb_protos::fdbwriter::member::Type as NodeType;
use serde::{de, Deserialize};

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

/// The Nuclia's API node representation.
#[derive(Deserialize)]
pub struct Node {
    /// The node identifier.
    pub id: String,
    /// The `gRPC` listen address.
    pub listen_address: SocketAddr,
    /// The node type.
    #[serde(deserialize_with = "deserialize_protobuf_node_type")]
    pub r#type: NodeType,
    /// The last known node score.
    #[allow(dead_code)]
    pub load_score: f32,
    /// The last known number of shards in the node.
    #[allow(dead_code)]
    pub shard_count: u64,
    /// Indicates if the node is a dummy one.
    pub dummy: bool,
}
