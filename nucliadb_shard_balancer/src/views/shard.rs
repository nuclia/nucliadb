use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct LogicShard {
    #[serde(rename = "shard")]
    pub id: String,
    pub replicas: Vec<ShardReplica>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardReplica {
    #[serde(rename = "node")]
    pub node_id: String,
    pub shard: VirtualShard,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VirtualShard {
    pub id: String,
    pub document_service: String,
    pub paragraph_service: String,
    pub vector_service: String,
    pub relation_service: String,
}
