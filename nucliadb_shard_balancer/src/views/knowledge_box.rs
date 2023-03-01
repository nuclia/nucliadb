use std::collections::HashMap;

use serde::Deserialize;

use super::LogicShard;

#[derive(Deserialize)]
pub struct KnowledgeBox {
    #[serde(rename = "kbid")]
    pub id: String,
    #[allow(dead_code)]
    pub actual: usize,
    pub shards: Vec<LogicShard>,
}
