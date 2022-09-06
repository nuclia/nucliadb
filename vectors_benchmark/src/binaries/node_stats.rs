// use nucliadb_node::reader::NodeReaderService;
use std::collections::HashMap;
use std::time::SystemTime;

use nucliadb_node::writer::NodeWriterService;
use nucliadb_protos::{
    resource, IndexMetadata, IndexParagraph, IndexParagraphs, Resource, ResourceId, ShardId,
    VectorSentence,
};
use vectors_benchmark::random_vectors::RandomVectors;
use vectors_benchmark::stats::Stats;
const BATCH_SIZE: usize = 5000;
const NO_LABELS: usize = 5;
// const NO_NEIGHBOURS: usize = 5;
const INDEX_SIZE: usize = 1000000;
const VECTOR_DIM: usize = 128;

fn label_set(batch_id: &str) -> Vec<String> {
    (0..NO_LABELS)
        .into_iter()
        .map(|l| format!("L{batch_id}_{l}"))
        .collect()
}

fn create_vector_sentences(prefix: &str) -> HashMap<String, VectorSentence> {
    RandomVectors::new(VECTOR_DIM)
        .take(BATCH_SIZE)
        .enumerate()
        .map(|(id, vector)| (format!("{prefix}/{id}"), VectorSentence { vector }))
        .collect()
}
fn create_index_paragraph(prefix: String) -> IndexParagraphs {
    let paragraph = IndexParagraph {
        start: 0,
        end: 0,
        labels: label_set(&prefix),
        sentences: create_vector_sentences(&prefix),
        field: "".to_string(),
        split: "".to_string(),
        index: 0,
    };
    IndexParagraphs {
        paragraphs: HashMap::from([(format!("{prefix}_0"), paragraph)]),
    }
}
fn create_index_paragraphs(prefix: String) -> HashMap<String, IndexParagraphs> {
    HashMap::from([(prefix.clone(), create_index_paragraph(prefix))])
}

fn create_reource(shard_id: String) -> Resource {
    let uuid = uuid::Uuid::new_v4();
    let paragraph = format!("{uuid}_para");
    Resource {
        resource: Some(ResourceId {
            shard_id: shard_id.clone(),
            uuid: uuid.to_string(),
        }),
        metadata: Some(IndexMetadata {
            modified: Some(SystemTime::now().into()),
            created: Some(SystemTime::now().into()),
        }),
        texts: HashMap::default(),
        labels: vec![],
        status: resource::ResourceStatus::Pending as i32,
        paragraphs: create_index_paragraphs(paragraph.to_string()),
        paragraphs_to_delete: vec![],
        sentences_to_delete: vec![],
        relations: vec![],
        relations_to_delete: vec![],
        shard_id: shard_id,
    }
}
#[tokio::main]
async fn main() {
    let mut metrics = Stats::default();
    let mut writer = NodeWriterService::new();
    let shard = writer.new_shard().await;
    println!("{shard:?}");
    let shard_id = ShardId { id: shard.id };
    let time = SystemTime::now();
    for id in 0..(INDEX_SIZE / BATCH_SIZE) {
        let r_id = shard_id.id.clone();
        let resource = create_reource(format!("{r_id}_{id}"));
        println!("Resource: {}", resource.resource.clone().unwrap().uuid);
        writer
            .set_resource(&shard_id, &resource)
            .await
            .unwrap()
            .unwrap();
    }
    metrics.writing_time = time.elapsed().unwrap().as_millis();
    println!("{metrics}");
}
