use super::VectorEngine;
pub struct NoEngine;
impl VectorEngine for NoEngine {
    fn add_batch(&mut self, batch_id: String, keys: Vec<String>, embeddings: Vec<Vec<f32>>) {
        println!("Received {batch_id}:");
        for (key, embedding) in keys.into_iter().zip(embeddings.into_iter()) {
            println!("{key}:  {embedding:?}");
        }
    }
    fn search(&self, _: usize, _: &[f32]) {
        println!("Im no engine, I cant do that");
    }
}
