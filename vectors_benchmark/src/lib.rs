pub mod engines;
pub mod plot_writer;
pub mod reader;
pub mod vector_iter;
pub mod writer;
pub mod cli;

pub trait VectorEngine {
    fn add_batch(&mut self, batch_id: String, keys: Vec<String>, embeddings: Vec<Vec<f32>>);
    fn search(&self, no_results: usize, query: &[f32]);
}
