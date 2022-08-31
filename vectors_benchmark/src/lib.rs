pub mod cli;
pub mod engines;
pub mod plot_writer;
pub mod query_iter;
pub mod reader;
pub mod vector_iter;
pub mod writer;

pub mod cli_interface {
    pub use super::cli::Args;
    pub use super::engines;
    pub use super::plot_writer::*;
    pub use super::query_iter::*;
    pub use super::reader;
    pub use super::vector_iter::*;
    pub use super::writer;
}

pub trait VectorEngine {
    fn add_batch(&mut self, batch_id: String, keys: Vec<String>, embeddings: Vec<Vec<f32>>);
    fn search(&self, no_results: usize, query: &[f32]);
}
