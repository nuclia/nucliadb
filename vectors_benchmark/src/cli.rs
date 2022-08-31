use clap::Parser;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use thiserror::Error;

mod out_files {
    pub const WRT: &str = "writer.data";
    pub const RDR: &str = "reader.data";
}

#[derive(Debug, Error)]
pub enum BenchErr {
    #[error("IO error: {0}")]
    IoErr(#[from] io::Error),
}
pub type BenchR<O> = Result<O, BenchErr>;

/// Nuclia benchmark for distrubed semantic engines
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Path to the .fvectors file that will populate the index
    #[clap(short, long)]
    vectors: String,
    /// Path where the output should be located
    #[clap(short, long, default_value_t = String::from("./"))]
    output: String,
    /// Dimension of the embeddings
    #[clap(short, long)]
    embedding_dim: usize,
    /// Number of results per query
    #[clap(short, long, default_value_t = 10)]
    neighbours: usize,
    /// Batch size
    #[clap(short, long, default_value_t = 5000)]
    batch_size: usize,
}

impl Args {
    pub fn new() -> Args {
        Args::parse()
    }
    pub fn vectors(&self) -> BenchR<File> {
        let path = PathBuf::from(&self.vectors);
        let file = OpenOptions::new().read(true).open(&path)?;
        Ok(file)
    }
    pub fn writer_plot(&self) -> BenchR<File> {
        let path = PathBuf::from(&self.output).join(out_files::WRT);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;
        Ok(file)
    }
    pub fn reader_plot(&self) -> BenchR<File> {
        let path = PathBuf::from(&self.output).join(out_files::RDR);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;
        Ok(file)
    }
    pub fn neighbours(&self) -> usize {
        self.neighbours
    }
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
    pub fn embedding_dim(&self) -> usize {
        self.embedding_dim
    }
}
