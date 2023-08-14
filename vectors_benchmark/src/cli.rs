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
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::PathBuf;

use clap::Parser;
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
    #[clap(short, long, default_value_t = 100000)]
    index: usize,
    /// Path where the output should be located
    #[clap(short, long, default_value_t = String::from("./"))]
    output: String,
    /// Dimension of the embeddings
    #[clap(short, long, default_value_t = 128)]
    embedding_dim: usize,
    /// Number of results per query
    #[clap(short, long, default_value_t = 10)]
    neighbours: usize,
    /// Batch size
    #[clap(short, long, default_value_t = 5000)]
    batch_size: usize,
    /// Path of the json output file
    #[clap(short, long, default_value_t = String::from("./benchmark.json"))]
    json_output: String,
}

impl Default for Args {
    fn default() -> Self {
        Args::new()
    }
}
impl Args {
    pub fn new() -> Args {
        Args::parse()
    }
    pub fn index_len(&self) -> usize {
        self.index
    }
    pub fn writer_plot(&self) -> BenchR<File> {
        let command = format!(
            "plot \"{}\" u 1:2 with linespoints ls 1;\npause -1",
            out_files::WRT
        );
        let plot = PathBuf::from(&self.output).join("plot_writer.gp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(plot)?;
        write!(file, "{command}")?;

        let path = PathBuf::from(&self.output).join(out_files::WRT);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(file)
    }
    pub fn reader_plot(&self) -> BenchR<File> {
        let command = format!(
            "plot \"{}\" u 1:2 with linespoints ls 1;\npause -1",
            out_files::RDR
        );
        let plot = PathBuf::from(&self.output).join("plot_reader.gp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(plot)?;
        write!(file, "{command}")?;

        let path = PathBuf::from(&self.output).join(out_files::RDR);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
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
    pub fn json_output(&self) -> String {
        self.json_output.clone()
    }
}
