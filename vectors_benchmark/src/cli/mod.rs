use clap::Parser;

/// Vectors Structural Integrity Check
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Path to the .fvectors file that will populate the index
    #[clap(short, long)]
    vectors: String,
    /// Path to the .fvectors file containing the queries
    #[clap(short, long)]
    queries: String,
    /// Path where the output should be located
    #[clap(short, long, default_value_t = String::from("./"))]
    output: String,
    /// Number of results per query
    #[clap(short, long, default_value_t = 10)]
    no_results: usize,
    /// Batch size
    #[clap(short, long, default_value_t = 5000)]
    batch_size: usize,
}
