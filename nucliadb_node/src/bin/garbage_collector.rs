use std::process::ExitCode;

use clap::Parser;
use nucliadb_node::shards::shard_writer::ShardWriter;
#[derive(Parser, Debug)]
struct Args {
    shard_id: String,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let shards_path = nucliadb_node::env::shards_path();
    let collect_at = shards_path.join(&args.shard_id);
    let Ok(shard) = ShardWriter::open(args.shard_id, &collect_at) else {
        return ExitCode::FAILURE;
    };
    match shard.gc() {
        Ok(_) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
