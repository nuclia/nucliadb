use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use crate::memory_system::elements::Log;

mod file_names {
    pub const LOG: &str = "log.json";
}
pub fn save_log(path: &Path, graph_log: &Log) {
    let mut stream = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.join(file_names::LOG))
            .unwrap(),
    );
    serde_json::to_writer(&mut stream, graph_log).unwrap();
    stream.flush().unwrap();
}

pub fn load_log(path: &Path) -> Log {
    let mut stream = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(path.join(file_names::LOG))
            .unwrap(),
    );
    serde_json::from_reader(&mut stream).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persist_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log {
            fresh_segment: 0,
            max_layer: 0,
            entry_point: None,
        };
        save_log(dir.path(), &log);
        let tested = load_log(dir.path());
        assert_eq!(log.max_layer, tested.max_layer);
        assert_eq!(log.entry_point, tested.entry_point);
    }
}
