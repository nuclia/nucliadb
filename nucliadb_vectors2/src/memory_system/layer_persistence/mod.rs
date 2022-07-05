use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use crate::memory_system::elements::GraphLayer;

mod file_names {
    pub fn layer_file(layer_no: usize) -> String {
        format!("{layer_no}.bincode")
    }
}

pub fn save_layer(path: &Path, layer_no: usize, layer: &GraphLayer) {
    let mut stream = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.join(&file_names::layer_file(layer_no)))
            .unwrap(),
    );
    bincode::serialize_into(&mut stream, layer).unwrap();
    stream.flush().unwrap();
}

pub fn load_layer(path: &Path, layer_no: usize) -> GraphLayer {
    let mut stream = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(path.join(&file_names::layer_file(layer_no)))
            .unwrap(),
    );
    bincode::deserialize_from(&mut stream).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persist_layer() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphLayer::new();
        save_layer(dir.path(), 0, &graph);
        save_layer(dir.path(), 1, &graph);
        save_layer(dir.path(), 2, &graph);
        save_layer(dir.path(), 3, &graph);
        let tested0 = load_layer(dir.path(), 0);
        let tested1 = load_layer(dir.path(), 1);
        let tested2 = load_layer(dir.path(), 2);
        let tested3 = load_layer(dir.path(), 3);
        assert_eq!(graph.cnx, tested0.cnx);
        assert_eq!(graph.cnx, tested1.cnx);
        assert_eq!(graph.cnx, tested2.cnx);
        assert_eq!(graph.cnx, tested3.cnx);
    }
}
