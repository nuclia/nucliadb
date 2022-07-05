use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Lines, Write};
use std::path::Path;

use crate::memory_system::elements::Node;

mod file_name {
    pub const DELETED: &str = "delete.log";
}

pub struct DeleteLogWriter {
    deleted: File,
}

impl DeleteLogWriter {
    pub fn new(path: &Path) -> DeleteLogWriter {
        let path_deleted = path.join(file_name::DELETED);
        let deleted = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path_deleted)
            .unwrap();
        DeleteLogWriter { deleted }
    }
    pub fn record_delete(&mut self, entry: Node) {
        let encoded = serde_json::to_string(&entry).unwrap().replace('\n', " ");
        self.deleted.write_all(encoded.as_bytes()).unwrap();
        self.deleted.write_all(b"\n").unwrap();
        self.deleted.flush().unwrap();
    }
}

pub struct DeletedLogReader {
    deleted: Lines<BufReader<File>>,
}

impl std::iter::Iterator for DeletedLogReader {
    type Item = Node;
    fn next(&mut self) -> Option<Self::Item> {
        self.deleted
            .next()
            .map(|l| l.unwrap())
            .map(|s| serde_json::from_str(&s).unwrap())
    }
}

impl DeletedLogReader {
    pub fn new(path: &Path) -> DeletedLogReader {
        let path_deleted = path.join(file_name::DELETED);
        let deleted = OpenOptions::new().read(true).open(&path_deleted).unwrap();
        DeletedLogReader {
            deleted: BufReader::new(deleted).lines(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::memory_system::elements::SegmentSlice;
    #[test]
    pub fn write_and_read_log() {
        let dir = tempfile::tempdir().unwrap();
        let deleted = HashSet::from([
            Node {
                segment: 0,
                vector: SegmentSlice { start: 0, end: 0 },
            },
            Node {
                segment: 1,
                vector: SegmentSlice { start: 1, end: 1 },
            },
            Node {
                segment: 2,
                vector: SegmentSlice { start: 2, end: 2 },
            },
            Node {
                segment: 3,
                vector: SegmentSlice { start: 3, end: 3 },
            },
        ]);
        let mut writer = DeleteLogWriter::new(dir.path());
        deleted.iter().for_each(|v| writer.record_delete(*v));
        let reader = DeletedLogReader::new(dir.path());
        let got_deleted: HashSet<_> = reader.into_iter().collect();
        assert_eq!(deleted, got_deleted);
    }
}
