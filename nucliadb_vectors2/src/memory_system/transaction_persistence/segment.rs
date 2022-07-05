use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use memmap2::Mmap;

use crate::memory_system::elements::{MappedSegment, SegmentSlice, Vector};

mod file_name {
    pub const SEGMENT: &str = "segment.vectors";
}

pub struct SegmentWriter {
    file: File,
}
impl SegmentWriter {
    pub fn new(path: &Path) -> SegmentWriter {
        let path_inserted = path.join(file_name::SEGMENT);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path_inserted)
            .unwrap();
        SegmentWriter { file }
    }
    pub fn record_vector(&mut self, vector: &Vector) -> SegmentSlice {
        let start = self.file.metadata().unwrap().len();
        vector.write_into(&mut self.file);
        self.file.flush().unwrap();
        let end = self.file.metadata().unwrap().len();
        SegmentSlice { start, end }
    }
}

pub fn load_segment(path: &Path) -> MappedSegment {
    let path_inserted = path.join(file_name::SEGMENT);
    let inserted = OpenOptions::new().read(true).open(&path_inserted).unwrap();
    MappedSegment {
        segment: unsafe { Mmap::map(&inserted).unwrap() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn write_and_read_segment() {
        let dir = tempfile::tempdir().unwrap();
        let inserted = vec![
            Vector { raw: vec![0.0; 13] },
            Vector { raw: vec![1.0; 13] },
            Vector { raw: vec![2.0; 13] },
            Vector { raw: vec![3.0; 13] },
            Vector { raw: vec![4.0; 13] },
        ];
        let mut writer = SegmentWriter::new(dir.path());
        let vector_pointers: Vec<_> = inserted.iter().map(|v| writer.record_vector(v)).collect();
        let reader = load_segment(dir.path());
        let got_inserted: Vec<_> = vector_pointers
            .into_iter()
            .map_while(|p| reader.get_inserted(p))
            .map(Vector::read_from)
            .collect();
        assert_eq!(inserted, got_inserted);
    }
}
