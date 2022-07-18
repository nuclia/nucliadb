use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentSlice {
    pub start: u64,
    pub end: u64,
}

pub struct Segment {
    mmaped: Mmap,
}

impl Segment {
    pub fn new(path: &Path) -> Segment {
        let file = OpenOptions::new().read(true).open(path).unwrap();
        Segment {
            mmaped: unsafe { Mmap::map(&file).unwrap() },
        }
    }
    pub fn get_vector(&self, slice: SegmentSlice) -> Option<&[u8]> {
        let range = (slice.start as usize)..(slice.end as usize);
        self.mmaped.get(range)
    }
}
