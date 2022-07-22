use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

use fs2::FileExt;

pub struct Lock {
    file: File,
}

impl Lock {
    pub fn new(path: &Path) -> Lock {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        file.lock_exclusive().unwrap();
        Lock { file }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lock_unlock() {
        let dir = tempfile::tempdir().unwrap();
        let lock = Lock::new(&dir.path().join("state.lock"));
        std::mem::drop(lock);
    }
}
