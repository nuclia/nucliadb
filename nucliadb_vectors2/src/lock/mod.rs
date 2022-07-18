use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

use fs2::FileExt;

struct Locked;
struct Unlocked;

pub struct Lock<State> {
    file: File,
    state: PhantomData<State>,
}

impl Lock<Unlocked> {
    pub fn new(path: &Path) -> Lock<Unlocked> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        Lock {
            file,
            state: PhantomData,
        }
    }
    pub fn lock(self) -> Lock<Locked> {
        self.file.lock_exclusive().unwrap();
        Lock {
            file: self.file,
            state: PhantomData,
        }
    }
}

impl Lock<Locked> {
    pub fn unlock(self) -> Lock<Unlocked> {
        self.file.unlock().unwrap();
        Lock {
            file: self.file,
            state: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lock_unlock() {
        let dir = tempfile::tempdir().unwrap();
        let lock = Lock::new(&dir.path().join("state.lock"));
        let locked = lock.lock();
        let _unlocked = locked.unlock();
    }
}
