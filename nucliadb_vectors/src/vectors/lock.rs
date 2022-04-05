use std::fmt;
use std::fs::File;
use std::path::PathBuf;
use std::time::Duration;

pub struct Lock {
    location: PathBuf,
}

impl fmt::Debug for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lock")
            .field("location", &self.location)
            .field("locked", &self.is_locked())
            .finish()
    }
}

impl Lock {
    pub fn open<T: Into<PathBuf>>(location: T) -> Result<Self, String> {
        let location = location.into();
        let dir = location.parent().unwrap();

        if !dir.exists() {
            return Err("Parent directory doesn't exist.".to_string());
        }

        Ok(Lock { location })
    }

    pub fn try_lock(&self) -> Result<(), String> {
        if self.is_locked() {
            return Err("Lock already adquired".to_string());
        }

        match File::create(&self.location) {
            Ok(_) => {
                debug!("Set lock file {:?}", self.location);
                Ok(())
            }
            Err(e) => {
                let message = format!("Error setting dirty file: {}", e);
                error!("{}", message);
                Err(message)
            }
        }
    }

    pub fn is_locked(&self) -> bool {
        self.location.exists()
    }

    pub fn unlock(&self) {
        match std::fs::remove_file(&self.location) {
            Ok(_) => debug!("Removed {:?}", self.location),
            Err(_) => trace!("Remove ignored, {:?} doesn't exist", self.location),
        }
    }

    pub fn lock(&self) {
        debug!("Triying to lock: {:?}", self.location);
        while self.is_locked() {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.try_lock().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use log::LevelFilter;
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use tokio::time::Instant;

    use super::Lock;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    fn get_random_string(length: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    fn get_temp_path() -> PathBuf {
        let temp_path = std::env::temp_dir();
        temp_path.join(get_random_string(7))
    }

    #[test]
    fn basic_lock() {
        let temp_file = get_temp_path();
        let lock = Lock::open(&temp_file).unwrap();

        assert!(lock.try_lock().is_ok());
        assert!(temp_file.exists());
        lock.unlock();
        assert!(!temp_file.exists());
    }

    #[test]
    fn try_second_lock() {
        let temp_file = get_temp_path();
        let lock1 = Lock::open(&temp_file).unwrap();
        let lock2 = Lock::open(&temp_file).unwrap();

        assert!(lock1.try_lock().is_ok());
        assert!(lock1.is_locked());
        assert!(lock2.try_lock().is_err());

        lock1.unlock();
        assert!(!lock1.is_locked());
        assert!(!lock2.is_locked());
        assert!(lock2.try_lock().is_ok());
        assert!(lock2.is_locked());
    }

    #[test]
    fn destroy_dont_delete_lock_file() {
        let temp_file = get_temp_path();
        {
            let lock = Lock::open(&temp_file).unwrap();
            assert!(lock.try_lock().is_ok());
        }

        assert!(temp_file.exists());

        let lock2 = Lock::open(&temp_file).unwrap();
        assert!(lock2.try_lock().is_err());

        lock2.unlock();
        assert!(lock2.try_lock().is_ok());
    }

    #[test]
    fn parent_dir_doesnt_exists() {
        let temp_file = "/tmp/this_dir_doesnt_exists/lock";
        let lock = Lock::open(&temp_file);
        assert!(lock.is_err());
    }

    #[test]
    fn multithreaded_lock() {
        init();
        let temp_file = get_temp_path();
        let lock1 = Lock::open(&temp_file).unwrap();
        let lock2 = Lock::open(&temp_file).unwrap();

        let t1 = std::thread::spawn(move || {
            // Task with a lot of work to do.
            assert!(lock1.try_lock().is_ok());
            info!("Job start");
            std::thread::sleep(Duration::from_millis(1000));
            info!("Job done");
            lock1.unlock();
        });

        let t2 = std::thread::spawn(move || {
            let time0 = Instant::now();
            // Task that waits for the other to finish.
            std::thread::sleep(Duration::from_millis(100));
            lock2.lock(); // Lock has to wait 3 seconds to the other task to finish.
            info!(
                "Finally I managed to get some work done. {:?}",
                time0.elapsed()
            );
            assert!(time0.elapsed() > Duration::from_secs(1));
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
