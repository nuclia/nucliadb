use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::SystemTime;

use super::{disk_handler, VectorR};
use crate::data_point::{DataPoint, DpId};

type SubsList = Arc<RwLock<BTreeSet<PathBuf>>>;

struct Updater {
    subs: SubsList,
}

impl Updater {
    const MERGE_WINDOW: u64 = 60;
    fn work_loop(&self, subscriber: &Path) -> VectorR<()> {
        let start = SystemTime::now();
        let mut time_left = Updater::MERGE_WINDOW;
        while time_left > 0 {
            let lock = disk_handler::shared_lock(subscriber)?;
            let state = disk_handler::load_state(&lock)?;
            std::mem::drop(lock);
            match state.get_work() {
                None => break,
                Some(work) => {
                    let ids: Vec<_> = work.iter().map(|journal| journal.id()).collect();
                    let new_dp = DataPoint::merge(subscriber, &ids, state.get_delete_log())?;
                    std::mem::drop(state);

                    let new_id = new_dp.meta().id();
                    let lock = disk_handler::exclusive_lock(subscriber)?;
                    let mut state = disk_handler::load_state(&lock)?;
                    state.replace_work_unit(new_dp);
                    disk_handler::persist_state(&lock, &state)?;
                    std::mem::drop(lock);
                    println!(
                        "Merge on {subscriber:?}:\n {}",
                        self.merge_report(&ids, new_id)
                    );
                    ids.into_iter()
                        .map(|dp| (subscriber, dp, DataPoint::delete(subscriber, dp)))
                        .filter(|(.., r)| r.is_err())
                        .for_each(|(s, id, ..)| tracing::info!("Error while deleting {s:?}/{id}"));
                }
            }
            time_left = time_left
                .checked_sub(start.elapsed().unwrap().as_secs())
                .unwrap_or_default();
        }
        Ok(())
    }
    fn merge_report(&self, old: &[DpId], new: DpId) -> String {
        use std::fmt::Write;
        let mut msg = String::new();
        for (id, dp_id) in old.iter().copied().enumerate() {
            writeln!(msg, "  ({id}) {dp_id}").unwrap();
        }
        write!(msg, "==> {new}").unwrap();
        msg
    }
    pub fn new(subs: SubsList) -> Updater {
        Updater { subs }
    }
    pub fn update(&self) -> VectorR<()> {
        let reader = self.subs.read().unwrap();
        let subscribers = reader.clone();
        std::mem::drop(reader);
        subscribers
            .iter()
            .map(|p| p.as_path())
            .try_for_each(|sub| self.work_loop(sub))?;
        Ok(())
    }
    pub fn run(self) {
        use std::time::Duration;
        loop {
            match self.update() {
                Err(err) => tracing::info!("Error while merging {}", err),
                Ok(_) => thread::sleep(Duration::from_secs(5)),
            }
        }
    }
}

#[derive(Clone)]
struct Subscriber {
    subs: SubsList,
}
impl Subscriber {
    fn subscribe(&self, subscriber: PathBuf) {
        let mut writer = self.subs.write().unwrap();
        writer.insert(subscriber);
    }
    pub fn new(subs: SubsList) -> Subscriber {
        Subscriber { subs }
    }
}

lazy_static::lazy_static! {
    static ref SUBSCRIBER: Subscriber = {
        let subs = Arc::new(RwLock::new(BTreeSet::new()));
        let updater = Updater::new(subs.clone());
        let subscriber = Subscriber::new(subs);
        thread::spawn(move || updater.run());
        subscriber
    };
}

pub fn subscribe(path: PathBuf) {
    SUBSCRIBER.subscribe(path);
}
