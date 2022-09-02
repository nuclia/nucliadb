use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Stats {
    pub writing_time: u128,
    pub read_time: u128,
    pub tagged_time: u128,
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string_pretty(self).unwrap();
        f.write_str(&json)
    }
}
