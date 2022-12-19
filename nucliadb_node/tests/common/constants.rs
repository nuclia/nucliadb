use std::time::Duration;


pub const READER_HOST: &str = "127.0.0.1";
pub const READER_PORT: u16 = 18031;

pub const WRITER_HOST: &str = "127.0.0.1";
pub const WRITER_PORT: u16 = 18030;

pub const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
