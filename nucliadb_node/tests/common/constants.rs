use std::time::Duration;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use once_cell::sync::Lazy;

const READER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const READER_PORT: u16 = 18031;
pub const READER_ADDR: Lazy<SocketAddr> = Lazy::new(
    || SocketAddr::new(READER_IP, READER_PORT)
);

const WRITER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const WRITER_PORT: u16 = 18030;
pub const WRITER_ADDR: Lazy<SocketAddr> = Lazy::new(
    || SocketAddr::new(WRITER_IP, WRITER_PORT)
);

pub const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
