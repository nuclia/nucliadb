// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use once_cell::sync::Lazy;

const READER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const READER_PORT: u16 = 18031;
pub static READER_ADDR: Lazy<SocketAddr> = Lazy::new(|| SocketAddr::new(READER_IP, READER_PORT));

const WRITER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const WRITER_PORT: u16 = 18030;
pub static WRITER_ADDR: Lazy<SocketAddr> = Lazy::new(|| SocketAddr::new(WRITER_IP, WRITER_PORT));

pub const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
