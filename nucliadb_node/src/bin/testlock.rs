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

use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
};

fn main() {
    let mut lock = OpenOptions::new().create_new(true).write(true).open("/home/javier/patata.tmp").unwrap();

    println!("File is locked {}", File::open("/home/javier/patata.tmp").unwrap().try_lock_exclusive().is_err());

    lock.lock_exclusive().unwrap();

    println!("File is locked {}", File::open("/home/javier/patata.tmp").unwrap().try_lock_exclusive().is_err());

    fs::rename("/home/javier/patata.tmp", "/home/javier/patata.final").unwrap();

    println!("File is locked {}", File::open("/home/javier/patata.final").unwrap().try_lock_exclusive().is_err());

    println!("Try write");
    lock.write("juanito".as_bytes()).unwrap();

    println!("File is locked {}", File::open("/home/javier/patata.final").unwrap().try_lock_exclusive().is_err());
}
