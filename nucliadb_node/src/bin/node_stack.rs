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
#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
use std::env;
#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
use std::process;

#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
use rstack;

#[cfg(not(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl"))))]
fn main() {
    println!("error: rstack is only supported on Linux");
}

#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
fn main() {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("Usage: {} <pid>", args[0]);
        process::exit(1);
    }

    let pid = match args[1].parse() {
        Ok(pid) => pid,
        Err(e) => {
            eprintln!("error parsing PID: {}", e);
            process::exit(1);
        }
    };

    let process = match rstack::trace(pid) {
        Ok(threads) => threads,
        Err(e) => {
            eprintln!("error tracing threads: {}", e);
            process::exit(1);
        }
    };

    println!("{{");
    println!("  \"threads\":");
    println!("[");
    for thread in process.threads() {
        println!("{{");
        println!("\"thread_id\": {},", thread.id());
        println!(
            "\"thread_name\": \"{}\",",
            thread.name().unwrap_or("<unknown>")
        );

        println!("\"frames\":");
        println!("[");
        for frame in thread.frames() {
            println!("{");

            match frame.symbol() {
                Some(symbol) => {
                    println!("\"ip\": \"{:#016x}\",", frame.ip());
                    println!("\"symbol_name\": \"{}\",", symbol.name());
                    println!("\"symbol_offset\": \"{:#x}\",", symbol.offset());
                }
                None => {
                    println!("\"ip\": \"{:#016x}\",", frame.ip());
                    println!("\"symbol_name\": \"???\",");
                    println!("\"symbol_offset\": \"???\",");
                }
            }
            println!("}}");
            println!(",");
        }
        println!("]");
        println!("}}");
        println!(",");
    }
    println!("]");
    println!("}}");
}
