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

#[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
mod node_stack {
    use addr2line;
    use async_std::io;
    use memmap2;
    #[allow(clippy::all)]
    use rstack;
    use rustc_demangle::demangle;
    use serde::{Deserialize, Serialize};
    use std::fs::File;
    use std::{env, fs, process};

    #[derive(Debug, Serialize, Deserialize)]
    struct LineInfo {
        file: String,
        line: u64,
        column: u64,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct FrameInfo {
        ip: String,
        symbol_name: String,
        symbol_offset: String,
        line_info: Option<LineInfo>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct ThreadInfo {
        thread_id: u32,
        thread_name: String,
        frames: Vec<FrameInfo>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct ProcessInfo {
        threads: Vec<ThreadInfo>,
    }

    fn get_binary_path(pid: u32) -> io::Result<String> {
        let exe_path = format!("/proc/{}/exe", pid);
        let path = fs::read_link(exe_path)?;
        Ok(path.to_string_lossy().to_string())
    }

    pub fn print_stack() {
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

        let binary_path = get_binary_path(pid).unwrap();
        let binary = File::open(binary_path).unwrap();
        let map = unsafe { memmap2::Mmap::map(&binary).unwrap() };
        let file = addr2line::object::File::parse(&*map).unwrap();
        let map = addr2line::ObjectContext::new(&file).expect("debug symbols not found");

        // Get trace
        let process = match rstack::trace(pid) {
            Ok(threads) => threads,
            Err(e) => {
                eprintln!("error tracing threads: {}", e);
                process::exit(1);
            }
        };

        // Create ProcessInfo struct to store the JSON data
        let mut process_info = ProcessInfo { threads: vec![] };

        for thread in process.threads() {
            let mut thread_info = ThreadInfo {
                thread_id: thread.id(),
                thread_name: thread.name().unwrap_or("<unknown>").to_string(),
                frames: vec![],
            };

            for frame in thread.frames() {
                let mut frame_info = FrameInfo {
                    ip: format!("{:#016x}", frame.ip()),
                    symbol_name: String::new(),
                    symbol_offset: String::new(),
                    line_info: None,
                };

                match frame.symbol() {
                    Some(symbol) => {
                        frame_info.symbol_name = demangle(symbol.name()).to_string();
                        frame_info.symbol_offset = format!("{:#x}", symbol.offset());

                        // Convert the offset to file, line, col
                        let loc = map.find_location(symbol.offset()).unwrap();

                        if let Some(loc) = loc {
                            let line_info = LineInfo {
                                file: loc.file.unwrap_or("???").to_string(),
                                line: loc.line.unwrap_or(0) as u64,
                                column: loc.column.unwrap_or(0) as u64,
                            };
                            frame_info.line_info = Some(line_info);
                        }
                    }
                    None => {
                        frame_info.symbol_name = "???".to_string();
                        frame_info.symbol_offset = "???".to_string();
                    }
                }

                thread_info.frames.push(frame_info);
            }

            process_info.threads.push(thread_info);
        }

        // Serialize process_info to JSON using serde_json
        let json_output = serde_json::to_string_pretty(&process_info)
            .expect("Failed to serialize process_info to JSON");

        // Print the JSON output
        println!("{}", json_output);
    }
}

fn main() {
    #[cfg(not(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl"))))]
    println!("error: rstack is only supported on Linux");

    #[cfg(all(target_arch = "x86_64", target_os = "linux", not(target_env = "musl")))]
    node_stack::print_stack();
}
