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
mod node_stack {
    extern crate gimli;
    extern crate rustc_demangle;
    extern crate serde;
    extern crate serde_json;

    use std::{env, process};

    use gimli::{
        DebugInfo, Dwarf, LineProgramRow, LittleEndian, Reader, RegisterRule,
        UninitializedUnwindContext,
    };
    use rstack;
    use rustc_demangle::demangle;
    use serde::{Deserialize, Serialize}; // Add Serde traits

    // Define Rust structs to represent your JSON data
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
        line_info: Option<LineInfo>, // Add line_info field
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

    fn print_stack() {
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
                    line_info: None, // Initialize line_info as None
                };

                match frame.symbol() {
                    Some(symbol) => {
                        frame_info.symbol_name = demangle(symbol.name()).to_string();
                        frame_info.symbol_offset = format!("{:#x}", symbol.offset());

                        // Use dwarf_info.get_line_info and store it in frame_info.line_info
                        match dwarf_info.get_line_info(symbol.offset()) {
                            Ok(Some((file, line, column))) => {
                                frame_info.line_info = Some(LineInfo { file, line, column });
                            }
                            _ => {}
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
