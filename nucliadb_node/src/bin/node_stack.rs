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

    struct DwarfInfo {
        dwarf: Dwarf<gimli::EndianSlice<gimli::LittleEndian>>,
    }

    impl DwarfInfo {
        pub fn new(elf_binary_path: &str) -> Result<Self, gimli::Error> {
            let file = std::fs::File::open(elf_binary_path)?;
            let file = std::io::BufReader::new(file);
            let debug_info = gimli::read::DebugInfo::new(file);
            let dwarf = Dwarf::new(debug_info);
            Ok(Self { dwarf })
        }

        pub fn get_line_info(
            &self,
            symbol_offset: u64,
        ) -> Result<Option<(String, u64, u64)>, gimli::Error> {
            let mut unwind_context = UninitializedUnwindContext::new();
            let register_number = gimli::Register(0);

            let result =
                self.dwarf
                    .find_fde_from_pc(&unwind_context, symbol_offset, register_number)?;

            if let Ok(fde) = result {
                let cfi_program = fde.cie().cie_def().initial_instructions();

                let mut rows = cfi_program.rows(&self.dwarf);
                while let Some(row) = rows.next_row()? {
                    if let Some(pc) = row.address() {
                        if pc == symbol_offset {
                            if let Some(file) = row.file_index() {
                                let file_entry = self.dwarf.line_program(file)?.header().file(file);
                                let line = row.line().unwrap_or(0);
                                let column = row.column().unwrap_or(0);
                                return Ok(Some((file_entry.directory.to_string(), line, column)));
                            }
                        }
                    }
                }
            }

            Ok(None)
        }
    }

    fn get_binary_path(pid: u32) -> io::Result<String> {
        let exe_path = format!("/proc/{}/exe", pid);
        let path = fs::read_link(exe_path)?;
        Ok(path.to_string_lossy().to_string())
    }

    fn print_stack() {
        let dwarf_info = DwarfInfo::new(elf_binary_path).expect("Failed to initialize DwarfInfo");

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
