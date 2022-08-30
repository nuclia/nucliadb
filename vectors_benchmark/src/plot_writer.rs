use std::fs::File;
use std::io;
use std::io::Write;

pub struct PlotWriter {
    file: File,
}

impl PlotWriter {
    pub fn new(file: File) -> PlotWriter {
        PlotWriter { file }
    }
    pub fn add(&mut self, x: usize, y: u128) -> io::Result<()> {
        writeln!(self.file, "{x} {y}")
    }
}
