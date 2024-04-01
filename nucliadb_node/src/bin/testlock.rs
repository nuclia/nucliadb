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
