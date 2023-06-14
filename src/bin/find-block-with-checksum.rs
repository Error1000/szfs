use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::atomic::AtomicU64,
};

use szfs::yolo_block_recovery;

type ChecksumTableEntry = u32;

fn main() {
    let mut checksum_map_file = File::open("checksum-map.bin").unwrap();
    let checksum_map_file_size = checksum_map_file.seek(SeekFrom::End(0)).unwrap();
    let psize: usize = str::parse(env::args().nth(1).unwrap().trim())
        .expect("Usage: find-block-with-checksum (psize) (sector_size)");
    let sector_size: usize = str::parse(env::args().nth(2).unwrap().trim())
        .expect("Usage: find-block-with-checksum (psize) (sector_size)");

    let disk_size = (checksum_map_file_size / core::mem::size_of::<ChecksumTableEntry>() as u64)
        * sector_size as u64;

    println!(
        "RAIDZ total size (GB): {}",
        disk_size as f64 / 1024.0 / 1024.0 / 1024.0
    );

    let mut input_line = String::new();
    std::io::stdout().flush().unwrap();
    print!("Please enter checksum of block to find: ");
    std::io::stdout().flush().unwrap();
    input_line.clear();
    std::io::stdin()
        .read_line(&mut input_line)
        .expect("Reading a line should work!");
    let Ok(checksum) = parse_checksum_from_str(&input_line) else {
        panic!("Couldn't parse hash!");
    };

    let raidz_ndevices = 4;
    let raidz_nparity = 1;

    use rayon::prelude::*;
    let potential_matches: Vec<u64> =
        yolo_block_recovery::potential_matches_for_block_with_fletcher4_checksum_vectorized(
            raidz_ndevices,
            raidz_nparity,
            sector_size,
            psize,
            HashMap::from([(checksum[0] as u32, checksum)]),
            || File::open("checksum-map.bin").unwrap(),
        )
        .unwrap()
        .map(|(_, potential_match)| potential_match)
        .collect();

    println!(
        "Found {} potential matches in total!",
        potential_matches.len()
    );

    for pmatch in potential_matches {
        println!("- {}", pmatch);
    }
}

fn parse_checksum_from_str(s: &str) -> Result<[u64; 4], ()> {
    let mut res = [0u64; 4];
    for (index, part) in s
        .trim()
        .split(',')
        .map(|s| s.trim())
        .enumerate()
        .map(|(index, s)| {
            match index {
                0 => &s[1..],           // remove the beginning [
                3 => &s[..s.len() - 1], // remove the ending ],
                _ => s,
            }
        })
        .enumerate()
    {
        res[index] = part.parse::<u64>().map_err(|_| ())?;
    }
    Ok(res)
}
