use std::{
    collections::{HashMap, HashSet},
    env,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    sync::atomic::AtomicU64,
};

use szfs::yolo_block_recovery;

type ChecksumTableEntry = u32;

#[derive(serde::Deserialize, serde::Serialize)]
struct BlockInfo {
    block_number: u64,
    checksum: [u64; 4],
    main_offset: u64,
}

fn main() {
    let mut checksum_map_file = File::open("checksum-map.bin").unwrap();
    let checksum_map_file_size = checksum_map_file.seek(SeekFrom::End(0)).unwrap();
    let sector_size = 4096;

    let disk_size = (checksum_map_file_size / core::mem::size_of::<ChecksumTableEntry>() as u64)
        * sector_size as u64;

    println!(
        "RAIDZ total size (GB): {}",
        disk_size as f64 / 1024.0 / 1024.0 / 1024.0
    );

    let blocks_info: Vec<BlockInfo> =
        serde_json::from_reader(File::open("bad-block-info.json").unwrap()).unwrap();

    let block_checksums: Vec<(u32, [u64; 4])> = blocks_info
        .into_iter()
        .map(|block_info| (block_info.checksum[0] as u32, block_info.checksum))
        .collect();

    {
        let mut temp_check_set = HashSet::new();
        for e in &block_checksums {
            if !temp_check_set.insert(e) {
                panic!("AAAAH!");
            }
        }
    }

    let block_checksums = block_checksums.into_iter().collect();

    use rayon::prelude::*;
    let res: Vec<([u64; 4], u64)> =
        yolo_block_recovery::potential_matches_for_block_with_fletcher4_checksum_vectorized(
            4,
            1,
            sector_size,
            128 * 1024,
            block_checksums,
            || File::open("checksum-map.bin").unwrap(),
        )
        .unwrap()
        .collect();
    serde_json::to_writer(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open("bad-block-extra-info.json")
            .unwrap(),
        &res,
    )
    .unwrap();
}
