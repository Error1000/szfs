use std::{
    env,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

use szfs::yolo_block_recovery;

type ChecksumTableEntry = u32;

fn main() {
    let mut checksum_map_file = File::open("checksum-map.bin").unwrap();
    let checksum_map_file_size = checksum_map_file.seek(SeekFrom::End(0)).unwrap();
    let psize: usize = str::parse(env::args().nth(1).unwrap().trim()).unwrap();
    let sector_size: u64 = str::parse(env::args().nth(2).unwrap().trim()).unwrap();

    let disk_size =
        (checksum_map_file_size / core::mem::size_of::<ChecksumTableEntry>() as u64) * sector_size;

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
    let Ok(hsh) = parse_checksum_from_str(&input_line) else {
        panic!("Couldn't parse hash!");
    };

    let partial_checksum_to_look_for = hsh[0] as ChecksumTableEntry;

    let mut npotential_matches = 0;
    let raidz_ndevices = 4;
    let is_raidz1 = true;

    let block_size_upper_bound =
        psize / sector_size as usize + psize / sector_size as usize / (raidz_ndevices - 1) + 1;

    for off in (0..disk_size).step_by(1024 * 1024) {
        if off % (4 * 1024 * 1024 * 1024) == 0 && off != 0 {
            // Every ~4 gb
            println!(
                "{}% done looking for checksum ...",
                ((off as f32) / (disk_size as f32)) * 100.0
            );
        }

        // We over-read because the convolution needs more than
        // 1 mb of sectors to calculate the partial checksum
        // of the block starting at each one of the sectors
        let mut hunk = vec![
            0u8;
            (1024 * 1024 / sector_size as usize + block_size_upper_bound)
                * core::mem::size_of::<ChecksumTableEntry>()
        ];

        let checksum_file_offset =
            (off / sector_size) * core::mem::size_of::<ChecksumTableEntry>() as u64;
        checksum_map_file
            .seek(SeekFrom::Start(checksum_file_offset))
            .unwrap();
        let _ = checksum_map_file.read(&mut hunk).unwrap();
        let mut checksums = Vec::<ChecksumTableEntry>::new();
        for index in (0..hunk.len()).step_by(core::mem::size_of::<ChecksumTableEntry>()) {
            checksums.push(ChecksumTableEntry::from_le_bytes(
                hunk[index..index + core::mem::size_of::<ChecksumTableEntry>()]
                    .try_into()
                    .unwrap(),
            ));
        }

        let res = yolo_block_recovery::calculate_fletcher4_partial_block_checksums(
            off,
            psize,
            is_raidz1,
            sector_size as usize,
            raidz_ndevices,
            &checksums,
        );

        for ind in 0..res.len() {
            if res[ind] as u32 == partial_checksum_to_look_for {
                println!(
                    "Found potential match at {}!",
                    off + (ind as u64) * sector_size
                );
                npotential_matches += 1;
            }
        }
    }

    println!("Found {} potential matches in total!", npotential_matches);
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
