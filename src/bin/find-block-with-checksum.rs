use std::{
    env,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

type ChecksumTableEntry = u32;

fn calculate_fletcher4_block_partial_checksum(
    mut off: u64,
    mut psize: usize,
    checksum_map_file: &mut File,
    file_cache: &mut lru::LruCache<u64, ChecksumTableEntry>,
    is_raidz1: bool,
    raidz_ndevices: usize,
) -> Option<ChecksumTableEntry> {
    let mut column_mapping = (0..raidz_ndevices).collect::<Vec<usize>>();

    // Source: https://github.com/openzfs/zfs/blob/master/module/zfs/vdev_raidz.c#L398
    // Second source: https://github.com/openzfs/zfs/issues/12538#issuecomment-1251651412
    if is_raidz1 && (off / (1 * 1024 * 1024)) % 2 != 0 {
        column_mapping.swap(0, 1);
    }

    off /= 512;
    off *= core::mem::size_of::<ChecksumTableEntry>() as u64;
    psize /= 512;
    // Note: Even though the checksums are added in the wrong
    // order ( row major instead of column major )
    // it doesn't matter because it's addition
    let mut final_checksum = 0u64;
    for index in 0.. {
        let column = index % raidz_ndevices;
        let mapped_column = column_mapping[column];

        if mapped_column == 0 {
            off += core::mem::size_of::<ChecksumTableEntry>() as u64;
            // parity blocks are not included
            continue;
        }

        let sector_checksum = if let Some(val) = file_cache.get(&off) {
            *val
        } else {
            if checksum_map_file.seek(SeekFrom::Start(off)).is_err() {
                return None;
            }
            let mut raw_checksum = [0u8; core::mem::size_of::<ChecksumTableEntry>()];
            if checksum_map_file.read(&mut raw_checksum).is_err() {
                return None;
            }

            let val = ChecksumTableEntry::from_le_bytes(raw_checksum);
            file_cache.push(off, val);
            val
        };

        final_checksum = final_checksum.wrapping_add(sector_checksum as u64);
        psize -= 1;
        if psize == 0 {
            break;
        }

        off += core::mem::size_of::<ChecksumTableEntry>() as u64;
    }

    Some(final_checksum as ChecksumTableEntry)
}

fn main() {
    let mut checksum_map_file = File::open("checksum-map.bin").unwrap();
    let checksum_map_file_size = checksum_map_file.seek(SeekFrom::End(0)).unwrap();
    let mut file_cache: lru::LruCache<u64, ChecksumTableEntry> =
        lru::LruCache::new(1000.try_into().unwrap());

    let disk_size =
        (checksum_map_file_size / core::mem::size_of::<ChecksumTableEntry>() as u64) * 512;

    println!("RAIDZ total size (GB): {}", disk_size / 1024 / 1024 / 1024);

    let psize: usize = str::parse(env::args().nth(1).unwrap().trim()).unwrap();
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

    let mut npotential_matches = 0;
    for off in (0..disk_size).step_by(512) {
        if off % (512 * 1024 * 1024) == 0 && off != 0 {
            // Every ~512 mb
            println!(
                "{}% done looking for checksum ...",
                ((off as f32) / (disk_size as f32)) * 100.0
            );
        }

        if let Some(block_checksum) = calculate_fletcher4_block_partial_checksum(
            off,
            psize,
            &mut checksum_map_file,
            &mut file_cache,
            true,
            4,
        ) {
            if block_checksum == hsh[0] as ChecksumTableEntry {
                println!("Found possible match at offset {}!", off);
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
