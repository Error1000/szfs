use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    sync::Mutex,
};

use fftconvolve::fftconvolve;
use lazy_static::lazy_static;
use ndarray::arr1;

use crate::{
    fletcher::do_fletcher4,
    zio::{DataVirtualAddress, Vdevs},
};

type ChecksumTableEntry = u32;

pub fn calculate_convolution_vector_for_block(
    off: u64,
    mut psize: usize,
    is_raidz1: bool,
    sector_size: usize,
    raidz_ndevices: usize,
    raidz_nparity: usize,
) -> Vec<bool> {
    assert!(raidz_nparity < raidz_ndevices);
    let mut column_mapping = (0..raidz_ndevices).collect::<Vec<usize>>();

    // Source: https://github.com/openzfs/zfs/blob/master/module/zfs/vdev_raidz.c#L398
    // Second source: https://github.com/openzfs/zfs/issues/12538#issuecomment-1251651412
    if is_raidz1 && (off / (1 * 1024 * 1024)) % 2 != 0 {
        column_mapping.swap(0, 1);
    }

    psize /= sector_size;
    let mut res = Vec::new();
    for index in 0.. {
        let column = index % raidz_ndevices;
        let mapped_column = column_mapping[column];

        if mapped_column < raidz_nparity {
            // parity blocks are not included
            res.push(false);
            continue;
        }

        res.push(true);

        psize -= 1;
        if psize == 0 {
            break;
        }
    }

    res
}

pub fn calculate_fletcher4_partial_block_checksums(
    off: u64,
    psize: usize,
    is_raidz1: bool,
    sector_size: usize,
    raidz_ndevices: usize,
    raidz_nparity: usize,
    sector_checksums: &[ChecksumTableEntry],
) -> Vec<u64> {
    let cv: Vec<f64> = calculate_convolution_vector_for_block(
        off,
        psize,
        is_raidz1,
        sector_size,
        raidz_ndevices,
        raidz_nparity,
    )
    .into_iter()
    .map(|val| val as u8 as f64)
    .rev()
    .collect();
    let sv: Vec<f64> = sector_checksums.iter().map(|val| *val as f64).collect();
    let res = fftconvolve(&arr1(&sv), &arr1(&cv), fftconvolve::Mode::Full).unwrap();
    let mut res: Vec<u64> = res
        .into_iter()
        .skip(cv.len() - 1)
        .map(|val| val.round() as u64)
        .collect();

    res.resize(sector_checksums.len() - (cv.len() - 1), 0);
    res
}

lazy_static! {
    static ref YOLO_CACHE: Mutex<HashMap<([u64; 4], usize), Option<u64>>> = Mutex::new(
        serde_json::from_reader::<_, Vec<(_, _)>>(File::open("yolo-cache.json").unwrap(),)
            .unwrap()
            .into_iter()
            .collect()
    );
}

// Returns: Location of a block with the specifed checksum
// NOTE: Will *not* work for finding the contents of gang blocks
// but will work for finding the gang block itself

pub fn find_block_with_fletcher4_checksum(
    vdevs: &mut Vdevs,
    checksum: &[u64; 4],
    psize: usize,
) -> Option<u64> {
    if let Ok(Some(res_off)) = YOLO_CACHE
        .lock()
        .map(|m| m.get(&(*checksum, psize)).copied())
    {
        return res_off;
    }

    let raidz_vdev = vdevs.get_mut(&0)?;
    let raidz_vdev_info = raidz_vdev.get_raidz_info()?;
    let sector_size = raidz_vdev.get_asize();
    let vdevs = Mutex::from(vdevs);

    use crate::ansi_color::*;
    println!(
        "{YELLOW}Warning{WHITE}: Doing YOLO block recovery for block with checksum: {:?} of psize: {:?} using sector size: {:?}!",
        checksum,
        psize,
        sector_size
    );

    let mut checksum_map_file = File::open("checksum-map.bin").unwrap();
    let checksum_map_file_size = checksum_map_file.seek(SeekFrom::End(0)).unwrap();

    let disk_size = (checksum_map_file_size / core::mem::size_of::<ChecksumTableEntry>() as u64)
        * sector_size as u64;

    let partial_checksum_to_look_for = checksum[0] as ChecksumTableEntry;

    let raidz_ndevices = raidz_vdev_info.ndevices;
    let raidz_nparity = raidz_vdev_info.nparity;
    let is_raidz1 = raidz_vdev_info.nparity == 1;

    let block_size_upper_bound =
        psize / sector_size + psize / sector_size / (raidz_ndevices - 1) + 1;

    use rayon::prelude::*;
    let result: Option<u64> = (0..usize::try_from(disk_size).unwrap())
        .into_par_iter()
        .step_by(1024 * 1024)
        .fold(
            || (File::open("checksum-map.bin").unwrap(), Vec::new()),
            |(mut checksum_map_file, mut partial_matches), off| {
                let off = off as u64;

                // We over-read because the convolution needs more than
                // 1 mb of sectors to calculate the partial checksum
                // of the block starting at each one of the sectors

                let mut hunk = vec![
                    0u8;
                    (1024 * 1024 / sector_size + block_size_upper_bound)
                        * core::mem::size_of::<ChecksumTableEntry>()
                ];

                let checksum_file_offset =
                    (off / sector_size as u64) * core::mem::size_of::<ChecksumTableEntry>() as u64;
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

                let res = calculate_fletcher4_partial_block_checksums(
                    off,
                    psize,
                    is_raidz1,
                    sector_size,
                    raidz_ndevices,
                    raidz_nparity,
                    &checksums,
                );

                for ind in 0..res.len() {
                    if res[ind] as u32 == partial_checksum_to_look_for {
                        println!(
                            "{CYAN}Info{WHITE}: Found partial match at {}!",
                            off + (ind as u64) * (sector_size as u64)
                        );
                        partial_matches.push(off + (ind as u64) * (sector_size as u64));
                    }
                }

                (checksum_map_file, partial_matches)
            },
        )
        .map(|(_, r)| r)
        .flatten()
        .find_any(move |&partial_match_off| {
            // Check to see if the match is correct
            let dva = DataVirtualAddress::from(0, partial_match_off, false);
            let Ok(data) = dva.dereference(&mut vdevs.lock().unwrap(), psize) else { return false; };
            let checksum_of_match = do_fletcher4(&data);
            return checksum_of_match == *checksum;
        });

    let save_yolo_cache = |map: &HashMap<_, _>| {
        // Save the new cache
        write!(
            OpenOptions::new()
                .truncate(true)
                .create(true)
                .write(true)
                .open("yolo-cache.json")
                .unwrap(),
            "{}",
            serde_json::to_string(&map.iter().collect::<Vec<(_, _)>>()).unwrap()
        )
        .unwrap();
    };

    if let Some(off) = result {
        if let Ok(mut lock) = YOLO_CACHE.lock() {
            lock.insert((*checksum, psize), Some(off));
            save_yolo_cache(&*lock);
        } // Eh.. it's not that big a deal if we can't lock, we just miss some optimisations, just don't crash the app that's the main priority

        println!(
            "{CYAN}Info{WHITE}: YOLO block recovery succeded for block with checksum: {:?}, the result was offset {:?}!",
            checksum,
            off
        );

        return Some(off);
    } else {
        if let Ok(mut lock) = YOLO_CACHE.lock() {
            lock.insert((*checksum, psize), None);
            save_yolo_cache(&*lock);
        } // Eh.. it's not that big a deal if we can't lock, we just miss some optimisations, just don't crash the app that's the main priority

        println!(
            "{YELLOW}Warning{WHITE}: YOLO block recovery failed for block with checksum: {:?}!",
            checksum
        );

        return None;
    }
}
