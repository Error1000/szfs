use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    io::Write,
    iter,
    ops::Range,
    os::unix::prelude::FileExt,
};

use itertools::Itertools;
use szfs::{nvlist, zio::Vdevs, Vdev, VdevFile, VdevLabel, VdevRaidz};

#[derive(serde::Serialize, serde::Deserialize)]
struct BlockInfo {
    block_number: u64,
    checksum: [u64; 4],
    main_offset: u64,
    #[serde(default)]
    extra_offsets: Vec<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SquashfsBlockInfo {
    block_number: u64,
    ondisk_size: u32,
    is_compressed: bool,
}

fn overlaps<T>(range1: Range<T>, range2: Range<T>) -> bool
where
    T: std::cmp::PartialOrd,
{
    range1.start < range2.end && range2.start < range1.end
}

fn main() {
    let usage = format!("Usage: {} (vdevs...)", env::args().next().unwrap());
    let mut vdev0: VdevFile = File::open(env::args().nth(1).expect(&usage))
        .expect("Vdev 0 should be able to be opened!")
        .into();
    let mut vdev1: VdevFile = File::open(env::args().nth(2).expect(&usage))
        .expect("Vdev 1 should be able to be opened!")
        .into();
    let mut vdev2: VdevFile = File::open(env::args().nth(3).expect(&usage))
        .expect("Vdev 2 should be able to be opened!")
        .into();
    let mut vdev3: VdevFile = File::open(env::args().nth(4).expect(&usage))
        .expect("Vdev 3 should be able to be opened!")
        .into();

    // For now just use the first label
    let mut label0 = VdevLabel::from_bytes(
        &vdev0
            .read_raw_label(0)
            .expect("Vdev label 0 must be parsable!"),
    );

    let name_value_pairs =
        nvlist::from_bytes_xdr(&mut label0.get_name_value_pairs_raw().iter().copied())
            .expect("Name value pairs in the vdev label must be valid!");
    let nvlist::Value::NVList(vdev_tree) = &name_value_pairs["vdev_tree"] else {
        panic!("vdev_tree is not an nvlist!");
    };

    let nvlist::Value::U64(top_level_ashift) = vdev_tree["ashift"] else {
        panic!("no ashift found for top level vdev!");
    };

    use szfs::ansi_color::*;
    println!("{CYAN}Info{WHITE}: Parsed nv_list, {name_value_pairs:?}!");
    println!("{RED}Important{WHITE}: Please make sure the disks are actually in the right order by using the nv_list, i can't actually check that in a reliable way!!!");

    let mut devices = Vdevs::new();
    devices.insert(0, &mut vdev0);
    devices.insert(1, &mut vdev1);
    devices.insert(2, &mut vdev2);
    devices.insert(3, &mut vdev3);

    let mut vdev_raidz: VdevRaidz =
        VdevRaidz::from_vdevs(devices, 4, 1, 2_usize.pow(top_level_ashift as u32));

    label0.set_raw_uberblock_size(2_usize.pow(top_level_ashift as u32));

    let mut vdevs = HashMap::<usize, &mut dyn Vdev>::new();
    vdevs.insert(0usize, &mut vdev_raidz);

    let bad_blocks_info: Vec<BlockInfo> =
        serde_json::from_reader(File::open("bad-block-info.json").unwrap()).unwrap();
    let bad_blocks_info: HashMap<u64, BlockInfo> = bad_blocks_info
        .into_iter()
        .map(|bad_block_info| (bad_block_info.block_number, bad_block_info))
        .collect();

    let squashfs_info: Vec<SquashfsBlockInfo> =
        serde_json::from_reader(File::open("squashfs-info.json").unwrap()).unwrap();

    /*
        // This part merges the two outputs of undelete-postrecover and find-block-with-checksum-postrecover
        {
            let bad_block_extra: Vec<([u64; 4], u64)> =
                serde_json::from_reader(File::open("bad-block-extra-info.json").unwrap()).unwrap();

            'outer: for extra_info in bad_block_extra {
                for block_info in bad_blocks_info.iter_mut() {
                    if block_info.checksum == extra_info.0 {
                        block_info.extra_offsets.push(extra_info.1);
                        continue 'outer;
                    }
                }
            }
        }

        serde_json::to_writer(
            File::create("bad-block-info-merged.json").unwrap(),
            &bad_blocks_info,
        )
        .unwrap();
    */

    let recovered_file = File::open("recovered-file.bin").unwrap();

    /*
    // This part recovers the bad blocks using the main offset that the file metadata says it should be in a "binary patch" file
    let mut binary_patch_file = File::create("bad-blocks.binpatch").unwrap();

    for bad_block_info in bad_blocks_info.values() {
        let file_offset = bad_block_info.block_number * 128 * 1024;
        let dva = szfs::zio::DataVirtualAddress::from(0, bad_block_info.main_offset, false);
        let block_data = dva.dereference(&mut vdevs, 128 * 1024).unwrap();
        binary_patch_file
            .write_all(&u64::to_le_bytes(file_offset))
            .unwrap();
        binary_patch_file
            .write_all(&u64::to_le_bytes(128 * 1024))
            .unwrap();
        binary_patch_file.write_all(&block_data).unwrap();
    }
    */

    let mut binary_patch_file =
        File::create("squashfs-surgically-recovered-blocks.binpatch").unwrap();

    let mut current_squashfs_block_file_offset: u64 = 96;
    let mut last_log_offset = 0;
    for squashfs_block_info in squashfs_info {
        if squashfs_block_info.ondisk_size == 0 {
            continue;
        }

        if (current_squashfs_block_file_offset - last_log_offset) > (512 * 1024 * 1024) {
            // Every ~512 mb
            println!(
                "{}% done ...",
                (current_squashfs_block_file_offset as f32
                    / recovered_file.metadata().unwrap().len() as f32)
                    * 100.0
            );
            last_log_offset = current_squashfs_block_file_offset;
        }
        // first_file_block_number = the file block number of the block containing the first byte of the squashfs block
        // first_file_block_offset = the offset in the file block containing the first byte of the squashfs block
        // last_file_block_number = the file block number of the block containing the last byte of the squashfs block

        let first_file_block_number = current_squashfs_block_file_offset / (128 * 1024);
        let first_file_block_offset = current_squashfs_block_file_offset % (128 * 1024);
        let last_file_block_number = (current_squashfs_block_file_offset
            + squashfs_block_info.ondisk_size as u64)
            / (128 * 1024);

        let mut should_attempt_recovery = false;
        if squashfs_block_info.is_compressed {
            for file_block_number in first_file_block_number..=last_file_block_number {
                if bad_blocks_info.get(&file_block_number).is_some() {
                    assert!(overlaps(
                        current_squashfs_block_file_offset
                            ..current_squashfs_block_file_offset
                                + squashfs_block_info.ondisk_size as u64,
                        file_block_number * 128 * 1024..file_block_number * 128 * 1024 + 128 * 1024,
                    ));
                    should_attempt_recovery = true;
                    break;
                }
            }
        }

        if should_attempt_recovery {
            #[derive(Clone, Copy, Debug)]
            enum TypedOffset {
                File(u64),
                Raidz(u64),
            }

            let mut res = Vec::<Vec<TypedOffset>>::new();

            for file_block_number in first_file_block_number..=last_file_block_number {
                if let Some(bad_block_info) = bad_blocks_info.get(&file_block_number) {
                    res.push(
                        bad_block_info
                            .extra_offsets
                            .iter()
                            .copied()
                            .chain(iter::once(bad_block_info.main_offset))
                            .map(TypedOffset::Raidz)
                            .collect(),
                    );
                } else {
                    res.push(iter::once(TypedOffset::File(file_block_number)).collect());
                }
            }

            let mut res_data: HashSet<Vec<u8>> = HashSet::new();

            for combination in res
                .into_iter()
                .map(|offsets| offsets.into_iter())
                .multi_cartesian_product()
            {
                let mut combination_data = Vec::<u8>::new();
                for off in &combination {
                    match off {
                        TypedOffset::File(off) => {
                            let mut block_data = Vec::<u8>::with_capacity(128 * 1024);
                            recovered_file.read_exact_at(&mut block_data, *off).unwrap();
                            combination_data.extend(block_data);
                        }

                        TypedOffset::Raidz(off) => {
                            let dva = szfs::zio::DataVirtualAddress::from(0, *off, false);
                            let block_data = dva.dereference(&mut vdevs, 128 * 1024).unwrap();
                            combination_data.extend(block_data);
                        }
                    }
                }

                combination_data.drain(0..first_file_block_offset as usize);
                combination_data.resize(squashfs_block_info.ondisk_size as usize, 0);
                assert!(
                    combination_data.len()
                        == usize::try_from(squashfs_block_info.ondisk_size).unwrap()
                );

                if combination_data[0..6] == [0xFD, b'7', b'z', b'X', b'Z', 0x00]
                    && combination_data[combination_data.len() - 2..combination_data.len()]
                        != [b'Y', b'Z']
                {
                    println!("Squashfs block at file offset {} extracted using combination {:?}, has a correct beginning magic number but no ending magic number!",
                current_squashfs_block_file_offset, combination);
                }

                if combination_data[0..6] == [0xFD, b'7', b'z', b'X', b'Z', 0x00]
                    && combination_data[combination_data.len() - 2..combination_data.len()]
                        == [b'Y', b'Z']
                {
                    res_data.insert(combination_data);
                }
            }

            if res_data.len() > 1 {
                unimplemented!("I didn't expect there to be two valid and different versions of a compressed block, despite using multiple possible bad blocks, i just assumed this won't happen!");
            }

            if res_data.len() == 1 {
                let compressed_squashfs_block_data = res_data.iter().next().unwrap();
                binary_patch_file
                    .write_all(&u64::to_le_bytes(current_squashfs_block_file_offset))
                    .unwrap();
                binary_patch_file
                    .write_all(&u64::to_be_bytes(
                        compressed_squashfs_block_data.len() as u64
                    ))
                    .unwrap();
                binary_patch_file
                    .write_all(compressed_squashfs_block_data)
                    .unwrap();
            }
        }
        current_squashfs_block_file_offset += squashfs_block_info.ondisk_size as u64;
    }
}
