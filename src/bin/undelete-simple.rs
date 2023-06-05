#![feature(map_many_mut)]

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Write,
};
use szfs::{
    byte_iter::FromBytesLE,
    dmu::{DNode, DNodeDirectoryContents, DNodePlainFileContents, ObjSet},
    zio::{CompressionMethod, Vdevs},
    *,
};

// NOTE: This code assumes the hash function is perfect
const hash_function: fn(data: &[u8]) -> [u64; 4] = fletcher::do_fletcher4;

#[derive(Debug, Serialize, Deserialize)]
struct IndirectBlock {
    pub bps: Vec<Option<zio::BlockPointer>>,
}

#[derive(Serialize, Deserialize)]
enum FragmentData {
    FileDNode(DNodePlainFileContents),
    DirectoryDNode(DNodeDirectoryContents, Vec<String>),
    ObjSetDNode(ObjSet),
    IndirectBlock(IndirectBlock),
}

impl Debug for FragmentData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FragmentData::FileDNode(_) => write!(f, "File"),
            FragmentData::DirectoryDNode(_, _) => write!(f, "Dir"),
            FragmentData::ObjSetDNode(_) => write!(f, "ObjSet"),
            FragmentData::IndirectBlock(_) => write!(f, "Indirect"),
        }?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Fragment {
    data: FragmentData,
    children: HashSet<[u64; 4]>,
}

impl Debug for Fragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.data)?;
        write!(f, "(")?;
        for child in self.children.iter() {
            write!(f, "{:?}, ", child[0])?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl From<FragmentData> for Fragment {
    fn from(frag: FragmentData) -> Self {
        Self {
            data: frag,
            children: HashSet::new(),
        }
    }
}

// Note: 'data' must be from a 512-byte aligned offset of the original device
//       This is because of an optimization taking advantage of the fact that dva offsets are always multiples of 512 and a dnode "slot" is 512 bytes in size in the Objset
// Source: https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L407 which uses SPA_MINBLOCKSHIFT and DVA_GET_OFFSET
// SPA_MINBLOCKSHIFT and DVA_GET_OFFSET can be found at: https://github.com/openzfs/zfs/blob/master/include/sys/fs/zfs.h#L1783 and https://github.com/openzfs/zfs/blob/master/include/sys/bitops.h#L66
// As you can see SPA_MINBLOCKSHIFT is 9 and the macro shifts by 9
// Thus proving that the current code is shifting the offset read from disk by 9
// thus meaning that all DVA offsets are multiples of 512
fn search_le_bytes_for_dnodes(data: &[u8], vdevs: &mut Vdevs) -> HashMap<[u64; 4], Fragment> {
    let mut res = HashMap::<[u64; 4], Fragment>::new();
    if data.len() % 512 != 0 {
        if cfg!(feature = "debug") {
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Can't search data that is not a multiple of 512 bytes in size, ignoring extra bytes!");
        }
    }

    let mut data = data.chunks_exact(512);
    while let Some(sector) = data.next() {
        // Try to parse file or directory dnode
        let nsectors = dmu::DNode::get_n_slots_from_bytes_le(sector.iter().copied()).unwrap(); // NOTE: Unwrap should always succeed here, because we always have enough data
        let nextra_sectors_to_read = nsectors - 1;

        let mut dnode_data = Vec::<u8>::new();
        dnode_data.extend(sector);
        // We use a clone so as not to advance the actual iterator
        // so we don't accidentally ignore some sectors
        // because we read an invalid nsectors from one sector
        let mut data_iterator_clone = data.clone();
        for _ in 0..nextra_sectors_to_read {
            if let Some(extra_sector) = data_iterator_clone.next() {
                dnode_data.extend(extra_sector);
            } else {
                // If a Chunks Iterator returns None once, it will never return Some again, so no point in continuing
                break;
            }
        }

        let dnode_data_hash = hash_function(&dnode_data);
        // Note: This tries to parse it even if we don't have enough data, for a data recovery tool this seems like the better option
        let dnode = dmu::DNode::from_bytes_le(&mut dnode_data.into_iter());
        match dnode {
            Some(DNode::PlainFileContents(mut dnode)) => {
                if dnode
                    .0
                    .get_block_pointers()
                    .iter_mut()
                    .any(|bp| bp.dereference(vdevs).is_ok())
                {
                    res.insert(dnode_data_hash, FragmentData::FileDNode(dnode).into());
                }
            }
            Some(DNode::DirectoryContents(mut dnode)) => {
                if dnode
                    .0
                    .get_block_pointers()
                    .iter_mut()
                    .any(|bp| bp.dereference(vdevs).is_ok())
                {
                    let Some(contents) = dnode.dump_zap_contents(vdevs) else { continue; };
                    let contents = contents
                        .iter()
                        .map(|(name, _)| name)
                        .cloned()
                        .collect::<Vec<String>>();

                    res.insert(
                        dnode_data_hash,
                        FragmentData::DirectoryDNode(dnode, contents).into(),
                    );
                }
            }
            _ => (),
        }
    }

    res
}

fn main() {
    use szfs::ansi_color::*;

    let Ok(vdev0) = File::open(env::args().nth(1).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev0!");
        return;
    };
    let mut vdev0: VdevFile = vdev0.into();

    let Ok(vdev1) = File::open(env::args().nth(2).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev1!");
        return;
    };
    let mut vdev1: VdevFile = vdev1.into();

    let Ok(vdev2) = File::open(env::args().nth(3).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev2!");
        return;
    };
    let mut vdev2: VdevFile = vdev2.into();

    let Ok(vdev3) = File::open(env::args().nth(4).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev3!");
        return;
    };
    let mut vdev3: VdevFile = vdev3.into();

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

    let disk_size = vdev_raidz.get_size();
    let mut vdevs = HashMap::<usize, &mut dyn Vdev>::new();
    vdevs.insert(0usize, &mut vdev_raidz);

    // The sizes are just the most common sizes i have seen while looking at the sizes of compressed indirect blocks
    let compression_methods_and_sizes_to_try = [(
        CompressionMethod::Lz4,
        [512 * 2, 512 * 3, 512 * 21, 512 * 256],
        [0], /* irrelevant for lz4 */
    )];

    // This is the main graph
    let mut recovered_fragments = HashMap::<[u64; 4], Fragment>::new();

    println!("RAIDZ total size (GB): {}", disk_size / 1024 / 1024 / 1024);
    println!("Step 1. Gathering basic fragments");

    let mut checkpoint_number = 0;
    for off in (0..disk_size).step_by(512) {
        if off % (128 * 1024 * 1024) == 0 && off != 0 {
            println!(
                "{}% done gathering basic fragments ...",
                ((off as f32) / (disk_size as f32)) * 100.0
            );
        }

        if off % (100 * 1024 * 1024 * 1024) == 0 && off != 0 {
            // Every ~100 GB
            println!("Saving checkpoint...");
            write!(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(format!("undelete-step1-checkpoint{checkpoint_number}.json"))
                    .unwrap(),
                "{}",
                &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>())
                    .unwrap()
            )
            .unwrap();
            checkpoint_number += 1;
            println!("Done!");
        }

        // NOTE: Currently asize is just not used even though it's part of the data structure, because we read it form disk
        let dva = szfs::zio::DataVirtualAddress::from(0, off, false);

        // Since we don't know what the size of the block(if there is any) at this offset might be
        // we just try all possible options
        for compression_method_and_sizes in compression_methods_and_sizes_to_try {
            for possible_comp_size in compression_method_and_sizes.1 {
                let Ok(data) = dva.dereference(&mut vdevs, possible_comp_size) else {
                    continue;
                };

                for possible_decomp_size in compression_method_and_sizes.2 {
                    let decomp_data = zio::try_decompress_block(
                        &data,
                        compression_method_and_sizes.0,
                        possible_decomp_size,
                    )
                    .unwrap_or_else(|partial_data| partial_data);
                    let res = search_le_bytes_for_dnodes(&decomp_data, &mut vdevs);
                    recovered_fragments.extend(res);
                }
            }
        }
    }

    println!("Found {} basic fragments", recovered_fragments.len());
    println!("Saving checkpoint...");
    write!(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(format!("undelete-step1-checkpoint{checkpoint_number}.json"))
            .unwrap(),
        "{}",
        &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>()).unwrap()
    )
    .unwrap();
}
