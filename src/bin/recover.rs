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
    dmu::{DNodeDirectoryContents, DNodePlainFileContents, ObjSet},
    zio::Vdevs,
    *,
};

// NOTE: This code assumes the hash function is perfect
const hash_function: fn(data: &[u8]) -> [u64; 4] = fletcher::do_fletcher4;

#[derive(Debug, Serialize, Deserialize)]
struct IndirectBlock {
    pub bps: Vec<Option<zio::BlockPointer>>,
}

impl IndirectBlock {
    pub fn from_bytes_le(data: &[u8], vdevs: &mut Vdevs) -> Option<IndirectBlock> {
        let mut res = Vec::new();
        let mut nfound = 0;
        let data = data.chunks(zio::BlockPointer::get_ondisk_size());
        for potential_bp in data {
            if let Some(mut bp) =
                zio::BlockPointer::from_bytes_le(&mut potential_bp.iter().copied())
            {
                // Verify block pointer
                // NOTE: This might not necessarily guarantee that the block pointer
                // wasn't just misinterpreted random data, especially if
                // it is an embedded block pointer
                if bp.dereference(vdevs).is_ok() {
                    res.push(Some(bp));
                    nfound += 1;
                } else {
                    res.push(None);
                }
            } else {
                res.push(None);
                continue;
            }
        }

        if nfound == 0 {
            return None;
        }

        Some(IndirectBlock { bps: res })
    }

    // Assumes that all block pointers point to blocks of the same size
    // Will replace a missing block with a chunk of zeros, of the same size as all other blocks
    pub fn get_data_with_gaps(&mut self, vdevs: &mut Vdevs) -> Option<Vec<u8>> {
        let mut res = Vec::new();
        let block_pointer_chunck_size = self
            .bps
            .iter_mut()
            .find(|bp| bp.is_some())
            .unwrap()
            .as_mut()
            .unwrap()
            .parse_logical_size();
        for bp in self.bps.iter_mut() {
            if let Some(ref mut bp) = bp {
                if block_pointer_chunck_size != bp.parse_logical_size() {
                    return None;
                }
                res.extend(bp.dereference(vdevs).unwrap());
            } else {
                for _ in 0..block_pointer_chunck_size {
                    res.push(0u8);
                }
            }
        }
        Some(res)
    }
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

// Tries other recovered files if the first one does not work
fn aggregated_read_block(
    file_list: &mut Vec<([u64; 4], Fragment)>,
    block_id: usize,
    vdevs: &mut Vdevs,
) -> Option<Vec<u8>> {
    for (_, frag) in file_list {
        if let FragmentData::FileDNode(f) = &mut frag.data {
            if let Ok(res) = f.0.read_block(block_id, vdevs) {
                return Some(res);
            }
        }
    }

    None
}

fn main() {
    use szfs::ansi_color::*;
    let Ok(vdev0) = File::open(env::args().nth(1).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev0!");
        return;
    };
    let mut vdev0: VdevDisk = vdev0.into();

    let Ok(vdev1) = File::open(env::args().nth(2).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev1!");
        return;
    };
    let mut vdev1: VdevDisk = vdev1.into();

    let Ok(vdev2) = File::open(env::args().nth(3).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev2!");
        return;
    };
    let mut vdev2: VdevDisk = vdev2.into();

    let Ok(vdev3) = File::open(env::args().nth(4).unwrap().trim())
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev3!");
        return;
    };
    let mut vdev3: VdevDisk = vdev3.into();

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

    let nvlist::Value::U64(_label_txg) = name_value_pairs["txg"] else {
        panic!("no txg found in label!");
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

    let mut recovered_fragments: Vec<([u64; 4], Fragment)> =
        serde_json::from_reader(File::open("undelete-filtered-checkpoint.json").unwrap()).unwrap();
    recovered_fragments.retain(|(_, f)| matches!(f.data, FragmentData::FileDNode(_)));
    // write!(OpenOptions::new().create(true).truncate(true).write(true).open(format!("undelete-filtered-checkpoint.json")).unwrap(), "{}", &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>()).unwrap()).unwrap();

    recovered_fragments.sort_by(|a, b| {
        let size1 = match &a.1.data {
            FragmentData::FileDNode(f) => f.0.get_data_size(),
            _ => panic!(""),
        };

        let size2 = match &b.1.data {
            FragmentData::FileDNode(f) => f.0.get_data_size(),
            _ => panic!(""),
        };
        size2.cmp(&size1)
    });

    for res in recovered_fragments.iter() {
        println!("{:?}", res);
    }

    println!("N fragments: {}", recovered_fragments.len());
    println!("RAIDZ total size (GB): {}", disk_size / 1024 / 1024 / 1024);

    // NOTE: This is specifically ment for my scenario
    // where i lost a big file that i have recovered the size of
    // in a fs that only ever had 2-3 files
    let file_size: usize = 1084546955827;
    // I know the block size of the file system i'm recovering from
    let file_block_size: usize = 128 * 1024;
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open("recovered-file.bin")
        .unwrap();

    let nblocks_in_file = file_size / file_block_size;
    let mut bad_blocks: Vec<usize> = Vec::new();
    for block_id in 0..nblocks_in_file {
        if block_id % 1000 == 0 {
            // Every ~128 mb
            println!(
                "Copying data {}% done, {} bad blocks so far ...",
                (block_id as f32 / nblocks_in_file as f32) * 100.0,
                bad_blocks.len()
            );
        }

        if bad_blocks.len() >= 100 {
            println!("Too many bad blocks: {}, quitting!", bad_blocks.len());
            break;
        }

        if let Some(block_data) =
            aggregated_read_block(&mut recovered_fragments, block_id, &mut vdevs)
        {
            output_file.write_all(&block_data).unwrap();
        } else {
            bad_blocks.push(block_id);
            // Just write 0s
            output_file.write_all(&vec![0u8; file_block_size]).unwrap();
        }
    }
    println!("Bad blocks: ");
    for bad_block_id in bad_blocks {
        println!("{}", bad_block_id);
    }
}
