use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    env,
    fmt::Debug,
    fs::File,
};
use szfs::{
    dmu::{DNodeDirectoryContents, DNodePlainFileContents, ObjSet},
    zio::Vdevs,
    *,
};

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

fn aggregated_lookup_block(
    block_id: usize,
    fragments: &mut [([u64; 4], Fragment)],
    vdevs: &mut Vdevs,
) -> Result<(HashSet<[u64; 4]>, HashSet<u64>), ()> {
    let mut hashes = HashSet::<[u64; 4]>::new();
    let mut offsets = HashSet::<u64>::new();

    for f in fragments.iter_mut() {
        if let FragmentData::FileDNode(file) = &mut f.1.data {
            if let Ok(zio::BlockPointer::Normal(bp)) =
                file.0.get_data_block_pointer(block_id, vdevs)
            {
                hashes.insert(bp.get_checksum());
                offsets.extend(
                    bp.get_dvas()
                        .iter()
                        .filter_map(|v| v.as_ref())
                        .map(|dva| dva.parse_offset()),
                );
            }
        }
    }

    Ok((hashes, offsets))
}

fn main() {
    // Used to gather metadata about the blocks of the recovered file
    // Useful for carrying out special recovery on blocks that failed the checksum (a.k.a bad blocks)

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

    if cfg!(debug_assertions) {
        println!("{RED}Important{WHITE}: This is not an optimized binary!");
    }

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

    recovered_fragments.retain_mut(|frag| {
        if let FragmentData::FileDNode(file) = &mut frag.1.data {
            let file_cr_time_unix_timestamp = u64::from_le_bytes(
                file.0.get_bonus_data()[14 * 8..14 * 8 + 8]
                    .try_into()
                    .unwrap(),
            );
            file_cr_time_unix_timestamp == 1674749006
        } else {
            false
        }
    });

    recovered_fragments.sort_unstable_by_key(|f| {
        let FragmentData::FileDNode(f) = &f.1.data else {panic!("");};
        Reverse(f.0.get_data_size())
    });

    for res in recovered_fragments.iter() {
        println!("{:?}", res);
    }

    let bad_blocks: Vec<usize> =
        serde_json::from_reader(File::open("bad_blocks.json").unwrap()).unwrap();

    let bad_blocks: HashSet<usize> = bad_blocks.into_iter().collect();

    println!(
        "N fragments loaded form checkpoint: {}",
        recovered_fragments.len()
    );

    println!("RAIDZ total size (GB): {}", disk_size / 1024 / 1024 / 1024);

    // NOTE: This is specifically meant for my scenario
    // where i lost a big file that i have recovered the size of
    // in a fs that only ever had 2-3 files
    let file_size: usize = 1084546955827;

    // I know the block size of the file system i'm recovering from
    let file_block_size: usize = 128 * 1024;

    let nblocks_in_file = file_size / file_block_size
        + if file_size % file_block_size != 0 {
            1
        } else {
            0
        };

    for block_id in 0..nblocks_in_file {
        if !bad_blocks.contains(&block_id) {
            continue;
        }

        if block_id % (4 * 1024) == 0 {
            // Every ~512 mb
            println!(
                "{}% done ...",
                (block_id as f32 / nblocks_in_file as f32) * 100.0
            );
        }

        if let Ok(block_info) =
            aggregated_lookup_block(block_id, &mut recovered_fragments, &mut vdevs)
        {
            if block_info.0.is_empty() {
                println!("Really bad block: {}", block_id);
            } else if (block_info.0.len() == 1 && bad_blocks.contains(&block_id))
                || block_info.0.len() > 1
            {
                println!(
                    "Block {} has hashes: {:?}, and offsets: {:?}",
                    block_id, block_info.0, block_info.1
                );
            }
        } else {
            println!("Really bad block: {}", block_id);
        }
    }
}
