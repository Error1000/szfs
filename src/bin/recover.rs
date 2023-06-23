use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    env,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
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

fn aggregated_read_block(
    block_id: usize,
    fragments: &mut LruCache<[u64; 4], Fragment>,
    vdevs: &mut Vdevs,
) -> Result<(Vec<u8>, [u64; 4]), ()> {
    let mut res = Err(());
    for f in fragments.iter_mut() {
        if let FragmentData::FileDNode(file) = &mut f.1.data {
            if let Ok(res_block_data) = file.0.read_block(block_id, vdevs) {
                res = Ok((res_block_data, *f.0));
                // I just realized why my code is slow
                // i forgot to break, *facepalm*
                break;
            }
        }
    }

    if let Ok((_, hsh)) = res {
        fragments.get(&hsh); // Update LRU
    }

    res
}

fn main() {
    use szfs::ansi_color::*;
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
            // Note: I happen to have reliably recovered the creation time
            // and I also am pretty sure that no other files were created in the same second
            // So for my use case this is a pretty good filter
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

    let biggest_file_hsh = recovered_fragments[0].0;
    let mut recovered_fragments: LruCache<[u64; 4], Fragment> = {
        let mut res = LruCache::unbounded();
        for e in recovered_fragments {
            res.put(e.0, e.1);
        }
        res
    };

    recovered_fragments.get(&biggest_file_hsh); // Update LRU

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

    let mut output_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("recovered-file.bin")
        .unwrap();

    let mut resuming_block = 0;
    // NOTE: A file where offset 0 is the last offset is of size 1
    if output_file.metadata().unwrap().len() > 0 {
        let resuming_offset = output_file.metadata().unwrap().len() - 1;
        output_file.seek(SeekFrom::Start(resuming_offset)).unwrap();
        resuming_block = (resuming_offset / (file_block_size as u64))
            .try_into()
            .unwrap();
    }
    println!("Resuming from block {resuming_block}!");

    let nblocks_in_file = file_size / file_block_size
        + if file_size % file_block_size != 0 {
            1
        } else {
            0
        };

    let mut nbad_blocks = 0;

    for block_id in resuming_block..nblocks_in_file {
        if block_id % (4 * 1024) == 0 {
            // Every ~512 mb
            println!(
                "Copying data {}% done, {} bad blocks so far ...",
                (block_id as f32 / nblocks_in_file as f32) * 100.0,
                nbad_blocks
            );
        }

        if let Ok((block_data, _)) =
            aggregated_read_block(block_id, &mut recovered_fragments, &mut vdevs)
        {
            assert!(block_data.len() == file_block_size);
            output_file.write_all(&block_data).unwrap();
        } else {
            println!("Block {block_id} is bad!");
            nbad_blocks += 1;

            // Just write 0s
            output_file.write_all(&vec![0u8; file_block_size]).unwrap();
        }
    }
}
