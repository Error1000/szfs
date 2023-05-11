use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Write,
};
use szfs::{
    dmu::{DNode, DNodeDirectoryContents, DNodePlainFileContents, ObjSet},
    zio::{BlockPointer, CompressionMethod, Vdevs},
    *,
};
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
                res.push(Some(bp));
                nfound += 1;
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
            .filter(|bp| bp.is_some())
            .next()
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

    println!("RAIDZ total size (GB): {}", disk_size / 1024 / 1024 / 1024);

    let off: u64 = str::parse(env::args().nth(5).unwrap().trim()).unwrap();
    let psize: usize = str::parse(env::args().nth(6).unwrap().trim()).unwrap();
    let lsize: usize = str::parse(env::args().nth(7).unwrap().trim()).unwrap();
    // NOTE: Currently asize is just not used even though it's part of the data structure, because we read it form disk
    let dva = szfs::zio::DataVirtualAddress::from(0, 512, off, false);
    let res = dva.dereference(&mut vdevs, psize).unwrap();
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open("dva-data-raw.bin")
        .unwrap()
        .write_all(&res)
        .unwrap();

    println!("Fletcher4 checksum: {:?}!", fletcher::do_fletcher4(&res));
    let res_decomp =
        zio::try_decompress_block(&res, CompressionMethod::Lz4, lsize).unwrap_or_else(|res| res);

    let indir = IndirectBlock::from_bytes_le(&res_decomp, &mut vdevs).unwrap();
    write!(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open("dva-data-indir.json")
            .unwrap(),
        "{}",
        &serde_json::to_string(&indir).unwrap()
    )
    .unwrap();
}
