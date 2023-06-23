use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Write,
};
use szfs::{
    byte_iter::FromBytesLE,
    zio::{CompressionMethod, Vdevs},
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
            if let Some(bp) = zio::BlockPointer::from_bytes_le(&mut potential_bp.iter().copied()) {
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
}

fn main() {
    use szfs::ansi_color::*;

    let usage = format!(
        "Usage: {} (vdevs...) (offset) (psize) (lsize)",
        env::args().next().unwrap()
    );
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

    let off: u64 = str::parse(env::args().nth(5).expect(&usage).trim()).unwrap();
    let psize: usize = str::parse(env::args().nth(6).expect(&usage).trim()).unwrap();
    let lsize: usize = str::parse(env::args().nth(7).expect(&usage).trim()).unwrap(); // NOTE: Currently asize is just not used even though it's part of the data structure, because we read it form disk
    let dva = szfs::zio::DataVirtualAddress::from(0, off, false);
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
