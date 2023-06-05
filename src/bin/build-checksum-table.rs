use serde::{Deserialize, Serialize};
use std::{
    env,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
};
use szfs::{byte_iter::FromBytesLE, zio::Vdevs, *};
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
}

type ChecksumTableEntry = u32;

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
    let sector_size = vdev_raidz.get_asize() as u64;

    let mut checksum_map_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("checksum-map.bin")
        .unwrap();
    let checksum_map_file_size = checksum_map_file.seek(SeekFrom::End(0)).unwrap();
    let last_off =
        (checksum_map_file_size / core::mem::size_of::<ChecksumTableEntry>() as u64) * sector_size;
    println!(
        "RAIDZ total size (GB): {}",
        disk_size as f64 / 1024.0 / 1024.0 / 1024.0
    );

    println!(
        "Resuming from offset {}, which is sector {}, with sector size being: {}",
        last_off,
        last_off / sector_size,
        sector_size
    );

    for off in (last_off..disk_size).step_by(sector_size as usize) {
        if off % (512 * 1024 * 1024) == 0 && off != 0 {
            // Every ~512 mb
            println!(
                "{}% done building table ...",
                ((off as f32) / (disk_size as f32)) * 100.0
            );
        }

        let res = vdev_raidz.read(off, sector_size as usize).unwrap();
        let checksum = fletcher::do_fletcher4(&res);

        // Truncate to size
        let to_write: ChecksumTableEntry = checksum[0] as ChecksumTableEntry;
        checksum_map_file
            .write_all(&to_write.to_le_bytes())
            .unwrap();
    }
}
