use std::{
    collections::HashMap,
    env,
    fs::{File, OpenOptions},
    io::Write,
};
use szfs::{fletcher::do_fletcher4, zio::Vdevs, *};

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

    let psize = 256 * 512;
    print!("Hash to look for: ");
    let mut input_line = String::new();
    std::io::stdout().flush().unwrap();
    std::io::stdin()
        .read_line(&mut input_line)
        .expect("Reading a line should work!");
    let checksum_to_look_for = parse_hsh_from_str(input_line.trim()).unwrap();

    use rayon::prelude::*;

    for off in (0..disk_size).step_by(512 * num_cpus::get() * 256) {
        if off % (512 * 1024 * 1024) == 0 && off != 0 {
            println!(
                "{}% of the disk has been searched ...",
                ((off as f32) / (disk_size as f32)) * 100.0
            );
        }

        let mut results = Vec::new();
        for toff in (off..off + (512 * num_cpus::get() * 256) as u64).step_by(512) {
            // NOTE: Currently asize is just not used even though it's part of the data structure, because we read it form disk
            let dva = szfs::zio::DataVirtualAddress::from(0, 512, toff, false);
            let res = dva.dereference(&mut vdevs, psize).unwrap();
            results.push(res);
        }

        let res: Vec<Vec<u8>> = results
            .into_par_iter()
            .filter(|data| do_fletcher4(data) == checksum_to_look_for)
            .collect();

        if !res.is_empty() {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open("found-block.bin")
                .unwrap()
                .write_all(&res[0])
                .unwrap();
            break;
        }
    }
}

fn parse_hsh_from_str(s: &str) -> Result<[u64; 4], ()> {
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
