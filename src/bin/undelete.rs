use std::{collections::HashMap, fs::OpenOptions, io::Write};
use szfs::{*, zio::{CompressionMethod, Vdevs}};

fn main() {
    use szfs::ansi_color::*;

    let Ok(vdev0) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev0.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev!");
        return;
    };
    let mut vdev0: VdevFile = vdev0.into();

    let Ok(vdev1) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev1.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev!");
        return;
    };
    let mut vdev1: VdevFile = vdev1.into();

    let Ok(vdev2) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev2.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev!");
        return;
    };
    let mut vdev2: VdevFile = vdev2.into();

    let Ok(vdev3) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev3.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev!");
        return;
    };
    let mut vdev3: VdevFile = vdev3.into();

    // For now just use the first label
    let mut label0 = VdevLabel::from_bytes(&vdev0.read_raw_label(0).expect("Vdev label 0 must be parsable!"));

    let name_value_pairs = nvlist::from_bytes_xdr(&mut label0.get_name_value_pairs_raw().iter().copied()).expect("Name value pairs in the vdev label must be valid!");
    let nvlist::Value::NVList(vdev_tree) = &name_value_pairs["vdev_tree"] else {
        panic!("vdev_tree is not an nvlist!");
    };

    let nvlist::Value::U64(top_level_ashift) = vdev_tree["ashift"] else {
        panic!("no ashift found for top level vdev!");
    };

    let nvlist::Value::U64(_label_txg) = name_value_pairs["txg"] else {
        panic!("no txg found in label!");
    };

    println!("{CYAN}Info{WHITE}: Parsed nv_list, {:?}!", name_value_pairs);


    let mut devices = Vdevs::new();
    devices.insert(0, &mut vdev0);
    devices.insert(1, &mut vdev1);
    devices.insert(2, &mut vdev2);
    devices.insert(3, &mut vdev3);

    let mut vdev_raidz: VdevRaidz = VdevRaidz::from_vdevs(devices, 1, 2_usize.pow(top_level_ashift as u32));

    label0.set_raw_uberblock_size(2_usize.pow(top_level_ashift as u32));

    let disk_size = vdev_raidz.get_size();

    let mut vdevs = HashMap::<usize, &mut dyn Vdev>::new();
    vdevs.insert(0usize, &mut vdev_raidz);

    let mut recovered_file_number = 1;
    // Search the entire disk for FileContents dnodes
    for off in (0..disk_size).step_by(512) {

        if off % (512*1_000_000) == 0 {
            println!("{}% done", ((off as f32)/(disk_size as f32))*100.0);
        }

        let dva = szfs::zio::DataVirtualAddress::from(0, 0, off, false);
        let Ok(data) = dva.dereference(&mut vdevs, 512) else {
            continue;
        };

        let mut decomp_data = None;
        for compression_method in [CompressionMethod::Lz4, CompressionMethod::Lzjb, CompressionMethod::Off] {
            match compression_method {
                CompressionMethod::Off => decomp_data = Some(data.clone()),
                CompressionMethod::Lz4 | CompressionMethod::On => {
                    let comp_size = u32::from_be_bytes(data[0..4].try_into().unwrap());
                    if comp_size as usize > data.len()-4 {
                        continue;
                    }

                    // The data contains the size of the input as a big endian 32 bit int at the beginning before the lz4 stream starts
                    let Ok(res) = lz4::lz4_decompress_blocks(&mut data[4..comp_size as usize+4].iter().copied()).map_err(|_| ())
                    else { continue; };
                    decomp_data = Some(res);
                    break;
                },
                CompressionMethod::Lzjb => {
                    let Ok(res) = lzjb::lzjb_decompress(&mut data.iter().copied(), szfs::dmu::ObjSet::get_ondisk_size())
                    else { continue; };
                    decomp_data = Some(res);
                    break;
                },
                _ => todo!("Implement {:?} compression!", compression_method),
            };
        };

        let Some(decomp_data) = decomp_data else {
            continue;
        };

        let Some(mut objset) = szfs::dmu::ObjSet::from_bytes_le(&mut decomp_data.into_iter()) else {
            continue;
        };
        
        let Some(dmu::DNode::DirectoryContents(mut root_node)) = objset.get_dnode_at(34, &mut vdevs) else {
            continue;
        };

        let root_node_zap_data = root_node.dump_zap_contents(&mut vdevs).unwrap();
        println!("Root directory: {:?}", root_node_zap_data);
        let Some(zap::Value::U64(mut file_node_number)) = root_node_zap_data.get("test.mkv") else {
            continue;
        };
    
        // Only bottom 48 bits are the actual object id
        // Source: https://github.com/openzfs/zfs/blob/master/include/sys/zfs_znode.h#L152
        file_node_number &= (1 << 48) - 1;
    
        let Some(szfs::dmu::DNode::PlainFileContents(mut file_node)) = objset.get_dnode_at(file_node_number as usize, &mut vdevs) else {
            continue;
        };
    
        let mut data = Vec::<u8>::new();
        let blksize = file_node.0.parse_data_block_size();
        for blkid in 0..file_node.0.get_data_size()/blksize {
            let Ok(block_contents) = file_node.0.read_block(blkid, &mut vdevs) else {
                data.extend(&vec![0u8; blksize]);
                continue;
            };
            data.extend(&block_contents);
        }

        OpenOptions::new().create(true).write(true).open(&format!("file{}.mkv", recovered_file_number))
        .unwrap()
        .write_all(&data)
        .unwrap();  
        recovered_file_number += 1;
    }
}
