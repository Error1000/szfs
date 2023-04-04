use std::{collections::HashMap};
use szfs::{*, zio::{CompressionMethod, Vdevs}, dmu::{DNode, DNodePlainFileContents, DNodeDirectoryContents, ObjSet}};

#[derive(Debug)]
struct IndirectBlock {
    bps: Vec<zio::BlockPointer>
}

impl IndirectBlock {
    pub fn from_bytes_le(data: &[u8], vdevs: &mut Vdevs) -> Option<IndirectBlock> {
        let mut res = Vec::<zio::BlockPointer>::new();
        let data = data.chunks(zio::BlockPointer::get_ondisk_size());
        for potential_bp in data {
            if let Some(mut bp) = zio::BlockPointer::from_bytes_le(&mut potential_bp.iter().copied()) {
                // Verify block pointer
                // NOTE: This might not necessarily guarantee that the block pointer
                // wasn't just misinterpreted random data, especially if
                // it is an embedded block pointer
                if bp.dereference(vdevs).is_ok() {
                    res.push(bp);
                }
            } else {
                continue;
            }
        }

        Some(IndirectBlock { bps: res })
    }
}

#[derive(Debug)]
enum Fragment {
    FileDNode(DNodePlainFileContents),
    DirectoryDNode(DNodeDirectoryContents),
    ObjSetDNode(ObjSet),
    IndirectBlock(IndirectBlock)
}


// Note: 'data' must be from a 512-byte aligned offset of the original device
//       This is because of an optimization taking advantage of the fact that dva offsets are always multiples of 512 and a dnode "slot" is 512 bytes in size in the Objset
// Source: https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L407 which uses SPA_MINBLOCKSHIFT and DVA_GET_OFFSET
// SPA_MINBLOCKSHIFT and DVA_GET_OFFSET can be found at: https://github.com/openzfs/zfs/blob/master/include/sys/fs/zfs.h#L1783 and https://github.com/openzfs/zfs/blob/master/include/sys/bitops.h#L66
// As you can see SPA_MINBLOCKSHIFT is 9 and the macro shifts by 9
// Thus proving that the current code is shifting the offset read from disk by 9
// thus meaning that all DVA offsets are multiples of 512
fn search_le_bytes_for_dnodes(data: &[u8], vdevs: &mut Vdevs) -> Vec<Fragment> {
    let mut res = Vec::<Fragment>::new();
    let mut data = data.chunks(512);
    while let Some(sector) = data.next() {
        // Try to parse objset
        let mut objset_data = Vec::<u8>::new();
        objset_data.extend(sector);
        if let Some(extra_sector) = data.clone().next() {
            objset_data.extend(extra_sector);
        }

        // Note: This tries to parse it even if we don't have enough data, for a data recovery tool this seems like the better option
        if let Some(mut objset) = dmu::ObjSet::from_bytes_le(&mut objset_data.iter().copied()) {
            if objset.metadnode.get_block_pointers().iter_mut().any(|bp|bp.dereference(vdevs).is_ok()){
                res.push(Fragment::ObjSetDNode(objset));
            }
        };
        

        // Try to parse file or directory dnode
        let nsectors = dmu::DNode::get_n_slots_from_bytes_le(sector.iter().copied()).unwrap(); // NOTE: Unwrap should always succeed here, because we always have enough data
        let nextra_sectors_to_read = nsectors-1;

        let mut dnode_data = Vec::<u8>::new();
        dnode_data.extend(sector);
        // We use a clone so as not to advance the actual iterator
        // so we don't accidentally ignore some sectors
        // because we read an invalid nsectors from one sector
        let mut data_iterator_clone = data.clone();
        for _ in 0..nextra_sectors_to_read {
            if let Some(extra_sector) = data_iterator_clone.next(){
                dnode_data.extend(extra_sector);
            }else{
                // If a Chunks Iterator returns None once, it will never return Some again, so no point in continuing
                break;
            }
        }

        // Note: This tries to parse it even if we don't have enough data, for a data recovery tool this seems like the better option
        let dnode = dmu::DNode::from_bytes_le(&mut dnode_data.into_iter());
        match dnode {
            Some(DNode::PlainFileContents(mut dnode)) => {
                if dnode.0.get_block_pointers().iter_mut().any(|bp| bp.dereference(vdevs).is_ok()) {
                    res.push(Fragment::FileDNode(dnode))
                }
            },
            Some(DNode::DirectoryContents(mut dnode)) => {
                if dnode.0.get_block_pointers().iter_mut().any(|bp| bp.dereference(vdevs).is_ok()) {
                    res.push(Fragment::DirectoryDNode(dnode))
                }
            },
            _ => ()
        }
    }
    
    res
}


fn main() {
    use szfs::ansi_color::*;

    let Ok(vdev0) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev0.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev0!");
        return;
    };
    let mut vdev0: VdevFile = vdev0.into();

    let Ok(vdev1) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev1.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev1!");
        return;
    };
    let mut vdev1: VdevFile = vdev1.into();

    let Ok(vdev2) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev2.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev2!");
        return;
    };
    let mut vdev2: VdevFile = vdev2.into();

    let Ok(vdev3) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&"./test/vdev3.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev3!");
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


    let compression_methods_and_sizes_to_try = 
        [(CompressionMethod::Lz4, [512], [0]/* irrelevant for lz4 */)];

    // Gather basic fragments
    let mut recovered_fragments = Vec::<Fragment>::new();
    for off in (0..disk_size-512*2).step_by(512) {
        if off % (512*100_000) == 0 {
            println!("{}% done recovering fragments ...", ((off as f32)/(disk_size as f32))*100.0);
        }

        // NOTE: Currently asize is just not used even though it's part of the data structure, because we read it form disk
        let dva = szfs::zio::DataVirtualAddress::from(0, 512, off, false);

        // Since we don't know what the size of the block(if there is any) at this offset might be
        // we just try all possible options
        for compression_method_and_sizes in compression_methods_and_sizes_to_try {
            for possible_block_size in compression_method_and_sizes.1 {
                let Ok(data) = dva.dereference(&mut vdevs, possible_block_size) else {
                    continue;
                };

                for possible_decomp_size in compression_method_and_sizes.2 {
                    let Ok(decomp_data) = zio::try_decompress_block(&data, compression_method_and_sizes.0, possible_decomp_size) else {
                        continue;
                    };

                    let res = search_le_bytes_for_dnodes(&decomp_data, &mut vdevs);
                    recovered_fragments.extend(res);
                    if let Some(res) = IndirectBlock::from_bytes_le(&decomp_data, &mut vdevs) {
                        recovered_fragments.push(Fragment::IndirectBlock(res));
                    }
                }
            }
        }

    }

    println!("Found {} basic fragments", recovered_fragments.len());
    for fragment in recovered_fragments {
        match fragment {
            Fragment::FileDNode(_) => print!("FileDNode "),
            Fragment::DirectoryDNode(_) => print!("DirectoryDNode "),
            Fragment::ObjSetDNode(_) => print!("ObjSetDNode "),
            Fragment::IndirectBlock(_) => print!("IndirectBlock "),
        }
    }
}
