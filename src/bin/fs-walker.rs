use std::{collections::HashMap, fs::OpenOptions, io::Write};
use szfs::{zio::Vdevs, *};

fn main() {
    use szfs::ansi_color::*;

    let Ok(vdev0) = std::fs::OpenOptions::new().read(true).write(false).create(false).open("./test/vdev0.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev0!");
        return;
    };
    let mut vdev0: VdevFile = vdev0.into();

    let Ok(vdev1) = std::fs::OpenOptions::new().read(true).write(false).create(false).open("./test/vdev1.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev1!");
        return;
    };
    let mut vdev1: VdevFile = vdev1.into();

    let Ok(vdev2) = std::fs::OpenOptions::new().read(true).write(false).create(false).open("./test/vdev2.bin")
    else {
        println!("{RED}Fatal{WHITE}: Failed to open vdev2!");
        return;
    };
    let mut vdev2: VdevFile = vdev2.into();

    let Ok(vdev3) = std::fs::OpenOptions::new().read(true).write(false).create(false).open("./test/vdev3.bin")
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

    let nvlist::Value::U64(_label_txg) = name_value_pairs["txg"] else {
        panic!("no txg found in label!");
    };

    println!("{CYAN}Info{WHITE}: Parsed nv_list, {:?}!", name_value_pairs);

    let mut devices = Vdevs::new();
    devices.insert(0, &mut vdev0);
    devices.insert(1, &mut vdev1);
    devices.insert(2, &mut vdev2);
    devices.insert(3, &mut vdev3);

    let mut vdev_raidz: VdevRaidz =
        VdevRaidz::from_vdevs(devices, 4, 1, 2_usize.pow(top_level_ashift as u32));

    label0.set_raw_uberblock_size(2_usize.pow(top_level_ashift as u32));

    let mut uberblocks = Vec::<Uberblock>::new();
    for i in 0..label0.get_raw_uberblock_count() {
        let raw_uberblock = label0.get_raw_uberblock(i);
        if let Some(uberblock) = Uberblock::from_bytes(&mut raw_uberblock.iter().copied()) {
            uberblocks.push(uberblock);
        }
    }

    println!("{CYAN}Info{WHITE}: Found {} uberblocks!", uberblocks.len());
    uberblocks.sort_unstable_by(|a, b| a.txg.cmp(&b.txg));

    let mut vdevs = HashMap::<usize, &mut dyn Vdev>::new();
    vdevs.insert(0usize, &mut vdev_raidz);

    let mut uberblock_search_info = None;
    for ub in uberblocks.iter_mut().rev() {
        if let Ok(data) = ub.rootbp.dereference(&mut vdevs) {
            uberblock_search_info = Some((ub, data));
            break;
        }
    }

    let (active_uberblock, mos_data) = uberblock_search_info.unwrap();
    println!("{CYAN}Info{WHITE}: Using {:?}", active_uberblock);

    let mut meta_object_set =
        dmu::ObjSet::from_bytes_le(&mut mos_data.iter().copied()).expect("Mos should be valid!");

    let dmu::DNode::ObjectDirectory(mut object_directory) = meta_object_set.get_dnode_at(1, &mut vdevs).expect("Object directory should be valid!")
    else {panic!("DNode 1 is not an object directory!"); };
    let objdir_zap_data = object_directory.dump_zap_contents(&mut vdevs).unwrap();

    println!(
        "{CYAN}Info{WHITE}: Meta object set obj directory zap: {:?}",
        objdir_zap_data
    );

    let zap::Value::U64(root_dataset_number) = objdir_zap_data["root_dataset"] else {
        panic!("Couldn't read root_dataset id!");
    };

    let dmu::DNode::DSLDirectory(root_dataset) = meta_object_set.get_dnode_at(root_dataset_number as usize, &mut vdevs).unwrap() else {
        panic!("DNode {} which is the root_dataset is not a dsl directory!", root_dataset_number);
    };

    let head_dataset_number = root_dataset
        .parse_bonus_data()
        .unwrap()
        .get_head_dataset_object_number();
    let dmu::DNode::DSLDataset(head_dataset) = meta_object_set.get_dnode_at(head_dataset_number as usize, &mut vdevs).unwrap() else {
        panic!("DNode {} whichs is the head_dataset is not a dsl dataset!", head_dataset_number);
    };
    let mut head_dataset_bonus = head_dataset.parse_bonus_data().unwrap();
    let head_dataset_blockpointer = head_dataset_bonus.get_block_pointer();

    println!(
        "{CYAN}Info{WHITE}: Head dataset objset block pointer: {:?}",
        head_dataset_blockpointer
    );
    // Now we have access to the dataset we are interested in
    let mut head_dataset_object_set = dmu::ObjSet::from_bytes_le(
        &mut head_dataset_blockpointer
            .dereference(&mut vdevs)
            .unwrap()
            .iter()
            .copied(),
    )
    .unwrap();

    let dmu::DNode::MasterNode(mut head_dataset_master_node) = head_dataset_object_set.get_dnode_at(1, &mut vdevs).unwrap() else {
        panic!("DNode 1 which is the master_node is not a master node!");
    };

    let master_node_zap_data = head_dataset_master_node
        .dump_zap_contents(&mut vdevs)
        .unwrap();

    println!(
        "{CYAN}Info{WHITE}: Root dataset master node zap: {:?}",
        master_node_zap_data
    );

    let zap::Value::U64(system_attributes_info_number) = master_node_zap_data["SA_ATTRS"] else {
        panic!("SA_ATTRS entry is not a number!");
    };

    let mut system_attributes = zpl::SystemAttributes::from_attributes_node_number(
        system_attributes_info_number as usize,
        &mut head_dataset_object_set,
        &mut vdevs,
    )
    .unwrap();

    let zap::Value::U64(root_number) = master_node_zap_data["ROOT"] else {
        panic!("ROOT zap entry is not a number!");
    };

    let dmu::DNode::DirectoryContents(mut root_node) = head_dataset_object_set.get_dnode_at(root_number as usize, &mut vdevs).unwrap() else {
        panic!("DNode {} which is the root dnode is not a directory contents node!", root_number);
    };

    let root_node_zap_data = root_node.dump_zap_contents(&mut vdevs).unwrap();
    println!("Root directory data zap: {:?}", root_node_zap_data);

    let zap::Value::U64(mut file_node_number) = root_node_zap_data["file.bin"] else {
        panic!("File entry is not a number!");
    };

    // Only bottom 48 bits are the actual object id
    // Source: https://github.com/openzfs/zfs/blob/master/include/sys/zfs_znode.h#L152
    file_node_number &= (1 << 48) - 1;

    let szfs::dmu::DNode::PlainFileContents(mut file_node) = head_dataset_object_set.get_dnode_at(file_node_number as usize, &mut vdevs).unwrap() else {
        panic!("DNode {} which is the file node is not a plain file contents node!", file_node_number);
    };

    let file_info = system_attributes
        .parse_system_attributes_bytes_le(&mut file_node.0.get_bonus_data().iter().copied())
        .unwrap();
    let zpl::Value::U64(file_len) = file_info["ZPL_SIZE"] else {
        panic!("File length is not a number!");
    };
    println!("File size: {:?}", file_len);
    OpenOptions::new()
        .create(true)
        .write(true)
        .open("file.bin")
        .unwrap()
        .write_all(&file_node.0.read(0, file_len as usize, &mut vdevs).unwrap())
        .unwrap();
}
