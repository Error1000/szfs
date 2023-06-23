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
    byte_iter::FromBytesLE,
    dmu::{DNode, DNodeDirectoryContents, DNodePlainFileContents, ObjSet},
    zio::{CompressionMethod, Vdevs},
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

impl Fragment {
    pub fn is_child_of(
        &mut self,
        vdevs: &mut Vdevs,
        self_hash: [u64; 4],
        potential_parent: &mut Fragment,
    ) -> bool {
        if potential_parent.children.contains(&self_hash) {
            return true;
        }

        match (&mut potential_parent.data, &mut self.data) {
            (FragmentData::IndirectBlock(parent), FragmentData::IndirectBlock(_us)) => {
                for bptr in parent.bps.iter_mut() {
                    if let Some(Ok(data)) = bptr.as_mut().map(|val| val.dereference(vdevs)) {
                        let hsh = hash_function(&data);
                        if hsh == self_hash {
                            return true;
                        }
                    }
                }

                return false;
            }

            (FragmentData::IndirectBlock(parent), FragmentData::FileDNode(_))
            | (FragmentData::IndirectBlock(parent), FragmentData::DirectoryDNode(_, _)) => {
                // Since indirect blocks have sizes that are multiples of 512 this is fine
                let Some(parent_data) = parent.get_data_with_gaps(vdevs) else {
                    return false;
                };

                return search_le_bytes_for_dnodes(&parent_data, vdevs)
                    .iter()
                    .any(|(hash, _)| *hash == self_hash);
            }

            (FragmentData::ObjSetDNode(parent), FragmentData::IndirectBlock(_us)) => {
                for bptr in parent.metadnode.get_block_pointers().iter_mut() {
                    if let Ok(data) = bptr.dereference(vdevs) {
                        let hsh = hash_function(&data);
                        if hsh == self_hash {
                            return true;
                        }
                    }
                }

                return false;
            }

            (FragmentData::DirectoryDNode(parent, _), FragmentData::IndirectBlock(_us)) => {
                for bptr in parent.0.get_block_pointers().iter_mut() {
                    if let Ok(data) = bptr.dereference(vdevs) {
                        let hsh = hash_function(&data);
                        if hsh == self_hash {
                            return true;
                        }
                    }
                }

                return false;
            }

            (FragmentData::FileDNode(parent), FragmentData::IndirectBlock(_us)) => {
                for bptr in parent.0.get_block_pointers().iter_mut() {
                    if let Ok(data) = bptr.dereference(vdevs) {
                        let hsh = hash_function(&data);
                        if hsh == self_hash {
                            return true;
                        }
                    }
                }

                return false;
            }

            // We won't deal with recreating the directory structure
            (FragmentData::DirectoryDNode(_, _), FragmentData::FileDNode(_us)) => {
                return false;
            }
            (FragmentData::DirectoryDNode(_, _), FragmentData::DirectoryDNode(_us, _)) => {
                return false;
            }

            // The objset owns the indirect blocks which in turn own the file and directory dnodes
            // So the objset doesn't need to directly own these types of fragments
            (FragmentData::ObjSetDNode(_), FragmentData::FileDNode(_us)) => {
                return false;
            }
            (FragmentData::ObjSetDNode(_), FragmentData::DirectoryDNode(_us, _)) => {
                return false;
            }

            // A file can't have other file or directory children
            (FragmentData::FileDNode(_), FragmentData::FileDNode(_us)) => {
                return false;
            }
            (FragmentData::FileDNode(_), FragmentData::DirectoryDNode(_us, _)) => {
                return false;
            }

            // Objsets don't have parents
            (FragmentData::DirectoryDNode(_, _), FragmentData::ObjSetDNode(_us))
            | (FragmentData::FileDNode(_), FragmentData::ObjSetDNode(_us))
            | (FragmentData::ObjSetDNode(_), FragmentData::ObjSetDNode(_us))
            | (FragmentData::IndirectBlock(_), FragmentData::ObjSetDNode(_us)) => {
                return false;
            }
        }
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

// Note: 'data' must be from a 512-byte aligned offset of the original device
//       This is because of an optimization taking advantage of the fact that dva offsets are always multiples of 512 and a dnode "slot" is 512 bytes in size in the Objset
// Source: https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L407 which uses SPA_MINBLOCKSHIFT and DVA_GET_OFFSET
// SPA_MINBLOCKSHIFT and DVA_GET_OFFSET can be found at: https://github.com/openzfs/zfs/blob/master/include/sys/fs/zfs.h#L1783 and https://github.com/openzfs/zfs/blob/master/include/sys/bitops.h#L66
// As you can see SPA_MINBLOCKSHIFT is 9 and the macro shifts by 9
// Thus proving that the current code is shifting the offset read from disk by 9
// thus meaning that all DVA offsets are multiples of 512
fn search_le_bytes_for_dnodes(data: &[u8], vdevs: &mut Vdevs) -> HashMap<[u64; 4], Fragment> {
    let mut res = HashMap::<[u64; 4], Fragment>::new();
    if data.len() % 512 != 0 {
        if cfg!(feature = "verbose_debug") {
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Can't search data that is not a multiple of 512 bytes in size, ignoring {} extra bytes!", data.len()%512);
        }
    }

    let mut data = data.chunks_exact(512);
    while let Some(sector) = data.next() {
        // Try to parse objset
        let mut objset_data = Vec::<u8>::new();
        objset_data.extend(sector);
        if let Some(extra_sector) = data.clone().next() {
            objset_data.extend(extra_sector);
        }

        let objset_data_hash = hash_function(&objset_data);

        // Note: This tries to parse it even if we don't have enough data, for a data recovery tool this seems like the better option
        if let Some(mut objset) = dmu::ObjSet::from_bytes_le(&mut objset_data.iter().copied()) {
            if objset
                .metadnode
                .get_block_pointers()
                .iter_mut()
                .any(|bp| bp.dereference(vdevs).is_ok())
            {
                res.insert(objset_data_hash, FragmentData::ObjSetDNode(objset).into());
            }
        };

        // Try to parse file or directory dnode
        let nsectors = dmu::DNode::get_n_slots_from_bytes_le(sector.iter().copied()).unwrap(); // NOTE: Unwrap should always succeed here, because we always have enough data
        let nextra_sectors_to_read = nsectors - 1;

        let mut dnode_data = Vec::<u8>::new();
        dnode_data.extend(sector);
        // We use a clone so as not to advance the actual iterator
        // so we don't accidentally ignore some sectors
        // because we read an invalid nsectors from one sector
        let mut data_iterator_clone = data.clone();
        for _ in 0..nextra_sectors_to_read {
            if let Some(extra_sector) = data_iterator_clone.next() {
                dnode_data.extend(extra_sector);
            } else {
                // If a Chunks Iterator returns None once, it will never return Some again, so no point in continuing
                break;
            }
        }

        let dnode_data_hash = hash_function(&dnode_data);
        // Note: This tries to parse it even if we don't have enough data, for a data recovery tool this seems like the better option
        let dnode = dmu::DNode::from_bytes_le(&mut dnode_data.into_iter());
        match dnode {
            Some(DNode::PlainFileContents(mut dnode)) => {
                if dnode
                    .0
                    .get_block_pointers()
                    .iter_mut()
                    .any(|bp| bp.dereference(vdevs).is_ok())
                {
                    res.insert(dnode_data_hash, FragmentData::FileDNode(dnode).into());
                }
            }
            Some(DNode::DirectoryContents(mut dnode)) => {
                if dnode
                    .0
                    .get_block_pointers()
                    .iter_mut()
                    .any(|bp| bp.dereference(vdevs).is_ok())
                {
                    let Some(contents) = dnode.dump_zap_contents(vdevs) else { continue; };
                    let contents = contents
                        .iter()
                        .map(|(name, _)| name)
                        .cloned()
                        .collect::<Vec<String>>();

                    res.insert(
                        dnode_data_hash,
                        FragmentData::DirectoryDNode(dnode, contents).into(),
                    );
                }
            }
            _ => (),
        }
    }

    res
}

// Returns: The roots of the graph
fn build_graph(nodes: &mut HashMap<[u64; 4], Fragment>, vdevs: &mut Vdevs) -> HashSet<[u64; 4]> {
    // This is because we can't do nested mutable loops due to the borrow checker
    // So instead we are going to collect all keys in a vector
    // and then loop over indices in the keys vector
    // Yes this is not optimal in terms of memory usage
    // But even with a million fragments
    // This is still only 32 mb of temporary memory
    let hashes = nodes
        .iter()
        .map(|(hash, _)| *hash)
        .collect::<Vec<[u64; 4]>>();
    let mut roots: HashSet<[u64; 4]> = hashes.iter().copied().collect::<_>();

    for i in 0..hashes.len() {
        let hash1 = hashes[i];
        println!(
            "Figuring out children of node {}/{}, with hash: {:?}",
            i + 1,
            hashes.len(),
            hash1
        );

        // Figure out the children of the fragment at the key at index i by going through all other fragments and checking if they are children of this fragment
        for j in 0..hashes.len() {
            if i == j {
                continue;
            }
            let hash2 = hashes[j];
            let [frag1, frag2] = nodes.get_many_mut([&hash1, &hash2]).unwrap();
            if frag2.is_child_of(vdevs, hash2, frag1) {
                frag1.children.insert(hash2);
                roots.remove(&hash2); // frag2 has a parent of frag1 so it's not a root
            }
        }
    }

    roots
}

// Returns fragments contained within the fragment to expand
fn expand_fragment(
    fragment_to_expand: &mut Fragment,
    vdevs: &mut Vdevs,
) -> Option<HashMap<[u64; 4], Fragment>> {
    let mut subfragments = HashMap::<[u64; 4], Fragment>::new();
    match &mut fragment_to_expand.data {
        FragmentData::FileDNode(file) => {
            for bp in file.0.get_block_pointers() {
                if let Ok(data) = bp.dereference(vdevs) {
                    if let Some(indirect_block) = IndirectBlock::from_bytes_le(&data, vdevs) {
                        let hsh = hash_function(&data);
                        subfragments
                            .insert(hsh, FragmentData::IndirectBlock(indirect_block).into());
                        fragment_to_expand.children.insert(hsh);
                    }
                }
            }
        }

        FragmentData::DirectoryDNode(dir, _) => {
            for bp in dir.0.get_block_pointers() {
                if let Ok(data) = bp.dereference(vdevs) {
                    if let Some(indirect_block) = IndirectBlock::from_bytes_le(&data, vdevs) {
                        let hsh = hash_function(&data);
                        subfragments
                            .insert(hsh, FragmentData::IndirectBlock(indirect_block).into());
                        fragment_to_expand.children.insert(hsh);
                    }
                }
            }
        }

        FragmentData::ObjSetDNode(objset) => {
            for bp in objset.metadnode.get_block_pointers() {
                if let Ok(data) = bp.dereference(vdevs) {
                    if let Some(indirect_block) = IndirectBlock::from_bytes_le(&data, vdevs) {
                        let hsh = hash_function(&data);
                        subfragments
                            .insert(hsh, FragmentData::IndirectBlock(indirect_block).into());
                        fragment_to_expand.children.insert(hsh);
                    }
                }
            }
        }

        FragmentData::IndirectBlock(indir) => {
            for bptr in indir.bps.iter_mut() {
                if let Some(Ok(data)) = bptr.as_mut().map(|val| val.dereference(vdevs)) {
                    if let Some(indirect_block) = IndirectBlock::from_bytes_le(&data, vdevs) {
                        let hsh = hash_function(&data);
                        subfragments
                            .insert(hsh, FragmentData::IndirectBlock(indirect_block).into());
                        fragment_to_expand.children.insert(hsh);
                    }
                }
            }

            if let Some(data) = indir.get_data_with_gaps(vdevs) {
                subfragments.extend(search_le_bytes_for_dnodes(&data, vdevs));
            }
        }
    }

    let mut subsubfragments = HashMap::<_, _>::new();
    if subfragments.len() != 0 {
        for (_, subfrag) in subfragments.iter_mut() {
            if let Some(res) = expand_fragment(subfrag, vdevs) {
                subsubfragments.extend(res);
            }
        }
    }
    subfragments.extend(subsubfragments);

    Some(subfragments)
}

fn dump_graph_to_stdout(fragments: &mut HashMap<[u64; 4], Fragment>) {
    println!("!!!Begin dump!!");
    let mut hashes_to_info = HashMap::<[u64; 4], String>::new();
    let mut current_index = 0;

    println!("Dumping id to hash mapping ...");
    for (hash, frag) in fragments.iter() {
        match &frag.data {
            FragmentData::DirectoryDNode(_, contents) => {
                let mut dir_contents_str = String::new();
                for file in contents {
                    dir_contents_str += file;
                    dir_contents_str += ", ";
                }
                dir_contents_str.pop();
                dir_contents_str.pop();

                println!(
                    "\"{:?}{}({})\" -> {:?}",
                    frag.data, current_index, dir_contents_str, hash
                );
                hashes_to_info.insert(
                    *hash,
                    format!("{:?}{}({})", frag.data, current_index, dir_contents_str),
                );
            }
            _ => {
                println!("\"{:?}{}\" -> {:?}", frag.data, current_index, hash);
                hashes_to_info.insert(*hash, format!("{:?}{}", frag.data, current_index));
            }
        }
        current_index += 1;
    }
    println!("Dumping graph using ids ...");
    for (hash, fragment) in fragments.iter() {
        for child_hash in fragment.children.iter() {
            println!(
                "\"{}\" -> \"{}\"",
                hashes_to_info[hash], hashes_to_info[child_hash]
            );
        }

        if fragment.children.is_empty() {
            println!("\"{}\"", hashes_to_info[hash]);
        }
    }
}

fn main() {
    // NOTE: Undelete tries to recover and reconstruct as much of the original structures as possible
    // This is where all metadata is gathered and then recover uses that metadata to do the actual recovery

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

    // The sizes are just the most common sizes i have seen while looking at the sizes of compressed indirect blocks, and also 512
    let compression_methods_and_sizes_to_try = [(
        CompressionMethod::Lz4,
        [512 * 2, 512 * 3, 512 * 8, 512 * 24, 512 * 256],
        [0], /* irrelevant for lz4 */
    )];

    // This is the main graph
    let mut recovered_fragments = HashMap::<[u64; 4], Fragment>::new();

    println!("RAIDZ total size (GB): {}", disk_size / 1024 / 1024 / 1024);
    println!("Step 1. Gathering basic fragments");

    let mut checkpoint_number = 0;
    for off in (0..disk_size).step_by(512) {
        if off % (128 * 1024 * 1024) == 0 && off != 0 {
            println!(
                "{}% done gathering basic fragments ...",
                ((off as f32) / (disk_size as f32)) * 100.0
            );
        }

        if off % (50 * 1024 * 1024 * 1024) == 0 && off != 0 {
            // Every ~50 GB
            println!("Saving checkpoint...");
            write!(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(format!("undelete-step1-checkpoint{checkpoint_number}.json"))
                    .unwrap(),
                "{}",
                &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>())
                    .unwrap()
            )
            .unwrap();
            checkpoint_number += 1;
            println!("Done!");
        }

        // NOTE: Currently asize is just not used even though it's part of the data structure, because we read it form disk
        let dva = szfs::zio::DataVirtualAddress::from(0, off, false);

        // Since we don't know what the size of the block(if there is any) at this offset might be
        // we just try all possible options
        for compression_method_and_sizes in compression_methods_and_sizes_to_try {
            for possible_comp_size in compression_method_and_sizes.1 {
                let Ok(data) = dva.dereference(&mut vdevs, possible_comp_size) else {
                    continue;
                };

                for possible_decomp_size in compression_method_and_sizes.2 {
                    let decomp_data = zio::try_decompress_block(
                        &data,
                        compression_method_and_sizes.0,
                        possible_decomp_size,
                    )
                    .unwrap_or_else(|partial_data| partial_data);

                    // Note: order is sort of important here
                    // because some blocks that are actually objsets might get misinterpreted
                    // as indirect blocks that only contain 3 block pointers
                    // but because we do the objset interpretation last
                    // if it succeeds it can override the bad indirect block interpretation by having the same hash

                    let indirect_block_data_hash = hash_function(&decomp_data);
                    if let Some(res) = IndirectBlock::from_bytes_le(&decomp_data, &mut vdevs) {
                        recovered_fragments.insert(
                            indirect_block_data_hash,
                            FragmentData::IndirectBlock(res).into(),
                        );
                    }

                    recovered_fragments
                        .extend(search_le_bytes_for_dnodes(&decomp_data, &mut vdevs));
                }
            }
        }
    }

    println!("Found {} basic fragments", recovered_fragments.len());
    println!("Saving checkpoint...");
    write!(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(format!("undelete-step1-checkpoint{checkpoint_number}.json"))
            .unwrap(),
        "{}",
        &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>()).unwrap()
    )
    .unwrap();
    checkpoint_number += 1;

    println!("Step 2. Building graph");

    let roots = build_graph(&mut recovered_fragments, &mut vdevs);

    println!("Saving checkpoint...");
    write!(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(format!("undelete-step2-checkpoint{checkpoint_number}.json"))
            .unwrap(),
        "{}",
        &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>()).unwrap()
    )
    .unwrap();
    checkpoint_number += 1;

    println!("Step 3. Expanding root fragments");

    for root_frag_hash in roots {
        println!("Expanding fragment {:?}", root_frag_hash);
        if let Some(res) = expand_fragment(
            recovered_fragments.get_mut(&root_frag_hash).unwrap(),
            &mut vdevs,
        ) {
            recovered_fragments.extend(res);
        }
    }

    println!("Saving checkpoint...");
    write!(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(format!("undelete-step3-checkpoint{checkpoint_number}.json"))
            .unwrap(),
        "{}",
        &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>()).unwrap()
    )
    .unwrap();
    checkpoint_number += 1;

    println!("Step 4. Rebuilding graph");
    let _roots = build_graph(&mut recovered_fragments, &mut vdevs);

    println!("Saving checkpoint...");
    write!(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(format!("undelete-step4-checkpoint{checkpoint_number}.json"))
            .unwrap(),
        "{}",
        &serde_json::to_string(&recovered_fragments.iter().collect::<Vec<(_, _)>>()).unwrap()
    )
    .unwrap();
    checkpoint_number += 1;

    dump_graph_to_stdout(&mut recovered_fragments);
}
