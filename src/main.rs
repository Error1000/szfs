use std::{fs::File, io::{Read, Write, Seek, SeekFrom}, os::unix::prelude::MetadataExt, collections::HashMap};

use byte_iter::ByteIter;

mod nvlist;
mod byte_iter;
mod zio;
mod fletcher;

pub trait Vdev {
    fn get_size(&self) -> u64;
    fn read(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()>;
    fn write(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()>;
    fn read_raw_label0(&mut self) -> Result<Vec<u8>, ()>;
    fn read_raw_label1(&mut self) -> Result<Vec<u8>, ()>;
    fn read_raw_label2(&mut self) -> Result<Vec<u8>, ()>;
    fn read_raw_label3(&mut self) -> Result<Vec<u8>, ()>;
}


#[derive(Debug)]
struct VdevFile {
    device: File,
}

impl Vdev for VdevFile {
    fn read(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        let mut buf = vec![0u8; amount_in_bytes];
        self.device.seek(SeekFrom::Start(offset_in_bytes)).map_err(|_| ())?;
        self.device.read(&mut buf).map_err(|_| ())?;
        Ok(buf)
    }

    fn write(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        self.device.seek(SeekFrom::Start(offset_in_bytes)).map_err(|_| ())?;
        self.device.write(data).map_err(|_| ())?;
        Ok(())
    }

    fn get_size(&self) -> u64 {
        self.device.metadata().expect("File must have size in metadata!").size()
    }

    // Source: http://www.giis.co.in/Zfs_ondiskformat.pdf
    // Section 1.2.1

    fn read_raw_label0(&mut self) -> Result<Vec<u8>, ()>{
        self.read(0, 256*1024)
    }

    fn read_raw_label1(&mut self) -> Result<Vec<u8>, ()>{
        self.read(256*1024, 256*1024)
    }

    fn read_raw_label2(&mut self) -> Result<Vec<u8>, ()>{
        self.read(self.get_size()-512*1024, 256*1024)
    }

    fn read_raw_label3(&mut self) -> Result<Vec<u8>, ()>{
        self.read(self.get_size()-256*1024, 256*1024)
    }
}

impl From<File> for VdevFile {
    fn from(f: File) -> Self {
        Self { device: f }
    }
}


#[derive(Debug)]
struct VdevLabel {
    name_value_pairs_raw: Vec<u8>,
    uberblocks_raw: Vec<u8>,
    uberblock_size: Option<usize>,
}

impl VdevLabel {
    pub fn from_bytes(data: &[u8]) -> VdevLabel {
        VdevLabel { 
            name_value_pairs_raw: data[16*1024..128*1024].to_owned(), 
            uberblocks_raw: data[128*1024..].to_owned(),
            uberblock_size: None
        }
    }

    pub fn set_raw_uberblock_size(&mut self, uberblock_size: usize) {
        if self.uberblock_size.is_some() {
            panic!("Can't set uberblock size twice!");
        } else {
            self.uberblock_size = Some(uberblock_size);
        }
    }

    pub fn get_raw_uberblock_size(&self) -> usize {
        self.uberblock_size.expect("Uberblock size should be initialised!")
    }

    pub fn get_raw_uberblock(&self, index: usize) -> &[u8] {
        if index >= self.get_raw_uberblock_count() { panic!("Attempt to get uberblock pas the end of the uberblock array!"); }
        &self.uberblocks_raw[index*self.get_raw_uberblock_size()..(index+1)*self.get_raw_uberblock_size()]
    }

    pub fn get_raw_uberblock_count(&self) -> usize {
        self.uberblocks_raw.len()/self.get_raw_uberblock_size()
    }
}


#[derive(Debug)]
struct Uberblock {
    ub_magic: u64,
    ub_version: u64,
    ub_txg: u64,
    ub_guid_sum: u64,
    ub_timestamp: u64,
    ub_rootbp: zio::BlockPointer
}

const UBERBLOCK_MAGIC: u64 = 0x00bab10c;
impl Uberblock {
   pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<Uberblock> {
        let ub_magic = data.read_u64_le()?;

        // Verify magic, to make sure we are using the correct endianness
        if ub_magic != UBERBLOCK_MAGIC {
            return None;
        } 

        Some(Uberblock { 
            ub_magic,
            ub_version: data.read_u64_le()?, 
            ub_txg: data.read_u64_le()?, 
            ub_guid_sum: data.read_u64_le()?, 
            ub_timestamp: data.read_u64_le()?, 
            ub_rootbp: zio::BlockPointer::from_bytes_le(data)? 
        })
   }

   // Endianness invariant uberblock loading
   pub fn from_bytes(data: &mut (impl Iterator<Item = u8> + Clone)) -> Option<Uberblock> {
        let ub_magic_le = data.clone().read_u64_le()?;
        let ub_magic_be = data.clone().read_u64_be()?;

        if ub_magic_le == UBERBLOCK_MAGIC { // Little-endian
            Self::from_bytes_le(data)
        } else if ub_magic_be == UBERBLOCK_MAGIC { // Big-endian
            todo!("Implement big endian uberblock support!");
        } else { // Invalid magic
            return None;
        }
   }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} (device)", args[0]);
        return;
    }

    let Ok(vdev) = std::fs::OpenOptions::new().read(true).write(false).create(false).open(&args[1]) 
    else {
        println!("Failed to open vdev!");
        return;
    };

    let mut vdev: VdevFile = vdev.into();
    // For now just use the first label
    let mut label0 = VdevLabel::from_bytes(&vdev.read_raw_label0().expect("Vdev label 0 must exist!"));

    let name_value_pairs = nvlist::from_bytes_xdr(&mut label0.name_value_pairs_raw.iter().copied()).expect("Name value pairs in the vdev label must be valid!");
    let nvlist::Value::NVList(vdev_tree) = &name_value_pairs["vdev_tree"] else {
        panic!("vdev_tree is not an nvlist!");
    };

    let nvlist::Value::U64(top_level_ashift) = vdev_tree["ashift"] else {
        panic!("no ashift found for top level vdev!");
    };

    let nvlist::Value::U64(label_txg) = name_value_pairs["txg"] else {
        panic!("no txg found in label!");
    };


    label0.set_raw_uberblock_size(2_usize.pow(top_level_ashift as u32));
    let mut uberblocks = Vec::<Uberblock>::new();
    for i in 0..label0.get_raw_uberblock_count() {
        let raw_uberblock = label0.get_raw_uberblock(i);
        if let Some(uberblock) = Uberblock::from_bytes(&mut raw_uberblock.iter().copied()) {
            uberblocks.push(uberblock);
        }
    }
    
    // println!("Parsed nv_list: {:?}!", name_value_pairs);
    println!("Found {} uberblocks!", uberblocks.len());

    let uberblock_with_highest_txg = uberblocks.iter_mut().filter(|u|u.ub_txg >= label_txg).max_by(|a, b| a.ub_txg.cmp(&b.ub_txg)).expect("Active uberblock should exist!");

    println!("{:?}", uberblock_with_highest_txg);

    let mut vdevs = HashMap::<usize, &mut dyn Vdev>::new();
    vdevs.insert(0usize, &mut vdev);
    let data = uberblock_with_highest_txg.ub_rootbp.dereference(vdevs).unwrap();
    println!("{:x?}", data);
}
