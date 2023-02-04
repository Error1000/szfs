use std::{fs::File, io::{Read, Write, Seek, SeekFrom}, os::unix::prelude::MetadataExt};

use byte_iter::ByteIter;

mod nvlist;
mod byte_iter;


trait VdevOperations {
    fn get_size(&self) -> u64;
}

impl VdevOperations for File {
    fn get_size(&self) -> u64 {
        self.metadata().expect(&format!("File {:?} should have metadata, otherwise we can't get it's size!", self)).size()
    }
}

#[derive(Debug)]
struct Vdev<DeviceType> 
where DeviceType: Read + Write + Seek + VdevOperations {
    device: DeviceType,
}

impl<DeviceType> Vdev<DeviceType> 
where DeviceType: Read + Write + Seek + VdevOperations {
    pub fn read(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        let mut buf = vec![0u8; amount_in_bytes];
        self.device.seek(SeekFrom::Start(offset_in_bytes)).map_err(|_| ())?;
        self.device.read(&mut buf).map_err(|_| ())?;
        Ok(buf)
    }

    pub fn write(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        self.device.seek(SeekFrom::Start(offset_in_bytes)).map_err(|_| ())?;
        self.device.write(data).map_err(|_| ())?;
        Ok(())
    }

    pub fn get_size(&self) -> u64 {
        self.device.get_size()
    }

    // Source: http://www.giis.co.in/Zfs_ondiskformat.pdf
    // Section 1.2.1

    pub fn read_raw_label0(&mut self) -> Result<Vec<u8>, ()>{
        self.read(0, 256*1024)
    }

    pub fn read_raw_label1(&mut self) -> Result<Vec<u8>, ()>{
        self.read(256*1024, 256*1024)
    }

    pub fn read_raw_label2(&mut self) -> Result<Vec<u8>, ()>{
        self.read(self.get_size()-512*1024, 256*1024)
    }

    pub fn read_raw_label3(&mut self) -> Result<Vec<u8>, ()>{
        self.read(self.get_size()-256*1024, 256*1024)
    }
}

impl From<File> for Vdev<File> {
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
struct DataVirtualAddress {
    vdev_id: u32,
    data_allocated_size: u32, // technically a u24
    offset_in_sectors: u64, // 1 sector = 512 bytes, offset is after the labels and the boot block
    is_gang: bool
}

impl DataVirtualAddress {
   pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<DataVirtualAddress> {
    let vdev_id = (data.read_u32_le()?) & 0xFF_FF_FF_00; // ignore padding ( https://github.com/ahrens/zfsondisk/blob/master/docs/zfs_internals.md.pdf (chapter 1) )
    let grid_and_asize = data.read_u32_le()?;
    let offset_and_gang_bit = data.read_u64_le()?;
    Some(DataVirtualAddress { 
        vdev_id, 
        data_allocated_size: grid_and_asize&0x00_FF_FF_FF, // ignore GRID as it is reserved 
        offset_in_sectors: offset_and_gang_bit&(!(1<<63)), // bit 64 is the gang bit 
        is_gang: offset_and_gang_bit&(1<<63) != 0
    })
   }
}

#[derive(Debug)]
enum ChecksumMethod {
    Inherit = 0,
    On = 1, // equivalent to fletcher4 ( https://github.com/openzfs/zfs/blob/master/include/sys/zio.h#L122 )
    Off = 2,
    Label = 3, // equivalent to sha-256
    GangHeader = 4, // equivalent to sha-256
    Zilog = 5, // equivalent to fletcher2
    Fletcher2 = 6,
    Fletcher4 = 7,
    Sha256 = 8,
    Zilog2 = 9,
    NoParity = 10,
    Sha512 = 11,
    Skein = 12,
    Edonr = 13,
    Blake3 = 14,
}

impl ChecksumMethod {
    pub fn from_value(value: usize) -> Option<ChecksumMethod> {
        Some(match value {
            0 => ChecksumMethod::Inherit,
            1 => ChecksumMethod::On, 
            2 => ChecksumMethod::Off,
            3 => ChecksumMethod::Label,
            4 => ChecksumMethod::GangHeader,
            5 => ChecksumMethod::Zilog,
            6 => ChecksumMethod::Fletcher2,
            7 => ChecksumMethod::Fletcher4,
            8 => ChecksumMethod::Sha256,
            9 => ChecksumMethod::Zilog2,
            10 => ChecksumMethod::NoParity,
            11 => ChecksumMethod::Sha512,
            12 => ChecksumMethod::Skein,
            13 => ChecksumMethod::Edonr,
            14 => ChecksumMethod::Blake3,
            _ => return None
        })
    }
}

#[derive(Debug)]
enum CompressionMethod {
    Inherit = 0,
    On = 1, // Equivalent to lz4
    Off = 2,
    Lzjb = 3,
    Empty = 4,
    Gzip1 = 5,
    Gzip2 = 6,
    Gzip3 = 7,
    Gzip4 = 8,
    Gzip5 = 9,
    Gzip6 = 10,
    Gzip7 = 11,
    Gzip8 = 12,
    Gzip9 = 13,
    Zle = 14,
    Lz4 = 15,
    Zstd = 16
}

impl CompressionMethod {
    pub fn from_value(value: usize) -> Option<CompressionMethod> {
        Some(match value {
            0  => CompressionMethod::Inherit,
            1  => CompressionMethod::On, 
            2  => CompressionMethod::Off,
            3  => CompressionMethod::Lzjb,
            4  => CompressionMethod::Empty,
            5  => CompressionMethod::Gzip1,
            6  => CompressionMethod::Gzip2,
            7  => CompressionMethod::Gzip3,
            8  => CompressionMethod::Gzip4,
            9  => CompressionMethod::Gzip5,
            10 => CompressionMethod::Gzip6,
            11 => CompressionMethod::Gzip7,
            12 => CompressionMethod::Gzip8,
            13 => CompressionMethod::Gzip8,
            14 => CompressionMethod::Zle,
            15 => CompressionMethod::Lz4,
            16 => CompressionMethod::Zstd,
            _ => return None
        })
    }
}

#[derive(Debug)]
enum DMUBlockType {
    None = 0,
    ObjectDirectory = 1,
    ObjectArray = 2,
    PackedNVList = 3,
    NVListSize = 4,
    BlockPointerList = 5,
    BlockPointerListHeader = 6,
    SpaceMapHeader = 7,
    SpaceMap = 8,
    IntentLog = 9,
    DNode = 10,
    ObjSet = 11,
    DSLDataset = 12,
    DSLDatasetChildMap = 13,
    ObjSetSnapshotMap = 14,
    DSLProperties = 15,
    DSLObjSet = 16,
    ZNode = 17,
    AcessControlList = 18,
    PlainFileContents = 19,
    DirectoryContents = 20,
    MasterNode = 21,
    DeleteQueue = 22,
    ZVol = 23,
    ZVolProperties = 24
}

impl DMUBlockType {
    pub fn from_value(value: usize) -> Option<DMUBlockType> {
        Some(match value {
            0  => DMUBlockType::None,
            1  => DMUBlockType::ObjectDirectory, 
            2  => DMUBlockType::ObjectArray,
            3  => DMUBlockType::PackedNVList,
            4  => DMUBlockType::NVListSize,
            5  => DMUBlockType::BlockPointerList,
            6  => DMUBlockType::BlockPointerListHeader,
            7  => DMUBlockType::SpaceMapHeader,
            8  => DMUBlockType::SpaceMap,
            9  => DMUBlockType::IntentLog,
            10 => DMUBlockType::DNode,
            11 => DMUBlockType::ObjSet,
            12 => DMUBlockType::DSLDataset,
            13 => DMUBlockType::DSLDatasetChildMap,
            14 => DMUBlockType::ObjSetSnapshotMap,
            15 => DMUBlockType::DSLProperties,
            16 => DMUBlockType::DSLObjSet,
            17 => DMUBlockType::ZNode,
            18 => DMUBlockType::AcessControlList,
            19 => DMUBlockType::PlainFileContents,
            20 => DMUBlockType::DirectoryContents,
            21 => DMUBlockType::MasterNode,
            22 => DMUBlockType::DeleteQueue,
            23 => DMUBlockType::ZVol,
            24 => DMUBlockType::ZVolProperties,
            _ => return None
        })
    }
}

// Byte order (https://github.com/openzfs/zfs/blob/master/include/sys/spa.h)
// 0 = big endian
// 1 = little endian
// Note: This is mostly useless as it is a part of a 64-bit integer which is stored in *native* endianess ( i think ) in the block pointer
// The correct way to do this is probably to check the uberblock magic, but if an uberblock with a txg of n can ever lead to blocks with txg < n this would probably not work either
// That or i'm stupid, idk

// Embedded block pointer info
// BDX LVL   TYP      ETYP     E COMP    PSIZE   LSIZE 
// 100 00000 00001011 00000111 1 0001111 0000000 0000000000000000000000111
// 3   5     8        8        1 7       7       25

// Normal block pointer info
// BDX LVL   TYP      CKSUM    E COMP    PSIZE            LSIZE
// 100 00000 00001011 00000111 0 0001111 0000000000000000 0000000000000111
// 3   5     8        8        1 7       16	              16

#[derive(Debug)]
struct BlockPointer {
    dvas: [DataVirtualAddress; 3],
    level: usize,
    typ: DMUBlockType,
    checksum_method: ChecksumMethod,
    compression_method: CompressionMethod,
    physical_size: u16,
    logical_size: u16,
    checksum: [u64; 4]
}

impl BlockPointer {
    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<BlockPointer> {
        let dva1 = DataVirtualAddress::from_bytes_le(data)?;
        let dva2 = DataVirtualAddress::from_bytes_le(data)?;
        let dva3 = DataVirtualAddress::from_bytes_le(data)?;
        let info = data.read_u64_le()?;

        // Make sure we don't accidentally read an embedded block pointer
        if (info>>39)&1 != 0 { // Check embedded bit
            println!("Tried to load an embeded block pointer as if it were a normal block pointer, this shouldn't happen if the program is well designed as it should check before trying to load itself!");
            return None;
        }

        // Skip padding
        data.nth((core::mem::size_of::<u64>()*3) - 1);
        let birth_txg = data.read_u64_le()?;
        let fill_count = data.read_u64_le()?;
        let checksum = [data.read_u64_le()?, data.read_u64_le()?, data.read_u64_le()?, data.read_u64_le()?];

        Some(BlockPointer { 
            dvas: [dva1, dva2, dva3], 
            level: ((info >> 56)&0b1_1111) as usize, 
            typ: DMUBlockType::from_value(((info >> 48)&0b1111_1111) as usize)?, 
            checksum_method: ChecksumMethod::from_value(((info >> 40)&0b1111_1111) as usize)?, 
            compression_method: CompressionMethod::from_value(((info >> 32)&0b111_1111) as usize)?, 
            physical_size: ((info >> 16) & 0b1111_1111_1111_1111) as u16, 
            logical_size: ((info >> 0) & 0b1111_1111_1111_1111) as u16, 
            checksum
        })
    }


}

#[derive(Debug)]
struct Uberblock {
    ub_magic: u64,
    ub_version: u64,
    ub_txg: u64,
    ub_guid_sum: u64,
    ub_timestamp: u64,
    ub_rootbp: BlockPointer
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
            ub_rootbp: BlockPointer::from_bytes_le(data)? 
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

    let mut vdev: Vdev<_> = vdev.into();
    // For now just use the first label
    let mut label0 = VdevLabel::from_bytes(&vdev.read_raw_label0().expect("Vdev label 0 must exist!"));

    let name_value_pairs = nvlist::from_bytes_xdr(&mut label0.name_value_pairs_raw.iter().copied()).expect("Name value pairs in the vdev label must be valid!");
    let nvlist::Value::NVList(vdev_tree) = &name_value_pairs["vdev_tree"] else {
        panic!("vdev_tree is not an nvlist!");
    };

    let nvlist::Value::U64(top_level_ashift) = vdev_tree["ashift"] else {
        panic!("no ashift found for top level vdev!");
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
    for u in uberblocks { println!("Uberblock: {:?}", u); }
}
