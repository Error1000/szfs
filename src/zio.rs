use std::{fmt::Debug, collections::HashMap};
use crate::{byte_iter::ByteIter, Vdev, fletcher, lz4, dmu};


struct DataVirtualAddress {
    vdev_id: u32,
    data_allocated_size_minus_one_in_sectors: u32, // technically a u24
    offset_in_sectors: u64, // 1 sector = 512 bytes, offset is after the labels and the boot block
    is_gang: bool
}

impl Debug for DataVirtualAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<0x{:x}:0x{:x}:0x{:x}:{}>", self.vdev_id, self.parse_offset(), self.parse_allocated_size(), if self.is_gang { "is_gang" } else {"not_gang"})
    }
}


impl DataVirtualAddress {
   pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<DataVirtualAddress> {
    let vdev_id = ((data.read_u32_le()?) & 0xFF_FF_FF_00) >> 8; // ignore padding ( https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L129 )
    let grid_and_asize = data.read_u32_le()?;
    let offset_and_gang_bit = data.read_u64_le()?;
    Some(DataVirtualAddress { 
        vdev_id, 
        data_allocated_size_minus_one_in_sectors: (grid_and_asize&0xFF_FF_FF_00) >> 8, // ignore GRID as it is reserved 
        offset_in_sectors: offset_and_gang_bit&(!(1<<63)), // bit 64 is the gang bit 
        is_gang: offset_and_gang_bit&(1<<63) != 0
    })
   }

   // Returns: allocated size in bytes
   pub fn parse_allocated_size(&self) -> u64 {
        // All sizes are stored as the number of 512 byte sectors (minus one) needed to represent the size of this block. ( http://www.giis.co.in/Zfs_ondiskformat.pdf ( section 2.6 ) )
        (self.data_allocated_size_minus_one_in_sectors as u64 + 1)*512
   }

   // Returns: offset in bytes from beginning of vdev
   pub fn parse_offset(&self) -> u64 {
     self.offset_in_sectors*512
   }

   pub fn dereference(&self, vdevs: &mut HashMap<usize, &mut dyn Vdev>, size: usize) -> Result<Vec<u8>, ()> {
        if self.is_gang { todo!("Implement GANG blocks!"); }
        let Some(vdev) = vdevs.get_mut(&self.vdev_id.try_into().expect("overflow should be impossible")) else { return Err(()); };
        vdev.read(self.parse_offset()+4*1024*1024, size) 
   }
}

pub type Vdevs<'a> = HashMap<usize, &'a mut dyn Vdev>;

#[derive(Debug, PartialEq, Eq)]
pub enum ChecksumMethod {
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

#[derive(Debug, PartialEq, Eq)]
pub enum CompressionMethod {
    Inherit = 0,
    On = 1, // Equivalent to lz4 (https://github.com/openzfs/zfs/blob/master/include/sys/zio.h#L122)
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
            13 => CompressionMethod::Gzip9,
            14 => CompressionMethod::Zle,
            15 => CompressionMethod::Lz4,
            16 => CompressionMethod::Zstd,
            _ => return None
        })
    }
}



// Byte order (https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L591)
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

pub struct BlockPointer {
    dvas: [DataVirtualAddress; 3],
    level: usize,
    fill: u64,
    logical_birth_txg: u64,
    typ: dmu::Type,
    checksum_method: ChecksumMethod,
    compression_method: CompressionMethod,
    physical_size_in_sectors_minus_one: u16,
    logical_size_in_sectors_minus_one: u16,
    checksum: [u64; 4]
}

impl Debug for BlockPointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockPointer").field("dvas", &self.dvas).field("level", &self.level).field("fill", &self.fill).field("logical_birth_txg", &self.logical_birth_txg).field("typ", &self.typ).field("checksum_method", &self.checksum_method).field("compression_method", &self.compression_method).field("physical_size", &self.parse_physical_size()).field("logical_size", &self.parse_logical_size()).field("checksum", &self.checksum).finish()
    }
}

impl BlockPointer {
    pub fn get_ondisk_size() -> usize { 128 }
    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<BlockPointer> {
        let dva1 = DataVirtualAddress::from_bytes_le(data)?;
        let dva2 = DataVirtualAddress::from_bytes_le(data)?;
        let dva3 = DataVirtualAddress::from_bytes_le(data)?;
        let info = data.read_u64_le()?;

        // Make sure we don't accidentally read an embedded block pointer
        if (info>>39)&1 != 0 { // Check embedded bit
            todo!("Handle embedded block pointers!");
        }
        
        // Check endianness bit just in case
        if (info>>63)&1 != 1 {
            return None;
        }

        // Skip padding
        data.skip_n_bytes((core::mem::size_of::<u64>()*3));
        let logical_birth_txg = data.read_u64_le()?;
        let fill_count = data.read_u64_le()?;
        let checksum = [data.read_u64_le()?, data.read_u64_le()?, data.read_u64_le()?, data.read_u64_le()?];

        Some(BlockPointer { 
            dvas: [dva1, dva2, dva3], 
            level: ((info >> 56)&0b1_1111) as usize, 
            fill: fill_count,
            logical_birth_txg,
            typ: dmu::Type::from_value(((info >> 48)&0b1111_1111) as usize)?, 
            checksum_method: ChecksumMethod::from_value(((info >> 40)&0b1111_1111) as usize)?, 
            compression_method: CompressionMethod::from_value(((info >> 32)&0b111_1111) as usize)?, 
            physical_size_in_sectors_minus_one: ((info >> 16) & 0b1111_1111_1111_1111) as u16, 
            logical_size_in_sectors_minus_one: ((info >> 0) & 0b1111_1111_1111_1111) as u16, 
            checksum
        })
    }


    // Returns: Logical size in bytes
    pub fn parse_logical_size(&self) -> u64 {
        // All sizes are stored as the number of 512 byte sectors (minus one) needed to represent the size of this block. ( http://www.giis.co.in/Zfs_ondiskformat.pdf ( section 2.6 ) )
        (self.logical_size_in_sectors_minus_one as u64+1)*512
    }

    // Returns: Physical size in bytes
    pub fn parse_physical_size(&self) -> u64 {
        // All sizes are stored as the number of 512 byte sectors (minus one) needed to represent the size of this block. ( http://www.giis.co.in/Zfs_ondiskformat.pdf ( section 2.6 ) )
        (self.physical_size_in_sectors_minus_one as u64+1)*512
    }

    pub fn get_checksum(&self) -> &[u64; 4] {
        &self.checksum
    }

    // NOTE: zfs always checksums the data once put together, so the checksum is of the data of the gang blocks once stitched together, and it is done before decompression
    pub fn dereference(&mut self, vdevs: &mut Vdevs) -> Result<Vec<u8>, ()> {
        for dva in &self.dvas {
            let Ok(mut data) = dva.dereference(vdevs, self.parse_physical_size().try_into().unwrap()) else { continue; };
            
            // Truncate data to it's physical length, as the dva will read the entire allocated length which might be bigger
            data.resize(self.parse_physical_size().try_into().expect("overflow should be impossible"), 0);

            let computed_checksum = match self.checksum_method {
                ChecksumMethod::Fletcher4 | ChecksumMethod::On => fletcher::do_fletcher4(&data),
                ChecksumMethod::Fletcher2 => fletcher::do_fletcher2(&data),
                _ => todo!("Implement {:?} checksum!", self.checksum_method),
            };

            if &computed_checksum != self.get_checksum() {
                println!("Invalid checksum for dva: {:?}, the checksum should be: {:#x?}, ignoring.", dva, self.checksum);
                continue;
            }

            let data = match self.compression_method {
                CompressionMethod::Off => data,
                CompressionMethod::Lz4 | CompressionMethod::On => {
                    let comp_size = u32::from_be_bytes(data[0..4].try_into().unwrap());
                    // The data contains the size of the input as a big endian 32 bit int at the beginning before the lz4 stream starts
                    lz4::lz4_decompress_blocks(&mut data[4..comp_size as usize+4].iter().copied()).map_err(|_| ())?
                }
                _ => todo!("Implement {:?} compression!", self.compression_method),
            };
            assert!(data.len() == self.parse_logical_size() as usize);
            println!("Using dva: {:?}", dva);
            return Ok(data);
        }

        return Err(());
    }
}
