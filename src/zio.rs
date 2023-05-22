use crate::{byte_iter::ByteIter, dmu, fletcher, lz4, lzjb, yolo_block_recovery, Vdev};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};

#[derive(Serialize, Deserialize)]
pub struct DataVirtualAddress {
    vdev_id: u32,
    data_allocated_size_minus_one_in_512b_sectors: u32, // technically a u24
    offset_in_512b_sectors: u64, // offset is after the labels and the boot block
    is_gang: bool,
}

impl Debug for DataVirtualAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<0x{:x}:0x{:x}:0x{:x}:{}>",
            self.vdev_id,
            self.parse_offset(),
            self.parse_allocated_size(),
            if self.is_gang { "is_gang" } else { "not_gang" }
        )
    }
}

impl DataVirtualAddress {
    pub const fn get_ondisk_size() -> usize {
        core::mem::size_of::<u64>() * 2
    }

    pub fn from(vdev_id: u32, offset_in_bytes: u64, is_gang: bool) -> DataVirtualAddress {
        DataVirtualAddress {
            vdev_id,
            data_allocated_size_minus_one_in_512b_sectors: 0, /* unused */
            offset_in_512b_sectors: offset_in_bytes / 512,
            is_gang,
        }
    }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<DataVirtualAddress> {
        let vdev_id = ((data.read_u32_le()?) & 0xFF_FF_FF_00) >> 8; // ignore padding ( https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L129 )
        let grid_and_asize = data.read_u32_le()?;
        let offset_and_gang_bit = data.read_u64_le()?;

        // A non-existent dva is marked by all zeroes
        if vdev_id == 0 && grid_and_asize == 0 && offset_and_gang_bit == 0 {
            return None;
        }
        Some(DataVirtualAddress {
            vdev_id,
            data_allocated_size_minus_one_in_512b_sectors: (grid_and_asize & 0xFF_FF_FF_00) >> 8, // ignore GRID as it is reserved
            offset_in_512b_sectors: offset_and_gang_bit & ((1 << 63) - 1), // bit 64 is the gang bit
            is_gang: offset_and_gang_bit & (1 << 63) != 0,
        })
    }

    // Returns: allocated size in bytes
    pub fn parse_allocated_size(&self) -> u64 {
        // All sizes are stored as the number of 512 byte sectors (minus one) needed to represent the size of this block. ( http://www.giis.co.in/Zfs_ondiskformat.pdf ( section 2.6 ) )
        (self.data_allocated_size_minus_one_in_512b_sectors as u64 + 1) * 512
    }

    // Returns: offset in bytes from beginning of vdev
    pub fn parse_offset(&self) -> u64 {
        self.offset_in_512b_sectors * 512
    }

    pub fn dereference(&self, vdevs: &mut Vdevs, size: usize) -> Result<Vec<u8>, ()> {
        if self.is_gang {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!(
                    "{MAGENTA}TODO{WHITE}: Implement GANG blocks, currentl just retuning error!"
                );
            }
            return Err(());
        }

        if cfg!(feature = "debug") {
            if self.vdev_id != 0 {
                use crate::ansi_color::*;
                println!(
                    "{YELLOW}Warning{WHITE}: DVA has invalid vdev id {}, automatically correcting!",
                    self.vdev_id
                );
            }
        }

        let Some(vdev) = vdevs.get_mut(&0) else { return Err(()); };

        if let Some(raidz_info) = vdev.get_raidz_info() {
            let number_of_data_sectors = if size % vdev.get_asize() == 0 {
                size / vdev.get_asize()
            } else {
                (size / vdev.get_asize()) + 1
            };
            let number_of_stripes =
                if number_of_data_sectors % (raidz_info.ndevices - raidz_info.nparity) == 0 {
                    number_of_data_sectors / (raidz_info.ndevices - raidz_info.nparity)
                } else {
                    number_of_data_sectors / (raidz_info.ndevices - raidz_info.nparity) + 1
                };
            let number_of_parity_sectors = number_of_stripes * raidz_info.nparity;

            let size_with_parity =
                (number_of_data_sectors + number_of_parity_sectors) * vdev.get_asize();

            // TODO: This shouldn't need to be here once we can properly bubble up the out of bounds error from lower layers
            // This is only here for the undelete program so that a dva interpreted from bad data won't be noisy
            if self.parse_offset() + (size_with_parity as u64) > vdev.get_size() {
                return Err(());
            }

            let res = vdev.read(self.parse_offset(), size_with_parity)?;

            // If we are doing raidz1, then the parity switches places with the first data column on odd megabyte offsets
            // I'm not kidding, THAT is how it actually works, that was a fun one to debug :)
            // Source: https://github.com/openzfs/zfs/blob/master/module/zfs/vdev_raidz.c#L398
            // Second source: https://github.com/openzfs/zfs/issues/12538#issuecomment-1251651412

            let mut column_mapping = (0..raidz_info.ndevices).collect::<Vec<usize>>();
            if raidz_info.nparity == 1 && (self.parse_offset() / (1 * 1024 * 1024)) % 2 != 0 {
                column_mapping.swap(0, 1);
            }

            // We have to transpose the data blocks because raidz stores data in column major order
            // Source: https://github.com/openzfs/zfs/blob/master/lib/libzfs/libzfs_dataset.c#L5357
            let mut res_transposed =
                Vec::<u8>::with_capacity(number_of_data_sectors * vdev.get_asize());
            // Note: Each disk is usually a single row (however this may not be true if raidz expansion took place, but thanks to the abstractions made by VdevRaidz this doesn't matter)
            // Source: https://youtu.be/Njt82e_3qVo?t=2810
            // TODO: Don't just skip the parity sectors
            for column_number in raidz_info.nparity..raidz_info.ndevices {
                let actual_column = column_mapping[column_number];
                for sector in res
                    .chunks(vdev.get_asize())
                    .skip(actual_column)
                    .step_by(raidz_info.ndevices)
                {
                    res_transposed.extend(sector);
                }
            }

            if res_transposed.len() > size {
                res_transposed.resize(size, 0);
            }

            assert!(res_transposed.len() == size);
            Ok(res_transposed)
        } else {
            // TODO: This shouldn't need to be here once we can properly bubble up the out of bounds error from lower layers
            // This is only here for the undelete program so that a dva interpreted from bad data won't be noisy
            if self.parse_offset() + (size as u64) > vdev.get_size() {
                return Err(());
            }

            vdev.read(self.parse_offset(), size)
        }
    }
}

pub type Vdevs<'a> = HashMap<usize, &'a mut dyn Vdev>;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum ChecksumMethod {
    Inherit = 0,
    On = 1, // equivalent to fletcher4 ( https://github.com/openzfs/zfs/blob/master/include/sys/zio.h#L122 )
    Off = 2,
    Label = 3,      // equivalent to sha-256
    GangHeader = 4, // equivalent to sha-256
    Zilog = 5,      // equivalent to fletcher2
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
            _ => return None,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
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
    Zstd = 16,
}

impl CompressionMethod {
    pub fn from_value(value: usize) -> Option<CompressionMethod> {
        Some(match value {
            0 => CompressionMethod::Inherit,
            1 => CompressionMethod::On,
            2 => CompressionMethod::Off,
            3 => CompressionMethod::Lzjb,
            4 => CompressionMethod::Empty,
            5 => CompressionMethod::Gzip1,
            6 => CompressionMethod::Gzip2,
            7 => CompressionMethod::Gzip3,
            8 => CompressionMethod::Gzip4,
            9 => CompressionMethod::Gzip5,
            10 => CompressionMethod::Gzip6,
            11 => CompressionMethod::Gzip7,
            12 => CompressionMethod::Gzip8,
            13 => CompressionMethod::Gzip9,
            14 => CompressionMethod::Zle,
            15 => CompressionMethod::Lz4,
            16 => CompressionMethod::Zstd,
            _ => return None,
        })
    }
}

// NOTE: output_size is currently only used for lzjb
// NOTE: It is up to the caller to ensure the decompressed data is
//       of size output_size and valid
pub fn try_decompress_block(
    block_data: &[u8],
    compression_method: CompressionMethod,
    output_size: usize,
) -> Result<Vec<u8>, Vec<u8>> {
    let data = match compression_method {
        CompressionMethod::Off => Vec::from(block_data),
        CompressionMethod::Lz4 | CompressionMethod::On => {
            if block_data.len() < 4 {
                // There has to be at least 4 bytes for the comp_size
                return Err(Vec::new());
            }

            let comp_size = u32::from_be_bytes(block_data[0..4].try_into().unwrap());

            // Note: comp_size+4 may be equal to block_data.len(), just not greater
            if comp_size as usize + 4 > block_data.len() {
                return Err(Vec::new());
            }

            // The data contains the size of the input as a big endian 32 bit int at the beginning before the lz4 stream starts
            lz4::lz4_decompress_blocks(&mut block_data[4..comp_size as usize + 4].iter().copied())?
        }

        CompressionMethod::Lzjb => {
            lzjb::lzjb_decompress(&mut block_data.iter().copied(), output_size)
                .map_err(|_| Vec::new())?
        }

        _ => {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!(
                    "{MAGENTA}TODO{WHITE}: {:?} compression is not implemented, returning error",
                    compression_method
                );
            }

            return Err(Vec::new());
        }
    };

    Ok(data)
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

#[derive(Serialize, Deserialize)]
pub struct NormalBlockPointer {
    dvas: [Option<DataVirtualAddress>; 3],
    level: usize,
    fill: u64,
    logical_birth_txg: u64,
    typ: dmu::ObjType,
    checksum_method: ChecksumMethod,
    compression_method: CompressionMethod,
    physical_size_in_512b_sectors_minus_one: u16,
    logical_size_in_512b_sectors_minus_one: u16,
    checksum: [u64; 4],
}

impl Debug for NormalBlockPointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NormalBlockPointer")
            .field("dvas", &self.dvas)
            .field("level", &self.level)
            .field("fill", &self.fill)
            .field("logical_birth_txg", &self.logical_birth_txg)
            .field("typ", &self.typ)
            .field("checksum_method", &self.checksum_method)
            .field("compression_method", &self.compression_method)
            .field("physical_size", &self.parse_physical_size())
            .field("logical_size", &self.parse_logical_size())
            .field("checksum", &self.checksum)
            .finish()
    }
}

impl NormalBlockPointer {
    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<NormalBlockPointer>
    where
        Iter: Iterator<Item = u8> + Clone,
    {
        let dva1 = DataVirtualAddress::from_bytes_le(&mut data.clone());
        data.skip_n_bytes(DataVirtualAddress::get_ondisk_size())?;
        let dva2 = DataVirtualAddress::from_bytes_le(&mut data.clone());
        data.skip_n_bytes(DataVirtualAddress::get_ondisk_size())?;
        let dva3 = DataVirtualAddress::from_bytes_le(&mut data.clone());
        data.skip_n_bytes(DataVirtualAddress::get_ondisk_size())?;
        let info = data.read_u64_le()?;

        // Make sure we don't accidentally read an embedded block pointer
        if (info >> 39) & 1 != 0 {
            // Check embedded bit
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Attempted to read embedded block pointer as normal block pointer!");
            return None; // This function only handles normal block pointers
        }

        // Check encrypted bit
        if (info >> 61) & 1 != 0 {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!("{YELLOW}Warning{WHITE}: Attempted to read encrypted block pointer as normal block pointer!");
            }
            return None;
        }

        // Check endianness bit just in case
        if (info >> 63) & 1 != 1 {
            return None;
        }

        // Skip padding
        data.skip_n_bytes(core::mem::size_of::<u64>() * 3)?;

        let logical_birth_txg = data.read_u64_le()?;
        let fill_count = data.read_u64_le()?;
        let checksum = [
            data.read_u64_le()?,
            data.read_u64_le()?,
            data.read_u64_le()?,
            data.read_u64_le()?,
        ];

        Some(NormalBlockPointer {
            dvas: [dva1, dva2, dva3],
            level: ((info >> 56) & 0b1_1111) as usize,
            fill: fill_count,
            logical_birth_txg,
            typ: dmu::ObjType::from_value(((info >> 48) & 0b1111_1111) as usize)?,
            checksum_method: ChecksumMethod::from_value(((info >> 40) & 0b1111_1111) as usize)?,
            compression_method: CompressionMethod::from_value(
                ((info >> 32) & 0b0111_1111) as usize,
            )?,
            physical_size_in_512b_sectors_minus_one: ((info >> 16) & 0b1111_1111_1111_1111) as u16,
            logical_size_in_512b_sectors_minus_one: ((info >> 0) & 0b1111_1111_1111_1111) as u16,
            checksum,
        })
    }

    // Returns: Logical size of the data pointed to by the block pointer, in bytes
    pub fn parse_logical_size(&self) -> u64 {
        // All sizes are stored as the number of 512 byte sectors (minus one) needed to represent the size of this block. ( http://www.giis.co.in/Zfs_ondiskformat.pdf ( section 2.6 ) )
        (self.logical_size_in_512b_sectors_minus_one as u64 + 1) * 512
    }

    // Returns: Physical size of the data pointed to by the block pointer, in bytes
    pub fn parse_physical_size(&self) -> u64 {
        // All sizes are stored as the number of 512 byte sectors (minus one) needed to represent the size of this block. ( http://www.giis.co.in/Zfs_ondiskformat.pdf ( section 2.6 ) )
        (self.physical_size_in_512b_sectors_minus_one as u64 + 1) * 512
    }

    // NOTE: zfs always checksums the data once put together, so the checksum is of the data pointed to by the gang blocks once stitched together, and it is done before decompression
    pub fn dereference(&mut self, vdevs: &mut Vdevs) -> Result<Vec<u8>, ()> {
        for dva in self.dvas.iter().filter_map(|val| val.as_ref()) {
            let Ok(data) = dva.dereference(vdevs, self.parse_physical_size() as usize) else {
                if cfg!(feature = "debug") {
                    use crate::ansi_color::*;
                    println!("{YELLOW}Warning{WHITE}: Invalid dva {:?}", dva);
                }
                continue;
            };

            let computed_checksum = match self.checksum_method {
                ChecksumMethod::Fletcher4 | ChecksumMethod::On => fletcher::do_fletcher4(&data),
                ChecksumMethod::Fletcher2 => fletcher::do_fletcher2(&data),
                _ => {
                    use crate::ansi_color::*;
                    if cfg!(feature = "debug") {
                        println!(
                            "{MAGENTA}TODO{WHITE}: {:?} checksum is not implemented, ignoring!",
                            self.checksum_method
                        )
                    }

                    continue;
                }
            };

            if computed_checksum != self.checksum {
                use crate::ansi_color::*;
                if cfg!(feature = "debug") {
                    println!("{YELLOW}Warning{WHITE}: Invalid checksum for dva: {:?}, ignoring this dva.", dva);
                }
                continue;
            }

            let Ok(data) = try_decompress_block(&data, self.compression_method, self.parse_logical_size() as usize) else {
                continue;
            };

            if data.len() != self.parse_logical_size() as usize {
                use crate::ansi_color::*;
                if cfg!(feature = "debug") {
                    println!("{YELLOW}Warning{WHITE}: Normal block pointer doesn't point to as much data as it says it should, i refuse to return it's data!");
                }

                return Err(());
            }

            // use crate::ansi_color::*;
            // println!("{CYAN}Info{WHITE}: Using dva: {:?}", dva);
            return Ok(data);
        }

        if cfg!(feature = "yolo") && self.checksum_method == ChecksumMethod::Fletcher4 {
            if let Some(res_off) = yolo_block_recovery::find_block_with_fletcher4_checksum(
                vdevs,
                &self.checksum,
                self.parse_physical_size() as usize,
            ) {
                let dva = DataVirtualAddress::from(0 /* just a guess */, res_off, false);
                if let Ok(Ok(data)) = dva
                    .dereference(vdevs, self.parse_physical_size() as usize)
                    .map(|data| {
                        try_decompress_block(
                            &data,
                            self.compression_method,
                            self.parse_logical_size() as usize,
                        )
                    })
                {
                    if data.len() != self.parse_logical_size() as usize {
                        use crate::ansi_color::*;
                        if cfg!(feature = "debug") {
                            println!("{YELLOW}Warning{WHITE}: Normal block pointer doesn't point to as much data as it says it should, i refuse to return it's data!");
                        }

                        return Err(());
                    }

                    return Ok(data);
                };
            }
        }

        if cfg!(feature = "debug") {
            use crate::ansi_color::*;
            println!(
                "{YELLOW}Warning{WHITE}: Failed to dereference block pointer: {:?}.",
                self
            );
        }

        Err(())
    }
}

// Reference: https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L265

#[derive(Serialize, Deserialize)]
pub struct EmbeddedBlockPointer {
    payload: Vec<u8>,
    logical_birth_txg: u64,
    level: usize,
    typ: dmu::ObjType,
    embedded_data_type: dmu::ObjType,
    compression_method: CompressionMethod,
    physical_size_in_bytes: u8,
    logical_size_in_bytes: u32, // only takes up 24 bits on disk
}

impl Debug for EmbeddedBlockPointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedBlockPointer")
            .field("payload", &self.payload)
            .field("logical_birth_txg", &self.logical_birth_txg)
            .field("level", &self.level)
            .field("typ", &self.typ)
            .field("embedded_data_type", &self.embedded_data_type)
            .field("compression_method", &self.compression_method)
            .field("physical_size", &self.parse_physical_size())
            .field("logical_size", &self.parse_logical_size())
            .finish()
    }
}

impl EmbeddedBlockPointer {
    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<EmbeddedBlockPointer> {
        let mut payload = Vec::<u8>::new();
        for _ in 0..6 * core::mem::size_of::<u64>() {
            payload.push(data.read_u8()?);
        }

        let info = data.read_u64_le()?;

        // Make sure we don't accidentally read an embedded block pointer
        if (info >> 39) & 1 != 1 {
            // Check embedded bit
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Attempted to read normal block pointer as embedded block pointer!");
            return None; // This function only handles normal block pointers
        }

        // Check encrypted bit
        if (info >> 61) & 1 != 0 {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!("{YELLOW}Warning{WHITE}: Attempted to read encrypted block pointer as embedded block pointer!");
            }
            return None;
        }

        // Check endianness bit just in case
        if (info >> 63) & 1 != 1 {
            return None;
        }

        for _ in 0..3 * core::mem::size_of::<u64>() {
            payload.push(data.read_u8()?);
        }

        let logical_birth_txg = data.read_u64_le()?;

        for _ in 0..5 * core::mem::size_of::<u64>() {
            payload.push(data.read_u8()?);
        }

        Some(EmbeddedBlockPointer {
            payload,
            logical_birth_txg,
            level: ((info >> 56) & 0b1_1111) as usize,
            typ: dmu::ObjType::from_value(((info >> 48) & 0b1111_1111) as usize)?,
            embedded_data_type: dmu::ObjType::from_value(((info >> 40) & 0b1111_1111) as usize)?,
            compression_method: CompressionMethod::from_value(
                ((info >> 32) & 0b0111_1111) as usize,
            )?,
            physical_size_in_bytes: ((info >> 24) & 0xFF) as u8,
            logical_size_in_bytes: ((info >> 0) & 0xFF_FF_FF) as u32,
        })
    }

    // Source: https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L333
    // And: https://github.com/openzfs/zfs/blob/master/include/sys/bitops.h#L66
    pub fn parse_logical_size(&self) -> u64 {
        u64::from(self.logical_size_in_bytes) + 1
    }

    // Source: https://github.com/openzfs/zfs/blob/master/include/sys/spa.h#L341
    // And: https://github.com/openzfs/zfs/blob/master/include/sys/bitops.h#L66
    pub fn parse_physical_size(&self) -> u64 {
        u64::from(self.physical_size_in_bytes) + 1
    }

    pub fn dereference(&mut self) -> Result<Vec<u8>, ()> {
        let mut data = self.payload.clone();

        if data.len() > self.parse_physical_size() as usize {
            data.resize(self.parse_physical_size() as usize, 0);
        }

        let Ok(data) = try_decompress_block(&data, self.compression_method, self.parse_logical_size() as usize) else {
            return Err(());
        };

        if data.len() != self.parse_logical_size() as usize {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!("{YELLOW}Warning{WHITE}: Embedded block pointer doesn't contain as much data as it says it should, i refuse to return it's data!");
            }

            return Err(());
        }

        Ok(data)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BlockPointer {
    Normal(NormalBlockPointer),
    Embedded(EmbeddedBlockPointer),
}

impl BlockPointer {
    pub const fn get_ondisk_size() -> usize {
        128
    }

    pub fn get_info_form_bytes_le(mut data: impl Iterator<Item = u8>) -> Option<u64> {
        data.skip_n_bytes(6 * core::mem::size_of::<u64>())?;
        data.read_u64_le()
    }

    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<BlockPointer>
    where
        Iter: Iterator<Item = u8> + Clone,
    {
        let info = Self::get_info_form_bytes_le(data.clone())?;
        let is_embedded = ((info >> 39) & 1) != 0;
        if is_embedded {
            Some(BlockPointer::Embedded(EmbeddedBlockPointer::from_bytes_le(
                data,
            )?))
        } else {
            Some(Self::Normal(NormalBlockPointer::from_bytes_le(data)?))
        }
    }

    // Returns: Logical size of the data pointed to by the block pointer, in bytes
    pub fn parse_logical_size(&self) -> u64 {
        match self {
            BlockPointer::Normal(block_pointer) => block_pointer.parse_logical_size(),
            BlockPointer::Embedded(block_pointer) => block_pointer.parse_logical_size(),
        }
    }

    // Returns: Physical size of the data pointed to by the block pointer, in bytes
    pub fn parse_physical_size(&self) -> u64 {
        match self {
            BlockPointer::Normal(block_pointer) => block_pointer.parse_physical_size(),
            BlockPointer::Embedded(block_pointer) => block_pointer.parse_physical_size(),
        }
    }

    pub fn dereference(&mut self, vdevs: &mut Vdevs) -> Result<Vec<u8>, ()> {
        match self {
            BlockPointer::Normal(block_poiner) => block_poiner.dereference(vdevs),
            BlockPointer::Embedded(block_pointer) => block_pointer.dereference(),
        }
    }
}
