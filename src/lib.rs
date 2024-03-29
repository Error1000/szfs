#![allow(dead_code)]
#![allow(
    clippy::identity_op,
    clippy::result_unit_err,
    clippy::collapsible_if,
    clippy::needless_range_loop,
    clippy::needless_return,
    clippy::unusual_byte_groupings
)]

use std::{
    fmt::Debug,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    time,
};

use byte_iter::{FromBytes, FromBytesLE};
use lru::LruCache;
use zio::Vdevs;

pub mod byte_iter;
pub mod dmu;
pub mod dsl;
pub mod fletcher;
pub mod lz4;
pub mod lzjb;
pub mod nvlist;
pub mod yolo_block_recovery;
pub mod zap;
pub mod zil;
pub mod zio;
pub mod zpl;

pub mod ansi_color {
    pub const RED: &str = "\u{001b}[31m";
    pub const YELLOW: &str = "\u{001b}[33m";
    pub const CYAN: &str = "\u{001b}[36m";
    pub const WHITE: &str = "\u{001b}[0m";
    pub const MAGENTA: &str = "\u{001b}[35m";
}

// TODO:
// 1. Implement spill blocks
// 2. Implement non-embedded fat zap tables
// 3. Implement all nvlist values
// 4. Implement all fat zap values
// 5. Implement all system attributes
// 6. Don't just skip the parity sectors in RAIDZ
// 7. Test RAIDZ writing, and in general implement writing
// 8. Figure out why dvas at the end of a plain file contents indirect block tree have vdev id 1
// 9. Make sure usage of "as" is correct ( probably should use .try_into()? or something similar in some places )

pub struct RaidzInfo {
    ndevices: usize,
    nparity: usize,
}

pub trait Vdev: Send {
    // NOTE: If a vdev type doesn't have a cache it can just return None when getting and do nothing when putting
    // Return type is Option<Option> so we can cache a block that is unreadable
    // So there are 3 possible return values None - means not in cache, Some(None) - means in cache but all of the checksums failed so the block is unreadable, Some(Some) - in cache and has data
    fn get_from_block_cache(
        &mut self,
        key: &([u64; 4], zio::ChecksumMethod),
    ) -> Option<Option<&[u8]>>;

    fn put_in_block_cache(&mut self, key: ([u64; 4], zio::ChecksumMethod), value: Option<Vec<u8>>);

    fn get_size(&self) -> u64;
    // NOTE: Read and write ignore the labels and the boot block
    // A.k.a for a normal vdev the offset is relative to the end of the boot block instead
    // of the beginning of the vdev
    fn read(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()>;

    fn write(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()>;

    fn read_raw_label(&mut self, label_index: usize) -> Result<Vec<u8>, ()>;
    fn get_nlables(&mut self) -> usize;
    fn get_asize(&self) -> usize;
    fn get_raidz_info(&self) -> Option<RaidzInfo>;
}

#[derive(Debug)]
pub struct VdevFile {
    device: File,
    file_size: u64,
}

impl From<File> for VdevFile {
    fn from(mut f: File) -> Self {
        let file_size = f.seek(SeekFrom::End(0)).unwrap();
        Self {
            device: f,
            file_size,
        }
    }
}

impl VdevFile {
    fn read_raw(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        let mut buf = vec![0u8; amount_in_bytes];
        self.device
            .seek(SeekFrom::Start(offset_in_bytes))
            .map_err(|_| {
                if cfg!(feature = "debug") {
                    use crate::ansi_color::*;
                    println!("{YELLOW}Warning{WHITE}: The read at offset {:?} for device {:?} failed to seek!", offset_in_bytes, self);
                }
            })?;

        if self.device.read(&mut buf).map_err(|_| ())? != amount_in_bytes {
            if cfg!(feature = "debug") {
                use crate::ansi_color::*;
                println!(
                    "{YELLOW}Warning{WHITE}: The read at {:?} for device {:?} failed!",
                    offset_in_bytes, self
                );
            }

            return Err(());
        }

        Ok(buf)
    }

    fn write_raw(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        self.device
            .seek(SeekFrom::Start(offset_in_bytes))
            .map_err(|_| {
                if cfg!(feature = "debug") {
                    use crate::ansi_color::*;
                    println!("{YELLOW}Warning{WHITE}: The write at offset {:?} for device {:?} failed to seek!", offset_in_bytes, self);
                }
            })?;

        if self.device.write(data).map_err(|_| ())? != data.len() {
            if cfg!(feature = "debug") {
                use crate::ansi_color::*;
                println!(
                    "{YELLOW}Warning{WHITE}: The write at {:?} for device {:?} failed!",
                    offset_in_bytes, self
                );
            }

            return Err(());
        }

        Ok(())
    }

    fn get_raw_size(&self) -> u64 {
        self.file_size
    }
}

impl Vdev for VdevFile {
    fn get_from_block_cache(
        &mut self,
        _key: &([u64; 4], zio::ChecksumMethod),
    ) -> Option<Option<&[u8]>> {
        None
    }

    fn put_in_block_cache(
        &mut self,
        _key: ([u64; 4], zio::ChecksumMethod),
        _value: Option<Vec<u8>>,
    ) {
    }

    fn get_raidz_info(&self) -> Option<RaidzInfo> {
        None
    }

    fn get_asize(&self) -> usize {
        unimplemented!()
    }

    fn read(&mut self, mut offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        offset_in_bytes += 4 * 1024 * 1024;

        // 4 mb at the beginning and 2 labels at the end
        if offset_in_bytes + amount_in_bytes as u64
            > self.get_raw_size() - /* ending lables */ 2 * 256 * 1024
        {
            use ansi_color::*;
            println!(
                "{YELLOW}Warning{WHITE}: Trying to read {:?} bytes from offset: {:?} would go outside the device {:?}!",
                amount_in_bytes,
                offset_in_bytes,
                self
            );

            return Err(());
        }

        self.read_raw(offset_in_bytes, amount_in_bytes)
    }

    fn write(&mut self, mut offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        offset_in_bytes += 4 * 1024 * 1024;

        // 4 mb at the beginning and 2 labels at the end
        if offset_in_bytes + data.len() as u64
            > self.get_raw_size() - /* ending lables */ 2*256*1024
        {
            use ansi_color::*;
            println!(
                "{YELLOW}Warning{WHITE}: Offset: {:?} is past the end of device {:?}!",
                offset_in_bytes, self
            );
            return Err(());
        }
        self.write_raw(offset_in_bytes, data)
    }

    fn get_size(&self) -> u64 {
        self.get_raw_size()
        -4*1024*1024 /* beginning boot block and labels */
        -2*256*1024 /* ending labels */
    }

    // Source: http://www.giis.co.in/Zfs_ondiskformat.pdf
    // Section 1.2.1

    fn read_raw_label(&mut self, label_index: usize) -> Result<Vec<u8>, ()> {
        match label_index {
            0 => self.read_raw(0, 256 * 1024),
            1 => self.read_raw(256 * 1024, 256 * 1024),
            2 => self.read_raw(self.get_raw_size() - 2 * 256 * 1024, 256 * 1024),
            3 => self.read_raw(self.get_raw_size() - 1 * 256 * 1024, 256 * 1024),
            _ => Err(()),
        }
    }

    fn get_nlables(&mut self) -> usize {
        4
    }
}

pub struct VdevRaidz<'a> {
    devices: Vdevs<'a>,
    size: u64,
    ndevices: usize,
    nparity: usize,
    asize: usize,
    // This is based on a profiler showing that we hit read_sector heavily and since disk access is slow
    // and because we tend to access the same sectors multiple times (cache hit rate is ~97% as measured in runtime) in a non-sequential order,
    sector_cache: LruCache<u64, Vec<u8>>,
    sector_cache_hits: u64,
    sector_cache_misses: u64,
    block_cache: LruCache<([u64; 4], zio::ChecksumMethod), Option<Vec<u8>>>,
    block_cache_hits: u64,
    block_cache_misses: u64,
    last_debug: time::SystemTime,
}

impl<'a> VdevRaidz<'a> {
    pub fn from_vdevs(
        devices: Vdevs<'a>,
        ndevices: usize,
        nparity: usize,
        asize: usize,
    ) -> VdevRaidz {
        let device_size = devices.iter().map(|dev| dev.1.get_size()).min().unwrap();
        let size = device_size * (ndevices as u64);
        VdevRaidz {
            devices,
            size,
            ndevices,
            nparity,
            asize,
            // NOTE: A sector is usually 4k or 512b
            sector_cache: LruCache::new(64_000.try_into().unwrap()),
            sector_cache_hits: 0,
            sector_cache_misses: 0,
            // NOTE: A block is usually ~128kb
            block_cache: LruCache::new(32_000.try_into().unwrap()),
            block_cache_hits: 0,
            block_cache_misses: 0,
            last_debug: time::SystemTime::now(),
        }
    }

    pub fn read_sector(&mut self, sector_index: u64) -> Result<Vec<u8>, ()> {
        if let Some(res) = self.sector_cache.get_mut(&sector_index).cloned() {
            if cfg!(feature = "debug") {
                self.sector_cache_hits += 1;
                if time::SystemTime::now()
                    .duration_since(self.last_debug)
                    .unwrap()
                    .as_secs_f32()
                    > 10.0
                {
                    println!(
                        "Info: Raidz sector cache hit rate is {}%!",
                        ((self.sector_cache_hits as f64)
                            / (self.sector_cache_hits as f64 + self.sector_cache_misses as f64))
                            * 100.0
                    );

                    self.last_debug = time::SystemTime::now();
                }
            }
            return Ok(res);
        }

        if cfg!(feature = "debug") {
            self.sector_cache_misses += 1;
        }

        let device_sector_index = sector_index / (self.ndevices as u64);
        let device_number = (sector_index % (self.ndevices as u64)) as usize;
        let asize = self.get_asize();
        let res = self
            .devices
            .get_mut(&device_number)
            .ok_or(())?
            .read(device_sector_index * (asize as u64), asize)?;
        self.sector_cache.put(sector_index, res.clone());
        Ok(res)
    }

    pub fn write_sector(&mut self, sector_index: u64, data: &[u8]) -> Result<(), ()> {
        let device_sector_index = sector_index / (self.ndevices as u64);
        let device_number = (sector_index % (self.ndevices as u64)) as usize;
        let asize = self.get_asize();
        assert!(data.len() == asize);

        self.devices
            .get_mut(&device_number)
            .ok_or(())?
            .write(device_sector_index * (asize as u64), data)?;
        self.sector_cache.put(sector_index, Vec::from(data));
        Ok(())
    }
}

impl Vdev for VdevRaidz<'_> {
    fn get_from_block_cache(
        &mut self,
        key: &([u64; 4], zio::ChecksumMethod),
    ) -> Option<Option<&[u8]>> {
        let res = self.block_cache.get(key);
        if cfg!(feature = "debug") {
            if res.is_some() {
                self.block_cache_hits += 1;
            } else {
                self.block_cache_misses += 1;
            }

            if time::SystemTime::now()
                .duration_since(self.last_debug)
                .unwrap()
                .as_secs_f32()
                > 10.0
            {
                println!(
                    "Info: Raidz block cache hit rate is {}%!",
                    ((self.block_cache_hits as f64)
                        / (self.block_cache_hits as f64 + self.block_cache_misses as f64))
                        * 100.0
                );

                self.last_debug = time::SystemTime::now();
            }
        }

        res.map(|lookup| lookup.as_ref().map(|vec| vec.as_slice()))
    }

    fn put_in_block_cache(&mut self, key: ([u64; 4], zio::ChecksumMethod), value: Option<Vec<u8>>) {
        self.block_cache.put(key, value);
    }

    fn get_raidz_info(&self) -> Option<RaidzInfo> {
        Some(RaidzInfo {
            ndevices: self.ndevices,
            nparity: self.nparity,
        })
    }

    fn get_size(&self) -> u64 {
        self.size
    }

    fn get_asize(&self) -> usize {
        self.asize
    }

    // Note: Reading 0 bytes will *always* succeed
    fn read(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        if amount_in_bytes == 0 {
            return Ok(Vec::new());
        }

        let mut result: Vec<u8> = Vec::with_capacity(amount_in_bytes + self.get_asize() * 2);
        let first_sector_index = offset_in_bytes / (self.get_asize() as u64);
        let first_sector_offset = offset_in_bytes % (self.get_asize() as u64);
        let first_sector = self.read_sector(first_sector_index)?;
        result.extend(first_sector.iter().skip(first_sector_offset as usize));

        if result.len() >= amount_in_bytes {
            result.resize(amount_in_bytes, 0);
            return Ok(result);
        }

        let size_remaining = amount_in_bytes - result.len();
        let sectors_to_read = if size_remaining % self.get_asize() == 0 {
            size_remaining / self.get_asize()
        } else {
            (size_remaining / self.get_asize()) + 1
        };

        for sector_index in 1..=sectors_to_read {
            result.extend(self.read_sector(first_sector_index + sector_index as u64)?);
        }

        if result.len() > amount_in_bytes {
            result.resize(amount_in_bytes, 0);
        }

        assert!(result.len() == amount_in_bytes);
        Ok(result)
    }

    fn write(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut bytes_written = 0;
        let first_sector_index = offset_in_bytes / (self.get_asize() as u64);
        let first_sector_offset = (offset_in_bytes % (self.get_asize() as u64)) as usize;
        if first_sector_offset == 0 && data.len() >= self.get_asize() {
            self.write_sector(
                first_sector_index,
                &data[bytes_written..bytes_written + self.get_asize()],
            )?;
            bytes_written += self.get_asize();
        } else {
            let mut first_sector = self.read_sector(first_sector_index)?;
            for overwrite_index in first_sector_offset..self.get_asize() {
                first_sector[overwrite_index] = data[bytes_written];
                bytes_written += 1;
                if bytes_written >= data.len() {
                    break;
                }
            }
            self.write_sector(first_sector_index, &first_sector)?;
        }

        if bytes_written >= data.len() {
            return Ok(());
        }

        let size_remaining = data.len() - bytes_written;
        let full_sectors_to_write = size_remaining / self.get_asize();
        for sector_index in 1..=full_sectors_to_write {
            self.write_sector(
                first_sector_index + sector_index as u64,
                &data[bytes_written..bytes_written + self.get_asize()],
            )?;
            bytes_written += self.get_asize();
        }

        if size_remaining % self.get_asize() != 0 {
            let mut last_sector =
                self.read_sector(first_sector_index + (full_sectors_to_write as u64) + 1)?;
            for overwrite_index in 0..self.get_asize() {
                last_sector[overwrite_index] = data[bytes_written];
                bytes_written += 1;
                if bytes_written >= data.len() {
                    break;
                }
            }
            self.write_sector(
                first_sector_index + (full_sectors_to_write as u64) + 1,
                &last_sector,
            )?;
        }

        assert!(bytes_written == data.len());
        Ok(())
    }

    // Maps label_index to the devices
    // 0..=3 => first device
    // 4..=7 => second device
    // etc.
    // If a device is not present it returns Err(()) when trying to read a label from that device
    fn read_raw_label(&mut self, label_index: usize) -> Result<Vec<u8>, ()> {
        let device_number = label_index / 4;
        let label_number = label_index % 4;
        let device = self.devices.get_mut(&device_number).ok_or(())?;
        device.read_raw_label(label_number)
    }

    fn get_nlables(&mut self) -> usize {
        self.devices.len() * 4
    }
}

#[derive(Debug)]
pub struct VdevLabel {
    name_value_pairs_raw: Vec<u8>,
    uberblocks_raw: Vec<u8>,
    uberblock_size: Option<usize>,
}

impl VdevLabel {
    pub fn from_bytes(data: &[u8]) -> VdevLabel {
        VdevLabel {
            name_value_pairs_raw: data[16 * 1024..128 * 1024].to_owned(),
            uberblocks_raw: data[128 * 1024..].to_owned(),
            uberblock_size: None,
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
        self.uberblock_size
            .expect("Uberblock size should be initialised!")
    }

    pub fn get_raw_uberblock(&self, index: usize) -> &[u8] {
        if index >= self.get_raw_uberblock_count() {
            panic!("Attempt to get uberblock past the end of the uberblock array!");
        }
        &self.uberblocks_raw
            [index * self.get_raw_uberblock_size()..(index + 1) * self.get_raw_uberblock_size()]
    }

    pub fn get_raw_uberblock_count(&self) -> usize {
        self.uberblocks_raw.len() / self.get_raw_uberblock_size()
    }

    pub fn get_name_value_pairs_raw(&self) -> &[u8] {
        &self.name_value_pairs_raw
    }
}

#[derive(Debug)]
pub struct Uberblock {
    pub version: u64,
    pub txg: u64,
    pub guid_sum: u64,
    pub timestamp: u64,
    pub rootbp: zio::BlockPointer,
}

const UBERBLOCK_MAGIC: u64 = 0x00bab10c;

impl<It> FromBytesLE<It> for Uberblock
where
    It: Iterator<Item = u8> + Clone,
{
    fn from_bytes_le(data: &mut It) -> Option<Uberblock> {
        let magic = u64::from_bytes_le(data)?;

        // Verify magic, to make sure we are using the correct endianness
        if magic != UBERBLOCK_MAGIC {
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Tried to parse uberblock with invalid magic!");
            return None;
        }

        Some(Uberblock {
            version: u64::from_bytes_le(data)?,
            txg: u64::from_bytes_le(data)?,
            guid_sum: u64::from_bytes_le(data)?,
            timestamp: u64::from_bytes_le(data)?,
            rootbp: zio::BlockPointer::from_bytes_le(data)?,
        })
    }
}

impl<It> FromBytes<It> for Uberblock
where
    It: Iterator<Item = u8> + Clone,
{
    fn from_bytes(data: &mut It) -> Option<Uberblock> {
        let ub_magic_le = u64::from_bytes_le(&mut data.clone())?;
        let ub_magic_be = u64::from_bytes_le(&mut data.clone())?;

        if ub_magic_le == UBERBLOCK_MAGIC {
            // Little-endian
            Self::from_bytes_le(data)
        } else if ub_magic_be == UBERBLOCK_MAGIC {
            // Big-endian
            todo!("Implement big endian support!");
        } else {
            // Invalid magic
            return None;
        }
    }
}
