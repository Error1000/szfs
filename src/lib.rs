#![allow(dead_code)]
use std::{fs::File, io::{SeekFrom, Seek, Read, Write}, collections::HashMap};

use byte_iter::ByteIter;

pub mod nvlist;
pub mod byte_iter;
pub mod zio;
pub mod zil;
pub mod dmu;
pub mod fletcher;
pub mod lz4;
pub mod zap;
pub mod dsl;
pub mod zpl;
pub mod lzjb;

pub mod ansi_color {
    pub const RED: &str = "\u{001b}[31m";
    pub const YELLOW: &str = "\u{001b}[33m";
    pub const CYAN: &str = "\u{001b}[36m";
    pub const WHITE: &str = "\u{001b}[0m";
}

// TODO:
// 1. Implement spill blocks
// 2. Implement non-embedded fat zap tables
// 3. Implement gang blocks
// 4. Implement all nvlist values
// 5. Implement all fat zap values
// 6. Implement all system attributes
// 7. Don't hardcode vdev layout and implement ability to try other labels instead of just using the first one
// 8. Don't just skip the parity sectors in RAIDZ
// 9. Properly support sector sizes bigger than 512 bytes
// 10. Test RAIDZ writing, and in general implement writing
// 11. Figure out why dvas at the end of a plain file contents indirect block tree have vdev id 1


pub struct RaidzInfo {
    ndevices: usize,
    nparity: usize
}

pub trait Vdev {
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
}

impl VdevFile {
    pub fn read_raw(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        let mut buf = vec![0u8; amount_in_bytes];
        self.device.seek(SeekFrom::Start(offset_in_bytes)).map_err(|_| ())?;
        self.device.read(&mut buf).map_err(|_| ())?;
        Ok(buf)
    }

    pub fn write_raw(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        self.device.seek(SeekFrom::Start(offset_in_bytes)).map_err(|_| ())?;
        self.device.write(data).map_err(|_| ())?;
        Ok(())
    }
}

impl Vdev for VdevFile {
    fn get_raidz_info(&self) -> Option<RaidzInfo> {
        None
    }

    fn get_asize(&self) -> usize {
        unimplemented!()
    }

    fn read(&mut self, mut offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        if offset_in_bytes >= self.get_size()-4*1024*1024-2*256*1024 { return Err(()); }
        offset_in_bytes += 4*1024*1024;
        self.read_raw(offset_in_bytes, amount_in_bytes)
    }

    fn write(&mut self, mut offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        if offset_in_bytes >= self.get_size()-4*1024*1024-2*256*1024 { return Err(()); }
        offset_in_bytes += 4*1024*1024;
        self.write_raw(offset_in_bytes, data)
    }

    fn get_size(&self) -> u64 {
        self.device.metadata().unwrap().len()
    }

    // Source: http://www.giis.co.in/Zfs_ondiskformat.pdf
    // Section 1.2.1

    fn read_raw_label(&mut self, label_index: usize) -> Result<Vec<u8>, ()>{
        match label_index {
            0 => self.read_raw(0, 256*1024),
            1 => self.read_raw(256*1024, 256*1024),
            2 => self.read_raw(self.get_size()-2*256*1024, 256*1024),
            3 => self.read_raw(self.get_size()-256*1024, 256*1024),
            _ => Err(())
        }
    }

    fn get_nlables(&mut self) -> usize {
        4
    }
}

impl From<File> for VdevFile {
    fn from(f: File) -> Self {
        Self { device: f }
    }
}


pub struct VdevRaidz<'a> {
    devices: HashMap<usize, &'a mut dyn Vdev>,
    size: u64,
    ndevices: usize,
    nparity: usize,
    asize: usize
}

impl<'a> VdevRaidz<'a> {
    pub fn from_vdevs(devices: HashMap<usize, &'a mut dyn Vdev>, nparity: usize, asize: usize) -> VdevRaidz {
        let ndevices = devices.iter().max_by_key(|(k, _)| k.clone()).unwrap().0.clone()+1;
        let size = devices.iter().fold(0, |old, (_, v)| old +  v.get_size());
        VdevRaidz { 
            devices, 
            size, 
            ndevices, 
            nparity, 
            asize
        }
    }
    
    pub fn read_sector(&mut self, sector_index: u64) -> Result<Vec<u8>, ()> {
        let device_sector_index = sector_index/(self.ndevices as u64);
        let device_number = (sector_index%(self.ndevices as u64)) as usize;
        let asize = self.get_asize();

        self.devices
        .get_mut(&device_number)
        .ok_or(())?
        .read(device_sector_index*(asize as u64), asize)
    }

    pub fn write_sector(&mut self, sector_index: u64, data: &[u8]) -> Result<(), ()> {
        let device_sector_index = sector_index/(self.ndevices as u64);
        let device_number = (sector_index%(self.ndevices as u64)) as usize;
        let asize = self.get_asize();
        assert!(data.len() == asize);

        self.devices
        .get_mut(&device_number)
        .ok_or(())?
        .write(device_sector_index*(asize as u64), data)
    }
}

impl Vdev for VdevRaidz<'_> {
    fn get_raidz_info(&self) -> Option<RaidzInfo> {
        Some(RaidzInfo { ndevices: self.ndevices, nparity: self.nparity })
    }

    fn get_size(&self) -> u64 {
        self.size
    }

    fn get_asize(&self) -> usize {
        self.asize
    }

    // Note: Reading 0 bytes will *always* succeed
    fn read(&mut self, offset_in_bytes: u64, amount_in_bytes: usize) -> Result<Vec<u8>, ()> {
        if amount_in_bytes == 0 { return Ok(Vec::new()); }
        let mut result: Vec<u8> = Vec::new();
        let first_sector_index = offset_in_bytes/(self.get_asize() as u64);
        let first_sector_offset = offset_in_bytes%(self.get_asize() as u64);
        let first_sector = self.read_sector(first_sector_index)?;
        result.extend(first_sector.iter().skip(first_sector_offset as usize));
    
        if result.len() >= amount_in_bytes {
            result.resize(amount_in_bytes, 0);
            return Ok(result);
        }
    
        let size_remaining = amount_in_bytes-result.len();
        let sectors_to_read = if size_remaining%self.get_asize() == 0 { size_remaining/self.get_asize() } else { (size_remaining/self.get_asize())+1 };
        for sector_index in 1..=sectors_to_read {
            result.extend(self.read_sector(first_sector_index+sector_index as u64)?);
        }
    
        if result.len() >= amount_in_bytes {
            result.resize(amount_in_bytes, 0);
        }
        
        assert!(result.len() == amount_in_bytes);
        Ok(result)
    }

    fn write(&mut self, offset_in_bytes: u64, data: &[u8]) -> Result<(), ()> {
        if data.len() == 0 { return Ok(()); }
        let mut bytes_written = 0;
        let first_sector_index = offset_in_bytes/(self.get_asize() as u64);
        let first_sector_offset = (offset_in_bytes%(self.get_asize() as u64)) as usize;
        if first_sector_offset == 0 && data.len() >= self.get_asize() {
            self.write_sector(first_sector_index, &data[bytes_written..bytes_written+self.get_asize()])?;
            bytes_written += self.get_asize();
        }else{
            let mut first_sector = self.read_sector(first_sector_index)?;
            for overwrite_index in first_sector_offset..self.get_asize()-first_sector_offset {
                first_sector[overwrite_index] = data[bytes_written];
                bytes_written += 1;
                if bytes_written >= data.len() { break; }
            }
            self.write_sector(first_sector_index, &first_sector)?;
        }
    
        if bytes_written >= data.len() {
            return Ok(());
        }
    
        let size_remaining = data.len()-bytes_written;
        let full_sectors_to_write = size_remaining/self.get_asize();
        for sector_index in 1..=full_sectors_to_write {
            self.write_sector(first_sector_index+sector_index as u64, &data[bytes_written..bytes_written+self.get_asize()])?;
            bytes_written += self.get_asize();
        }

        if size_remaining%self.get_asize() != 0 {
            let mut last_sector = self.read_sector((full_sectors_to_write+1) as u64)?;
            for overwrite_index in 0..self.get_asize() {
                last_sector[overwrite_index] = data[bytes_written];
                bytes_written += 1;
                if bytes_written >= data.len() { break; }
            }
            self.write_sector((full_sectors_to_write+1) as u64, &last_sector)?;
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
        let device_number = label_index/4;
        let label_number = label_index%4;
        let device = self.devices.get_mut(&device_number).ok_or(())?;
        device.read_raw_label(label_number)
    }

    fn get_nlables(&mut self) -> usize {
        self.devices.len()*4
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
        if index >= self.get_raw_uberblock_count() { panic!("Attempt to get uberblock past the end of the uberblock array!"); }
        &self.uberblocks_raw[index*self.get_raw_uberblock_size()..(index+1)*self.get_raw_uberblock_size()]
    }

    pub fn get_raw_uberblock_count(&self) -> usize {
        self.uberblocks_raw.len()/self.get_raw_uberblock_size()
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
    pub rootbp: zio::BlockPointer
}

const UBERBLOCK_MAGIC: u64 = 0x00bab10c;
impl Uberblock {
   pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<Uberblock> 
   where Iter: Iterator<Item = u8> + Clone {
        let magic = data.read_u64_le()?;

        // Verify magic, to make sure we are using the correct endianness
        if magic != UBERBLOCK_MAGIC {
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Tried to parse uberblock with invalid magic!");
            return None;
        } 

        Some(Uberblock { 
            version: data.read_u64_le()?, 
            txg: data.read_u64_le()?, 
            guid_sum: data.read_u64_le()?, 
            timestamp: data.read_u64_le()?, 
            rootbp: zio::BlockPointer::from_bytes_le(data)? 
        })
   }

   // Endianness invariant uberblock loading
   pub fn from_bytes(data: &mut (impl Iterator<Item = u8> + Clone)) -> Option<Uberblock> {
        let ub_magic_le = data.clone().read_u64_le()?;
        let ub_magic_be = data.clone().read_u64_be()?;

        if ub_magic_le == UBERBLOCK_MAGIC { // Little-endian
            Self::from_bytes_le(data)
        } else if ub_magic_be == UBERBLOCK_MAGIC { // Big-endian
            todo!("Implement big endian support!");
        } else { // Invalid magic
            return None;
        }
   }

}