// Sources
// http://www.giis.co.in/Zfs_ondiskformat.pdf (Section 4.4)

use crate::byte_iter::ByteIter;

#[derive(Debug)]
pub struct DSLDirectoryData {
    creation_time: u64,
    head_dataset_object_number: u64,
    parent_object_number: u64,

    // For cloned object sets, this field contains the number of the snapshot from which this clone was created
    clone_parent_object_number: u64,  
    
    children_directory_object_number: u64,

    // Number of bytes used by all datasets within this directory, includes any snapshot and child dataset used bytes
    used_bytes: u64,

    // Number of compressed bytes for all datasets within this DSL directory
    compressed_bytes: u64,

    // Number of uncompressed bytes for all datasets within this DSL directory
    uncompressed_bytes: u64,

    // Quota can not be exceeded by the datasets within this DSL directory
    quota: u64,

    // The amount of space reserved for consumption by the datasets within this DSL directory
    reserved: u64,

    props_object_number: u64,
}

impl DSLDirectoryData {
    pub fn get_ondisk_size() -> usize {
        core::mem::size_of::<u64>()*11
    }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<DSLDirectoryData> {
        Some(DSLDirectoryData { 
            creation_time: data.read_u64_le()?, 
            head_dataset_object_number: data.read_u64_le()?, 
            parent_object_number: data.read_u64_le()?, 
            clone_parent_object_number: data.read_u64_le()?, 
            children_directory_object_number: data.read_u64_le()?, 
            used_bytes: data.read_u64_le()?, 
            compressed_bytes: data.read_u64_le()?, 
            uncompressed_bytes: data.read_u64_le()?, 
            quota: data.read_u64_le()?, 
            reserved: data.read_u64_le()?, 
            props_object_number: data.read_u64_le()? 
        })
    }

    pub fn get_head_dataset_object_number(&self) -> u64 {
        self.head_dataset_object_number
    }
}