// Source
// http://www.giis.co.in/Zfs_ondiskformat.pdf (Section 4.4)

use crate::{byte_iter::ByteIter, zio::BlockPointer};

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
    pub const fn get_ondisk_size() -> usize {
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

#[derive(Debug)]
pub struct DSLDatasetData {
    parent_directory_object_number: u64,


    // If this dataset represents a filesystem, volume or clone, this field contains
    // the 64 bit object number for the most recent snapshot taken
    // This field is zero if not snapshots have been taken

    // If this dataset represents a snapshot, this field contains
    // the 64 bit object number for the snapshot taken prior to this snapshot.
    // This field is zero if there are no previous snapshots.
    previous_snapshot_object_number: u64,
    previous_snapshot_txg: u64,

    // Only used for datasets representing a snapshot.
    // This field is always zero for datasets representing clones, volumes, or filesystems
    next_snapshot_object_number: u64,

    // The object contains all snapshot names along with their dataset object number
    snapshot_names_object_number: u64,

    // Always zero if it is *not* a snapshot
    // For snapshots, this is the number of references to this snapshot
    // 1 ( from the next snapshot, or from the active dataset) + the number of clones originating from this snapshot
    num_references: u64,

    // When this dataset was created
    creation_time: u64,
    creation_txg: u64,

    // The object contains an array of block pointers that were deleted since the last snapshot
    deadlist_object_number: u64,

    // Number of bytes used by the object set represented by this dataset
    used_bytes: u64,

    // Number of compressed bytes in the object set represented by this dataset
    compressed_bytes: u64,

    // Number of uncompressed bytes in the object set represented by this dataset
    uncompressed_bytes: u64,

    // The amount of unique data is stored in this field ( data that is no longer referenced by the source ( in the case of a snapshot ) )
    unique_bytes: u64,

    // ID that is unique amongst all currently open datasets
    // NOTE: Could change between successive dataset opens.
    fsid_guid: u64,

    // Global ID for this dataset.
    // This value never changes during the lifetime of the object set
    guid: u64,

    // This field is set to 1 if ZFS is in the process of restoring to this dataset through 'zfs restore'
    restoring: u64,

    // Block pointer to the object set that his dataset represents
    block_pointer: BlockPointer
}

impl DSLDatasetData {
    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<DSLDatasetData> 
    where Iter: Iterator<Item = u8> + Clone {
        Some(DSLDatasetData {
            parent_directory_object_number: data.read_u64_le()?, 
            previous_snapshot_object_number: data.read_u64_le()?, 
            previous_snapshot_txg: data.read_u64_le()?, 
            next_snapshot_object_number: data.read_u64_le()?, 
            snapshot_names_object_number: data.read_u64_le()?, 
            num_references: data.read_u64_le()?, 
            creation_time: data.read_u64_le()?, 
            creation_txg: data.read_u64_le()?, 
            deadlist_object_number: data.read_u64_le()?, 
            used_bytes: data.read_u64_le()?, 
            compressed_bytes: data.read_u64_le()?, 
            uncompressed_bytes: data.read_u64_le()?, 
            unique_bytes: data.read_u64_le()?, 
            fsid_guid: data.read_u64_le()?, 
            guid: data.read_u64_le()?, 
            restoring: data.read_u64_le()?, 
            block_pointer: BlockPointer::from_bytes_le(data)? 
        })
    }

    pub fn get_block_pointer(&mut self) -> &mut BlockPointer {
        &mut self.block_pointer
    }
}