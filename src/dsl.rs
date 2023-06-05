// Source
// http://www.giis.co.in/Zfs_ondiskformat.pdf (Section 4.4)

use crate::{byte_iter::FromBytesLE, zio::BlockPointer};

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

impl<It> FromBytesLE<It> for DSLDirectoryData
where
    It: Iterator<Item = u8>,
{
    fn from_bytes_le(data: &mut It) -> Option<DSLDirectoryData> {
        Some(DSLDirectoryData {
            creation_time: u64::from_bytes_le(data)?,
            head_dataset_object_number: u64::from_bytes_le(data)?,
            parent_object_number: u64::from_bytes_le(data)?,
            clone_parent_object_number: u64::from_bytes_le(data)?,
            children_directory_object_number: u64::from_bytes_le(data)?,
            used_bytes: u64::from_bytes_le(data)?,
            compressed_bytes: u64::from_bytes_le(data)?,
            uncompressed_bytes: u64::from_bytes_le(data)?,
            quota: u64::from_bytes_le(data)?,
            reserved: u64::from_bytes_le(data)?,
            props_object_number: u64::from_bytes_le(data)?,
        })
    }
}

impl DSLDirectoryData {
    pub const fn get_ondisk_size() -> usize {
        core::mem::size_of::<u64>() * 11
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
    block_pointer: BlockPointer,
}

impl DSLDatasetData {
    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<DSLDatasetData>
    where
        Iter: Iterator<Item = u8> + Clone,
    {
        Some(DSLDatasetData {
            parent_directory_object_number: u64::from_bytes_le(data)?,
            previous_snapshot_object_number: u64::from_bytes_le(data)?,
            previous_snapshot_txg: u64::from_bytes_le(data)?,
            next_snapshot_object_number: u64::from_bytes_le(data)?,
            snapshot_names_object_number: u64::from_bytes_le(data)?,
            num_references: u64::from_bytes_le(data)?,
            creation_time: u64::from_bytes_le(data)?,
            creation_txg: u64::from_bytes_le(data)?,
            deadlist_object_number: u64::from_bytes_le(data)?,
            used_bytes: u64::from_bytes_le(data)?,
            compressed_bytes: u64::from_bytes_le(data)?,
            uncompressed_bytes: u64::from_bytes_le(data)?,
            unique_bytes: u64::from_bytes_le(data)?,
            fsid_guid: u64::from_bytes_le(data)?,
            guid: u64::from_bytes_le(data)?,
            restoring: u64::from_bytes_le(data)?,
            block_pointer: BlockPointer::from_bytes_le(data)?,
        })
    }

    pub fn get_block_pointer(&mut self) -> &mut BlockPointer {
        &mut self.block_pointer
    }
}
