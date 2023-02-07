use crate::{zio::{self, ChecksumMethod, CompressionMethod}, byte_iter::ByteIter};

#[derive(Debug, PartialEq, Eq)]
pub enum Type {
    None = 0,
    Directory = 1,
    ObjectArray = 2,
    PackedNVList = 3,
    PackedNVListSize = 4,
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

impl Type {
    pub fn from_value(value: usize) -> Option<Self> {
        Some(match value {
            0  => Self::None,
            1  => Self::Directory, 
            2  => Self::ObjectArray,
            3  => Self::PackedNVList,
            4  => Self::PackedNVListSize,
            5  => Self::BlockPointerList,
            6  => Self::BlockPointerListHeader,
            7  => Self::SpaceMapHeader,
            8  => Self::SpaceMap,
            9  => Self::IntentLog,
            10 => Self::DNode,
            11 => Self::ObjSet,
            12 => Self::DSLDataset,
            13 => Self::DSLDatasetChildMap,
            14 => Self::ObjSetSnapshotMap,
            15 => Self::DSLProperties,
            16 => Self::DSLObjSet,
            17 => Self::ZNode,
            18 => Self::AcessControlList,
            19 => Self::PlainFileContents,
            20 => Self::DirectoryContents,
            21 => Self::MasterNode,
            22 => Self::DeleteQueue,
            23 => Self::ZVol,
            24 => Self::ZVolProperties,
            _ => return None
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BonusType {
    None = 0,
    PackedNVListSize = 4,
    SpaceMapHeader = 7,
    DSLDataset = 12,
    DSLObjSet = 16,
    ZNode = 17
}

impl BonusType {
    pub fn from_value(value: usize) -> Option<Self> {
        Some(match value {
            0 => Self::None,
            4 => Self::PackedNVListSize,
            7 => Self::SpaceMapHeader,
            12 => Self::DSLDataset,
            16 => Self::DSLObjSet,
            17 => Self::ZNode,
            _ => return None
        })
    }
}

mod dnode_flag {
    pub const UsedAmountIsInBytes: u8 = 1 << 0;
    pub const HasSpillBlkptr: u8 = 1 << 2;
}

#[derive(Debug)]
pub struct Dnode {
    typ: Type,
    indirect_blocksize_log2: u8,
    n_indirect_levels: u8,
    bonus_data_type: BonusType,
    checksum_method: zio::ChecksumMethod,
    compression_method: zio::CompressionMethod,
    data_blocksize_in_sectors: u16,
    num_slots: u8, // A big dnode may take up multiple dnode "slots", a dnode slot is 512 bytes
    max_indirect_block_id: u64,
    total_allocated: u64,
    total_allocated_is_in_bytes: bool, // if false then it is in sectors
    block_pointers: Vec<zio::BlockPointer>,
    bonus_data: Vec<u8>
}

impl Dnode {
    pub fn from_bytes(data: &mut impl Iterator<Item = u8>) -> Option<Dnode> {
        let dnode_type = Type::from_value(data.next()?.into())?;
        let indirect_blocksize_log2 = data.next()?;
        let n_indirect_levels = data.next()?;
        let n_block_pointers = data.next()?;
        let bonus_data_type = BonusType::from_value(data.next()?.into())?;
        let checksum_method = ChecksumMethod::from_value(data.next()?.into())?;
        let compression_method = CompressionMethod::from_value(data.next()?.into())?;
        let flags = data.next()?; // Ignore 1 padding byte ( dn_flags in newer versions )
        let data_blocksize_in_sectors = data.read_u16_le()?;
        let bonus_data_len = data.read_u16_le()?;
        let extra_slots = data.next()?;
        let _ = data.nth(3-1)?; // Ignore 3 padding bytes
        let max_indirect_block_id = data.read_u64_le()?;
        let total_allocated = data.read_u64_le()?; /* bytes (or sectors, depending on a flag) of disk space */
        let _ = data.nth(4*core::mem::size_of::<u64>()-1)?; // Ignore 4 u64 paddings

        if flags & dnode_flag::HasSpillBlkptr != 0 {
            todo!("Implement spill blocks for dnodes!");
        }

        // Currently there must be at least one block pointer and at most 3
        assert!(n_block_pointers >= 1 && n_block_pointers <= 3);

        // So far we have read 64 bytes, this is where the tail starts
        // The tail contains the variably sized data like the blkptrs, the bonus_data
        // and the padding needed to reach a multiple of 512 bytes

        // Read n_block_pointers block pointers
        let mut block_pointers = Vec::new();
        for _ in 0..n_block_pointers {
            if let Some(bp) = zio::BlockPointer::from_bytes_le(data) {
                block_pointers.push(bp);
            }
        }

        let mut bonus_data = Vec::new();

        // Read bonus_data
        for _ in 0..bonus_data_len {
            bonus_data.push(data.next()?);
        }

        // Read remaining padding until the next multiple of 512 bytes
        let total_size: usize = 64+usize::from(n_block_pointers)*zio::BlockPointer::get_ondisk_size()+usize::from(bonus_data_len);
        // Round up the size to the next multiple of 512 bytes
        let rounded_up_total_size = if total_size%512 == 0 { total_size } else { ((total_size/512)+1)*512 };

        // Check that the size of the dnode calculated using the n_block_pointers and bonus_data_len is the same as the one calculated form the number of slots this dnode takes up
        assert!(rounded_up_total_size == (usize::from(extra_slots)+1)*512); 

        let tail_padding_size = rounded_up_total_size-total_size;
        let _ = data.nth(tail_padding_size-1)?;

        Some(Dnode { 
            typ: dnode_type, 
            indirect_blocksize_log2, 
            n_indirect_levels, 
            bonus_data_type, 
            checksum_method, 
            compression_method, 
            data_blocksize_in_sectors, 
            num_slots: extra_slots+1, 
            max_indirect_block_id, 
            total_allocated, 
            total_allocated_is_in_bytes: (flags & dnode_flag::UsedAmountIsInBytes) != 0,
            block_pointers, 
            bonus_data 
        })
    }
}