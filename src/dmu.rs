use crate::{zio::{self, ChecksumMethod, CompressionMethod, BlockPointer, Vdevs}, byte_iter::ByteIter, zil::ZilHeader, zap, dsl};
use std::{fmt::Debug, collections::HashMap};

#[derive(Debug, PartialEq, Eq)]
pub enum ObjType {
    None = 0,
    ObjectDirectory = 1,
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
    DSLDirectory = 12,
    DSLDirectoryChildMap = 13,
    DSLDataSetSnapshotMap = 14,
    DSLProperties = 15,
    DSLDataset = 16,
    ZNode = 17,
    OldAccessControlList = 18,
    PlainFileContents = 19,
    DirectoryContents = 20,
    MasterNode = 21,
    DeleteQueue = 22,
    ZVol = 23,
    ZVolProperties = 24,

    PlainOther = 25,
    U64Other = 26,
    ZapOther = 27,

    ErrorLog = 28,
    SpaHistory = 29,
    SpaHistoryOffsets = 30,
    PoolProperties = 31,
    DSLPermissions = 32,
    AccessControlList = 33,
    SystemAccessControlList = 34,
    FUidTable = 35,
    FUidSize = 36,
    NextClones = 37,
    ScanQueue = 38,
    UserGroupUsed = 39,
    UserGroupQuota = 40,
    UserRefs = 41,
    DDTZap = 42,
    DDTStats = 43,
    SystemAttributes = 44,
    SystemAttributesMasterNode = 45,
    SystemAttributesRegistrations = 46,
    SystemAttributesLayouts = 47,
    ScanXLate = 48,
    Dedup = 49,
    DeadList = 50,
    DeadListHeader = 51,
    DSLClones = 52,
    BlockPointerObjectSubObject = 53,
}

impl ObjType {
    pub fn from_value(value: usize) -> Option<Self> {
        Some(match value {
            0 => Self::None,
            1 => Self::ObjectDirectory,
            2 => Self::ObjectArray,
            3 => Self::PackedNVList,
            4 => Self::PackedNVListSize,
            5 => Self::BlockPointerList,
            6 => Self::BlockPointerListHeader,
            7 => Self::SpaceMapHeader,
            8 => Self::SpaceMap,
            9 => Self::IntentLog,
            10 => Self::DNode,
            11 => Self::ObjSet,
            12 => Self::DSLDirectory,
            13 => Self::DSLDirectoryChildMap,
            14 => Self::DSLDataSetSnapshotMap,
            15 => Self::DSLProperties,
            16 => Self::DSLDataset,
            17 => Self::ZNode,
            18 => Self::OldAccessControlList,
            19 => Self::PlainFileContents,
            20 => Self::DirectoryContents,
            21 => Self::MasterNode,
            22 => Self::DeleteQueue,
            23 => Self::ZVol,
            24 => Self::ZVolProperties,
        
            25 => Self::PlainOther,
            26 => Self::U64Other,
            27 => Self::ZapOther,
        
            28 => Self::ErrorLog,
            29 => Self::SpaHistory,
            30 => Self::SpaHistoryOffsets,
            31 => Self::PoolProperties,
            32 => Self::DSLPermissions,
            33 => Self::AccessControlList,
            34 => Self::SystemAccessControlList,
            35 => Self::FUidTable,
            36 => Self::FUidSize,
            37 => Self::NextClones,
            38 => Self::ScanQueue,
            39 => Self::UserGroupUsed,
            40 => Self::UserGroupQuota,
            41 => Self::UserRefs,
            42 => Self::DDTZap,
            43 => Self::DDTStats,
            44 => Self::SystemAttributes,
            45 => Self::SystemAttributesMasterNode,
            46 => Self::SystemAttributesRegistrations,
            47 => Self::SystemAttributesLayouts,
            48 => Self::ScanXLate,
            49 => Self::Dedup,
            50 => Self::DeadList,
            51 => Self::DeadListHeader,
            52 => Self::DSLClones,
            53 => Self::BlockPointerObjectSubObject,
            _ => return None
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BonusType {
    None = 0,
    PackedNVListSize = 4,
    SpaceMapHeader = 7,
    DSLDirectory = 12,
    DSLDataset = 16,
    ZNode = 17,
    // Source: https://github.com/openzfs/zfs/blob/master/include/sys/dmu.h#L226
    SystemAttributes = 44,
}

impl BonusType {
    pub fn from_value(value: usize) -> Option<Self> {
        Some(match value {
            0  => Self::None,
            4  => Self::PackedNVListSize,
            7  => Self::SpaceMapHeader,
            12 => Self::DSLDirectory,
            16 => Self::DSLDataset,
            17 => Self::ZNode,
            44 => Self::SystemAttributes,
            _ => return None
        })
    }
}

mod dnode_flag {
    pub const USED_AMOUNT_IS_IN_BYTES: u8 = 1 << 0;
    pub const HAS_SPILL_BLKPTR: u8 = 1 << 2;
}


// General dnode data, not specific to any type of dnode
pub struct DNodeBase {
    indirect_blocksize_log2: u8,
    n_indirect_levels: u8,
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

impl Debug for DNodeBase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
        .debug_struct("DNodeBase")
        .field("indirect_blocksize", &self.parse_indirect_block_size())
        .field("n_indirect_levels", &self.n_indirect_levels)
        .field("checksum_method", &self.checksum_method)
        .field("compression_method", &self.compression_method)
        .field("data_blocksize", &self.parse_data_block_size())
        .field("num_slots", &self.num_slots)
        .field("max_indirect_block_id", &self.max_indirect_block_id)
        .field("total_allocated", &self.total_allocated)
        .field("total_allocated_is_in_bytes", &self.total_allocated_is_in_bytes)
        .field("block_pointers", &self.block_pointers)
        .field("bonus_data", &self.bonus_data)
        .finish()
    }
}

#[derive(Debug)]
struct IndirectBlockTag {
    parent_id: usize, // Id of the block on the upper layer that contains the block that we want
    offset: usize // At what index in the upper layer block can you find the pointer to the this layer's block (the block that we want) 
}

impl DNodeBase {
    pub fn get_ondisk_size(&self) -> usize {
        usize::from(self.num_slots)*512
    }

    pub fn get_n_slots_from_bytes_le(mut data: impl Iterator<Item = u8>) -> Option<usize> {
        data.skip_n_bytes(12)?;
        let extra_slots = data.next()?;
        Some(usize::from(extra_slots)+1)
    }

    // Note: This will always read a multiple of 512 bytes as all dnodes have a size that is a multiple of 512 which was
    // the old size of one "slot", however newer implementations allow dnodes to take up multiple slots so therefore a multiple of 512.
    // Source: https://github.com/openzfs/zfs/blob/master/include/sys/dnode.h#L188
    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<(DNodeBase, ObjType, BonusType)>
    where Iter: Iterator<Item = u8> + Clone {
        let dnode_type = ObjType::from_value(data.next()?.into())?;
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
        data.skip_n_bytes(3)?; // Ignore 3 padding bytes
        // We have read 16 bytes up until now

        let max_indirect_block_id = data.read_u64_le()?;
        let total_allocated = data.read_u64_le()?; /* bytes (or sectors, depending on a flag) of disk space */
        data.skip_n_bytes(4*core::mem::size_of::<u64>())?; // Ignore 4 u64 paddings

        if flags & dnode_flag::HAS_SPILL_BLKPTR != 0 {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!("{YELLOW}Warning{WHITE}: Tried to read a dnode with spill block, this is not supported!");
            }
            return None;
        }

        // Currently there must be at least one block pointer and at most 3
        if !(n_block_pointers >= 1 && n_block_pointers <= 3) {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!("{YELLOW}Warning{WHITE}: Tried to parse a dnode with {} block pointers, sanity check failed!", n_block_pointers);
            }
            return None;
        }

        // So far we have read 64 bytes, this is where the tail starts
        // The tail contains the variably sized data like the blkptrs, the bonus_data
        // and the padding needed to reach a multiple of 512 bytes

        // Read n_block_pointers block pointers
        let mut block_pointers = Vec::new();
        for _ in 0..n_block_pointers {
            // NOTE: We try to read the block pointers even if we are not going to need them
            // This means that we sometimes try to parse "unallocated" block pointers that might be all zeros
            // but because we check the checksum and the endianness this will fail so it's fine
            if let Some(bp) = zio::BlockPointer::from_bytes_le(&mut data.clone()) {
                block_pointers.push(bp);
            }
            data.skip_n_bytes(zio::BlockPointer::get_ondisk_size())?;
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

        // Sanity check that the size of the dnode calculated using the n_block_pointers and bonus_data_len is the same as the one calculated form the number of slots this dnode takes up
        if rounded_up_total_size != (usize::from(extra_slots)+1)*512 {
            use crate::ansi_color::*;
            if cfg!(feature = "debug") {
                println!("{YELLOW}Warning{WHITE}: Tried to parse an dnode whose (nslots) size doesn't match up with the actual size read!");
            }
            return None;
        }

        let tail_padding_size = rounded_up_total_size-total_size;
        // We have all the data, and we don't need any data after the tail padding bytes
        // So if we can't read the tail padding bytes it's not the end of the world
        // Just log it
        if data.skip_n_bytes(tail_padding_size).is_none() {
            use crate::ansi_color::*;
            if cfg!(feature = "debug"){
                println!("{YELLOW}Warning{WHITE}: Tried to parse dnode whose size is smaller than expected, thankfully all the data is still there ( the only missing part is in the padding in the tail ) so we won't error out!")
            }
        }

        Some((DNodeBase { 
            indirect_blocksize_log2, 
            n_indirect_levels,  
            checksum_method, 
            compression_method, 
            data_blocksize_in_sectors, 
            num_slots: extra_slots+1, 
            max_indirect_block_id, 
            total_allocated, 
            total_allocated_is_in_bytes: (flags & dnode_flag::USED_AMOUNT_IS_IN_BYTES) != 0,
            block_pointers, 
            bonus_data 
        }, dnode_type, bonus_data_type))
    }

    pub fn parse_data_block_size(&self) -> usize {
        usize::from(self.data_blocksize_in_sectors)*512
    }

    pub fn parse_indirect_block_size(&self) -> usize {
        2usize.pow(u32::from(self.indirect_blocksize_log2))
    }

    // blocks_per_indirect_block is the branching factor of the upper layer
    // current_level_id is the id of the node in the current layer
    // Returns: The id of the parent block in the upper layer and the offset in the parent block
    fn next_level_id_and_offset(&self, current_level_id: usize, blocks_per_indirect_block: usize) -> IndirectBlockTag {
        IndirectBlockTag {
            parent_id: current_level_id/blocks_per_indirect_block, 
            offset: current_level_id%blocks_per_indirect_block
        }
    }

    pub fn get_data_size(&self) -> usize {
        ((self.max_indirect_block_id+1) as usize)*self.parse_data_block_size()
    }

    pub fn read_block(&mut self, block_id: usize, vdevs: &mut zio::Vdevs) -> Result<Vec<u8>, ()> {
        if block_id > self.max_indirect_block_id as usize { return Err(()); }
        assert!(self.n_indirect_levels >= 1);
        let blocks_per_indirect_block = self.parse_indirect_block_size()/BlockPointer::get_ondisk_size();

        let mut levels: Vec<IndirectBlockTag> = Vec::new();
        // Note: We are traversing the tree backwards from the leafs to the root
        for level in 1..=self.n_indirect_levels {
            let actual_id = if level == 1 {
                block_id
            } else {
                levels.last().unwrap().parent_id
            };

            let actual_blocks_per_indirect_block = if level == self.n_indirect_levels {
                self.block_pointers.len()
            } else {
                blocks_per_indirect_block
            };
            
            levels.push(self.next_level_id_and_offset(actual_id, actual_blocks_per_indirect_block));
        }

        // Travel back down to the leafs
        let top_level = levels.pop().unwrap();
        let mut indirect_block_data;
        let mut next_block_pointer_ref = &mut self.block_pointers[top_level.offset];
        let mut next_block_pointer;
        for _ in 0..self.n_indirect_levels-1 {
            indirect_block_data = next_block_pointer_ref.dereference(vdevs)?;
            let cur_level = levels.pop().unwrap();
            next_block_pointer = {
                let mut iter = indirect_block_data.iter().copied();
                iter.skip_n_bytes(BlockPointer::get_ondisk_size()*cur_level.offset).ok_or(())?;
                BlockPointer::from_bytes_le(&mut iter).ok_or(())?
            };
            next_block_pointer_ref = &mut next_block_pointer;
        }

        let block_data = next_block_pointer_ref.dereference(vdevs)?;
        assert!(block_data.len() == self.parse_data_block_size());
        Ok(block_data)
    }
    
    // Note: Reading 0 bytes will *always* succeed
    pub fn read(&mut self, offset: u64, size: usize, vdevs: &mut zio::Vdevs) -> Result<Vec<u8>, ()> {
        if size == 0 { return Ok(Vec::new()); }
        let mut result: Vec<u8> = Vec::new();
        let first_data_block_index = offset/(self.parse_data_block_size() as u64);
        let first_data_block_offset = offset%(self.parse_data_block_size() as u64);
        let first_data_block = self.read_block(first_data_block_index as usize, vdevs)?;
        result.extend(first_data_block.iter().skip(first_data_block_offset as usize));
    
        if result.len() >= size {
            result.resize(size, 0);
            return Ok(result);
        }
    
        let size_remaining = size-result.len();
        let blocks_to_read = if size_remaining%self.parse_data_block_size() == 0 { size_remaining/self.parse_data_block_size() } else { (size_remaining/self.parse_data_block_size())+1 };
        for block_index in 1..=blocks_to_read {
            result.extend(self.read_block((first_data_block_index+block_index as u64) as usize, vdevs)?);
        }
    
        if result.len() >= size {
            result.resize(size, 0);
        }
        
        assert!(result.len() == size);
        Ok(result)
    
    }

    pub fn get_block_pointers(&mut self) -> &mut Vec<BlockPointer> {
        &mut self.block_pointers
    }

    pub fn get_bonus_data(&self) -> &[u8] {
        &self.bonus_data
    }
}



pub struct DNodeDSLDirectory (pub DNodeBase);

impl Debug for DNodeDSLDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NOTE: Since this type of dnode does not contain data show info about the block pointers, data block size, and the allocated size, is useless, so we don't do it
        f
        .debug_struct("DNodeDSLDirectory")
        .field("checksum_method", &self.0.checksum_method)
        .field("compression_method", &self.0.compression_method)
        .field("num_slots", &self.0.num_slots)
        .field("bonus", &self.parse_bonus_data())
        .finish()
    }
}

impl DNodeDSLDirectory {
    pub fn parse_bonus_data(&self) -> Option<dsl::DSLDirectoryData> {
        dsl::DSLDirectoryData::from_bytes_le(&mut self.0.bonus_data.iter().copied())
    }
}

pub struct DNodeDSLDataset (pub DNodeBase);

impl Debug for DNodeDSLDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NOTE: Since this type of dnode does not contain data show info about the block pointers, data block size, and the allocated size, is useless, so we don't do it
        f
        .debug_struct("DNodeDSLDataset")
        .field("checksum_method", &self.0.checksum_method)
        .field("compression_method", &self.0.compression_method)
        .field("num_slots", &self.0.num_slots)
        .field("bonus", &self.parse_bonus_data())
        .finish()
    }
}

impl DNodeDSLDataset {
    pub fn parse_bonus_data(&self) -> Option<dsl::DSLDatasetData> {
        dsl::DSLDatasetData::from_bytes_le(&mut self.0.bonus_data.iter().copied())
    }
}

#[derive(Debug)]
pub struct ZapDNode (pub DNodeBase);
impl ZapDNode {
    pub fn get_zap_header(&mut self, vdevs: &mut Vdevs) -> Option<zap::ZapHeader> {
        zap::ZapHeader::from_bytes_le(&mut self.0.read_block(0, vdevs).ok()?.iter().copied(), self.0.parse_data_block_size())
    }

    pub fn dump_zap_contents(&mut self, vdevs: &mut Vdevs) -> Option<HashMap<String, zap::Value>> {
        let header = self.get_zap_header(vdevs)?;
        header.dump_contents(&mut self.0, vdevs)
    }
}


#[derive(Debug)]
pub struct DNodeDirectoryContents(pub DNodeBase, pub BonusType);

impl DNodeDirectoryContents {
    pub fn get_zap_header(&mut self, vdevs: &mut Vdevs) -> Option<zap::ZapHeader> {
        zap::ZapHeader::from_bytes_le(&mut self.0.read_block(0, vdevs).ok()?.iter().copied(), self.0.parse_data_block_size())
    }

    pub fn dump_zap_contents(&mut self, vdevs: &mut Vdevs) -> Option<HashMap<String, zap::Value>> {
        let header = self.get_zap_header(vdevs)?;
        header.dump_contents(&mut self.0, vdevs)
    }
}


#[derive(Debug)]
pub struct DNodePlainFileContents(pub DNodeBase, pub BonusType);


#[derive(Debug)]
pub enum DNode {
    ObjectDirectory(ZapDNode),
    DSLDirectory(DNodeDSLDirectory),
    DSLDataset(DNodeDSLDataset),
    MasterNode(ZapDNode),
    DirectoryContents(DNodeDirectoryContents),
    PlainFileContents(DNodePlainFileContents),
    SystemAttributesMasterNode(ZapDNode),
    SystemAttributesLayouts(ZapDNode),
    SystemAttributesRegistrations(ZapDNode),
}

impl DNode {
    pub fn get_n_slots_from_bytes_le(data: impl Iterator<Item = u8>) -> Option<usize> {
        DNodeBase::get_n_slots_from_bytes_le(data)
    }
    
    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<DNode>
    where Iter: Iterator<Item = u8> + Clone {
        let (dnode_base, dnode_type, bonus_data_type) = DNodeBase::from_bytes_le(data)?;
        Some(match (dnode_type, bonus_data_type) {
            (ObjType::ObjectDirectory, BonusType::None) => DNode::ObjectDirectory(ZapDNode(dnode_base)),
            (ObjType::DSLDirectory, BonusType::DSLDirectory) => DNode::DSLDirectory(DNodeDSLDirectory(dnode_base)),
            (ObjType::DSLDataset, BonusType::DSLDataset) => DNode::DSLDataset(DNodeDSLDataset(dnode_base)),
            (ObjType::PlainFileContents, bonus_type) => DNode::PlainFileContents(DNodePlainFileContents(dnode_base, bonus_type)),
            (ObjType::DirectoryContents, bonus_type) => DNode::DirectoryContents(DNodeDirectoryContents(dnode_base, bonus_type)),
            (ObjType::MasterNode, BonusType::None) => DNode::MasterNode(ZapDNode(dnode_base)),
            (ObjType::SystemAttributesMasterNode, BonusType::None) => DNode::SystemAttributesMasterNode(ZapDNode(dnode_base)),
            (ObjType::SystemAttributesLayouts, BonusType::None) => DNode::SystemAttributesLayouts(ZapDNode(dnode_base)),
            (ObjType::SystemAttributesRegistrations, BonusType::None) => DNode::SystemAttributesRegistrations(ZapDNode(dnode_base)),
            (obj_type, bonus_type) => {
                use crate::ansi_color::*;
                if cfg!(feature = "debug") {
                    println!("{YELLOW}Warning{WHITE}: Tried to parse dnode type {obj_type:?} with bonus buffer type {bonus_type:?}, which is not supported!")
                }
                return None;
            }
        })
    }

    pub fn get_inner(&mut self) -> &mut DNodeBase {
        match self {
            DNode::ObjectDirectory(d) => &mut d.0,
            DNode::DSLDirectory(d) => &mut d.0,
            DNode::DSLDataset(d) => &mut d.0,
            DNode::MasterNode(d) => &mut d.0,
            DNode::DirectoryContents(d) => &mut d.0,
            DNode::PlainFileContents(d) => &mut d.0,
            DNode::SystemAttributesMasterNode(d) => &mut d.0,
            DNode::SystemAttributesLayouts(d) => &mut d.0,
            DNode::SystemAttributesRegistrations(d) => &mut d.0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ObjSetType {
    None = 0,
    Meta = 1,
    Zfs  = 2,
    Zvol = 3
}

impl ObjSetType {
    pub fn from_value(value: usize) -> Option<Self> {
        Some(match value {
            0 => Self::None,
            1 => Self::Meta,
            2 => Self::Zfs,
            3 => Self::Zvol,
            _ => return None
        })
    }
}

#[derive(Debug)]
pub struct ObjSet {
    pub metadnode: DNodeBase,
    pub zil: Option<ZilHeader>,
    pub typ: ObjSetType
}

impl ObjSet {
    pub const fn get_ondisk_size() -> usize { 1024 }

    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<ObjSet>
    where Iter: Iterator<Item = u8> + Clone {
        let (metadnode, metadnode_type, _) = DNodeBase::from_bytes_le(data)?;
        if metadnode_type != ObjType::DNode { 
            use crate::ansi_color::*;
            if cfg!(feature = "debug"){
                println!("{YELLOW}Warning{WHITE}: Tried to parse objset with metadnode of type: {:?}, that is not the right type!", metadnode_type);
            }
            return None; 
        }

        let zil = ZilHeader::from_bytes_le(&mut data.clone());
        data.skip_n_bytes(ZilHeader::get_ondisk_size())?;

        let typ = ObjSetType::from_value(data.read_u64_le()?.try_into().ok()?)?;
        // Consume padding
        let size_read = metadnode.get_ondisk_size() + ZilHeader::get_ondisk_size() + core::mem::size_of::<u64>();
        let remaining = Self::get_ondisk_size() - size_read;
        if data.skip_n_bytes(remaining).is_none(){
            use crate::ansi_color::*;
            if cfg!(feature = "debug"){
                println!("{YELLOW}Warning{WHITE}: Tried to parse objset whose size is smaller than expected, thankfully all the data is still there ( the only missing part is in the padding in the tail ) so we won't error out!")
            }
        }

        Some(ObjSet { 
            metadnode, 
            zil, 
            typ
        })
    }

    pub fn get_dnode_at(&mut self, index: usize, vdevs: &mut Vdevs) -> Option<DNode> {
        let mut data = self.metadnode.read((index*512) as u64, 512, vdevs).ok()?;
        let dnode_slots = DNodeBase::get_n_slots_from_bytes_le(data.iter().copied())?;
        data.extend(self.metadnode.read(((index+1)*512) as u64, (dnode_slots-1)*512, vdevs).ok()?.iter());
        DNode::from_bytes_le(&mut data.iter().copied())
    }
}


