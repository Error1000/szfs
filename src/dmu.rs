use crate::{zio::{self, ChecksumMethod, CompressionMethod, BlockPointer, Vdevs}, byte_iter::ByteIter, zil::ZilHeader};

#[derive(Debug, PartialEq, Eq)]
pub enum ObjType {
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

impl ObjType {
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
            0  => Self::None,
            4  => Self::PackedNVListSize,
            7  => Self::SpaceMapHeader,
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
    typ: ObjType,
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

#[derive(Debug)]
struct IndirectBlockTag {
    id: usize, // With number is the block if you were to sequentially lay out all the blocks at this level
    offset: usize // At what index in the block can you find the pointer to the next level 
}

impl Dnode {
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
    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<Dnode>
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
        assert!(rounded_up_total_size == (usize::from(extra_slots)+1)*512); 

        let tail_padding_size = rounded_up_total_size-total_size;
        data.skip_n_bytes(tail_padding_size)?;

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

    pub fn parse_data_block_size(&self) -> usize {
        usize::from(self.data_blocksize_in_sectors)*512
    }

    pub fn parse_indirect_block_size(&self) -> usize {
        2usize.pow(u32::from(self.indirect_blocksize_log2))
    }

    fn next_level_id_and_offset(&self, current_level_id: usize) -> IndirectBlockTag {
        let blocks_per_indirect_block = self.parse_indirect_block_size()/BlockPointer::get_ondisk_size();
        IndirectBlockTag {
            id: current_level_id/blocks_per_indirect_block, 
            offset: current_level_id%blocks_per_indirect_block
        }
    }

    pub fn get_data_size(&self) -> usize {
        usize::try_from(self.max_indirect_block_id+1).unwrap()*self.parse_data_block_size()
    }

    pub fn read_block(&mut self, block_id: usize, vdevs: &mut zio::Vdevs) -> Result<Vec<u8>, ()> {
        if block_id > self.max_indirect_block_id.try_into().unwrap() { return Err(()); }
        assert!(self.n_indirect_levels >= 1);

        if self.n_indirect_levels == 1 { // There is no indirection
            return Ok(self.block_pointers[block_id].dereference(vdevs)?);
        }

        // If we got here then n_indirect_levels must be 2 or greater

        let mut levels: Vec<IndirectBlockTag> = Vec::new();
        levels.push(self.next_level_id_and_offset(block_id));
        for _ in 1..self.n_indirect_levels-1 {
            levels.push(self.next_level_id_and_offset( levels.last().unwrap().id));
        }

        // Travel back down the levels
        let top_level = levels.pop().unwrap();
        let mut indirect_block_data = self.block_pointers[top_level.id].dereference(vdevs)?;
        let mut next_block_pointer = {
            let mut iter = indirect_block_data.iter().copied();
            iter.skip_n_bytes(BlockPointer::get_ondisk_size()*top_level.offset);
            BlockPointer::from_bytes_le(&mut iter).ok_or(())?
        };

        for _ in 1..self.n_indirect_levels-1 {
            indirect_block_data = next_block_pointer.dereference(vdevs)?;
            let cur_level = levels.pop().unwrap();
            next_block_pointer = {
                let mut iter = indirect_block_data.iter().copied();
                iter.skip_n_bytes(BlockPointer::get_ondisk_size()*cur_level.offset);
                BlockPointer::from_bytes_le(&mut iter).ok_or(())?
            };
        }

        let block_data = next_block_pointer.dereference(vdevs)?;
        assert!(block_data.len() == self.parse_data_block_size());
        Ok(block_data)
    }
    
    // Note: Reading 0 bytes will *always* succeed
    pub fn read(&mut self, offset: usize, size: usize, vdevs: &mut zio::Vdevs) -> Result<Vec<u8>, ()> {
        if size == 0 { return Ok(Vec::new()); }
        let mut result: Vec<u8> = Vec::new();
        let first_data_block_id = offset/self.parse_data_block_size();
        let first_data_block_offset = offset%self.parse_data_block_size();
        let first_data_block = self.read_block(first_data_block_id, vdevs)?;
        result.extend(first_data_block.iter().skip(first_data_block_offset));

        if result.len() >= size {
            result.resize(size, 0);
            return Ok(result);
        }

        let size_remaining = size-result.len();
        let blocks_to_read = if size_remaining%self.parse_data_block_size() == 0 { size_remaining/self.parse_data_block_size() } else { (size_remaining/self.parse_data_block_size())+1 };
        for i in 1..=blocks_to_read {
            result.extend(self.read_block(first_data_block_id+i, vdevs)?);
        }

        result.resize(size, 0);
        assert!(result.len() == size);
        Ok(result)
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
    metadnode: Dnode,
    zil: Option<ZilHeader>,
    typ: ObjSetType
}

impl ObjSet {
    pub fn from_bytes_le<Iter>(data: &mut Iter) -> Option<ObjSet>
    where Iter: Iterator<Item = u8> + Clone {
        let metadnode = Dnode::from_bytes_le(data)?;
        let zil = ZilHeader::from_bytes_le(&mut data.clone());
        data.skip_n_bytes(ZilHeader::get_ondisk_size());

        let typ = ObjSetType::from_value(data.read_u64_le()?.try_into().ok()?)?;
        // Consume padding up to 1k
        let size = metadnode.get_ondisk_size() + ZilHeader::get_ondisk_size() + core::mem::size_of::<u64>();
        let remaining = 1024 - size;
        data.skip_n_bytes(remaining)?;
        Some(ObjSet { 
            metadnode, 
            zil, 
            typ
        })
    }

    pub fn get_dnode_at(&mut self, index: usize, vdevs: &mut Vdevs) -> Option<Dnode> {
        let mut data = self.metadnode.read(index*512, 512, vdevs).ok()?;
        let dnode_slots = Dnode::get_n_slots_from_bytes_le(data.iter().copied())?;
        data.extend(self.metadnode.read((index+1)*512, (dnode_slots-1)*512, vdevs).ok()?.iter());
        Dnode::from_bytes_le(&mut data.iter().copied())
    }

}