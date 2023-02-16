use crate::byte_iter::ByteIter;

#[derive(Debug, PartialEq)]
#[repr(u64)]
enum ZapType {
    MicroZap = (1u64 << 63) + 3,
    FatZapHeader = (1u64 << 63) + 1,
    FatZapLeaf = (1u64 << 63) + 0,
}

impl ZapType {
    pub fn from_value(value: u64) -> Option<ZapType> {
        Some(if value == (1u64 << 63) + 3 {
            Self::MicroZap
        } else if value == (1u64 << 63) + 1 {
            Self::FatZapHeader
        } else if value == (1u64 << 63) + 0 {
            Self::FatZapLeaf
        } else {
            return None;
        })
    }
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum ZapLeafChunkType {
    Entry = 252,
    Array = 251,
    Free = 253,
}

impl ZapLeafChunkType {
    pub fn from_value(value: u8) -> Option<ZapLeafChunkType> {
        Some(match value {
            252 => Self::Entry,
            251 => Self::Array,
            253 => Self::Free,
            _ => return None
        })
    }
}

#[derive(Debug)]
pub struct ZapLeaf {
    header: ZapLeafHeader,
    hash_table: Vec<u16>,
    chunks: Vec<ZapLeafChunk>,
}

impl ZapLeaf {
    fn get_hash_table_numentries(block_size: usize) -> usize {
        // https://github.com/openzfs/zfs/blob/master/include/sys/zap_leaf.h#L77
        block_size/32
    }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>, block_size: usize) -> Option<ZapLeaf> {
        let header = ZapLeafHeader::from_bytes_le(data)?;
        let mut hash_table = vec![0u16; Self::get_hash_table_numentries(block_size)];
        for value in hash_table.iter_mut() {
            *value = data.read_u16_le()?;
        }

        // Calculate length of chunk array
        // https://github.com/openzfs/zfs/blob/master/include/sys/zap_leaf.h#L45
        let remaining_bytes = block_size - ZapLeafHeader::get_ondisk_size() - Self::get_hash_table_numentries(block_size)*core::mem::size_of::<u16>();
        let nchunks = remaining_bytes/ZapLeafChunk::get_ondisk_size();
        let mut chunks = Vec::<ZapLeafChunk>::new();
        for _ in 0..nchunks{
            chunks.push(ZapLeafChunk::from_bytes_le(data)?);
        }
        
        Some(ZapLeaf { header, hash_table, chunks })
    }
}

#[derive(Debug)]
pub struct ZapLeafHeader {
    next_leaf: u64,
    prefix: u64,
    nfree: u16,
    nentries: u16,
    prefix_len: u16,
    freelist: u16
}

impl ZapLeafHeader {
    pub fn get_ondisk_size() -> usize {
        48
    }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<ZapLeafHeader> {
        let zap_type = ZapType::from_value(data.read_u64_le()?)?;
        if zap_type != ZapType::FatZapLeaf { println!("Attempted to parse a zap structure as a leaf, it was not a leaf!"); return None; };
        let next_leaf = data.read_u64_le()?;
        let prefix = data.read_u64_le()?;
        let magic = data.read_u32_le()?;
        assert!(magic == 0x2AB1EAF);
        let nfree = data.read_u16_le()?;
        let nentries = data.read_u16_le()?;
        let prefix_len = data.read_u16_le()?;
        let freelist = data.read_u16_le()?;
        data.skip_n_bytes(12)?;
        Some(ZapLeafHeader { 
            next_leaf, 
            prefix, 
            nfree, 
            nentries, 
            prefix_len, 
            freelist 
        })
    }
}

#[derive(Debug)]
pub enum ZapLeafChunk {
    Entry {
        int_size: u8,
        next_chunk: u16,
        name_chunk: u16,
        name_length: u16,
        value_chunk: u16,
        value_length: u16,
        collision_differentiator: u16,
        hash: u64
    },
    Array{
        array: Vec<u8>,
        next_chunk: u16,
    },
    Free{
        next_chunk: u16
    }
}

impl ZapLeafChunk {
    pub fn get_ondisk_size() -> usize {
        // Source: https://github.com/openzfs/zfs/blob/master/include/sys/zap_leaf.h#L42
        24
    }

    pub fn get_byte_array_size() -> usize {
        // https://github.com/openzfs/zfs/blob/master/include/sys/zap_leaf.h#L62
        Self::get_ondisk_size()-3
    }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<ZapLeafChunk> {
        let chunk_type = ZapLeafChunkType::from_value(data.read_u8()?)?;
        match chunk_type {
            ZapLeafChunkType::Entry => {
                let int_size = data.read_u8()?;
                let next_chunk = data.read_u16_le()?;
                let name_chunk = data.read_u16_le()?;
                let name_length = data.read_u16_le()?;
                let value_chunk = data.read_u16_le()?;
                let value_length = data.read_u16_le()?;
                let collision_differentiator = data.read_u16_le()?;
                data.skip_n_bytes(2)?; // padding
                let hash = data.read_u64_le()?;
                Some(ZapLeafChunk::Entry { 
                    int_size, 
                    next_chunk, 
                    name_chunk, 
                    name_length, 
                    value_chunk, 
                    value_length, 
                    collision_differentiator, 
                    hash 
                })
            },
            ZapLeafChunkType::Array => {
                let mut array = vec![0u8; Self::get_byte_array_size()];
                for byte in array.iter_mut() {
                    *byte = data.read_u8()?;
                }
                let next_chunk = data.read_u16_le()?;
                Some(ZapLeafChunk::Array { array, next_chunk })
            },
            ZapLeafChunkType::Free => {
                data.skip_n_bytes(Self::get_byte_array_size())?;
                let next_chunk = data.read_u16_le()?;
                Some(ZapLeafChunk::Free { next_chunk })
            },
        }
    }
}

#[derive(Debug)]
pub struct ZapPointerTable {
    block_id: u64,
    num_blocks: u64,
    shift: u64,
    next_block: u64,
    blocks_copied: u64
}

impl ZapPointerTable {
    pub fn get_ondisk_size() -> usize {
        core::mem::size_of::<u64>()*5
    }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<ZapPointerTable> {
        Some(ZapPointerTable { 
            block_id: data.read_u64_le()?, 
            num_blocks: data.read_u64_le()?, 
            shift: data.read_u64_le()?, 
            next_block: data.read_u64_le()?, 
            blocks_copied: data.read_u64_le()? 
        })
    }
}

#[derive(Debug)]

pub struct FatZapHeader {
    free_blocks: u64,
    num_leafs: u64,
    num_entries: u64,
    table: ZapPointerTable,
    embbeded_leafs_pointer_table: Vec<u64>
}

impl FatZapHeader {
    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>, block_size: usize) -> Option<FatZapHeader> {
        let zap_magic = data.read_u64_le()?;
        assert!(zap_magic == 0x2F52AB2AB);
        let table = ZapPointerTable::from_bytes_le(data)?;
        let free_blocks = data.read_u64_le()?;
        let num_leafs = data.read_u64_le()?;
        let num_entries = data.read_u64_le()?;
        let _salt = data.read_u64_le()?;
        data.skip_n_bytes(block_size/2-(core::mem::size_of::<u64>()*6+ZapPointerTable::get_ondisk_size()))?;
        let mut embbeded_leafs_pointer_table = vec![0u64; block_size/2/core::mem::size_of::<u64>()];
        for value in embbeded_leafs_pointer_table.iter_mut() {
            *value = data.read_u64_le()?;
        }

        Some(FatZapHeader{
            free_blocks, 
            num_leafs, 
            num_entries, 
            table, 
            embbeded_leafs_pointer_table 
        })
    }

    pub fn read_hash_table_at(&self, index: usize) -> u64 {
        if self.table.block_id == 0 {
            return self.embbeded_leafs_pointer_table[index];
        } else { todo!("Implement non-embedded fat zap tables!"); }
    }
}

#[derive(Debug)]
pub enum ZapHeader {
    FatZap(FatZapHeader),
    MicroZap
}

impl ZapHeader{
    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>, block_size: usize) -> Option<ZapHeader> {
        let zap_type = ZapType::from_value(data.read_u64_le()?)?;
        return match zap_type {
            ZapType::FatZapHeader => {
                FatZapHeader::from_bytes_le(data, block_size)
                .map(|header| ZapHeader::FatZap(header))
            },

            ZapType::MicroZap => {
                data.skip_n_bytes(128-core::mem::size_of::<u64>())?;
                Some(Self::MicroZap)
            },

            ZapType::FatZapLeaf => None
        };
    }

    pub fn unwrap_fat(self) -> FatZapHeader {
        match self {
            Self::FatZap(header) => header,
            _ => panic!("Expected to get a fat zap, got a micro zap!")
        }
    }
}