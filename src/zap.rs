use crate::byte_iter::ByteIter;

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

#[derive(Debug)]
pub struct ZapPointerTable {
    blkid: u64,
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
            blkid: data.read_u64_le()?, 
            num_blocks: data.read_u64_le()?, 
            shift: data.read_u64_le()?, 
            next_block: data.read_u64_le()?, 
            blocks_copied: data.read_u64_le()? 
        })
    }
}

#[derive(Debug)]
pub enum ZapHeader {
    FatZap {
        free_blocks: u64,
        num_leafs: u64,
        num_entries: u64,
        table: ZapPointerTable,
        embbeded_leafs_pointer_table: Vec<u64>
    },
    MicroZap
}

impl ZapHeader{
    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>, block_size: usize) -> Option<ZapHeader> {
        let zap_type = ZapType::from_value(data.read_u64_le()?)?;
        return match zap_type {
            ZapType::FatZapHeader => {
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
                Some(Self::FatZap { 
                    free_blocks, 
                    num_leafs, 
                    num_entries, 
                    table, 
                    embbeded_leafs_pointer_table 
                })
            },

            ZapType::MicroZap => {
                data.skip_n_bytes(128-core::mem::size_of::<u64>())?;
                Some(Self::MicroZap)
            },

            ZapType::FatZapLeaf => None
        };
    }
}