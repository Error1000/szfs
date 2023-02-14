use crate::zio::BlockPointer;
use crate::byte_iter::ByteIter;

#[derive(Debug)]
pub struct ZilHeader {
    claim_txg: u64,
    highest_replayed_seq_number: u64,
    log: BlockPointer
}

impl ZilHeader {
    pub fn get_ondisk_size() -> usize { BlockPointer::get_ondisk_size()+8*core::mem::size_of::<u64>() }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<ZilHeader> {
        let res = ZilHeader {
            claim_txg: data.read_u64_le()?,
            highest_replayed_seq_number: data.read_u64_le()?,
            log: BlockPointer::from_bytes_le(data)?
        };
        data.skip_n_bytes(core::mem::size_of::<u64>()*6)?;
        Some(res)
    }
}