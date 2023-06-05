use serde::{Deserialize, Serialize};

use crate::byte_iter::{ByteIter, FromBytesLE};
use crate::zio::BlockPointer;

#[derive(Debug, Serialize, Deserialize)]
pub struct ZilHeader {
    claim_txg: u64,
    highest_replayed_seq_number: u64,
    log: BlockPointer,
}
impl<It> FromBytesLE<It> for ZilHeader
where
    It: Iterator<Item = u8> + Clone,
{
    fn from_bytes_le(data: &mut It) -> Option<ZilHeader> {
        let res = ZilHeader {
            claim_txg: u64::from_bytes_le(data)?,
            highest_replayed_seq_number: u64::from_bytes_le(data)?,
            log: BlockPointer::from_bytes_le(data)?,
        };
        data.skip_n_bytes(core::mem::size_of::<u64>() * 6)?;
        Some(res)
    }
}

impl ZilHeader {
    pub const fn get_ondisk_size() -> usize {
        BlockPointer::get_ondisk_size() + 8 * core::mem::size_of::<u64>()
    }
}
