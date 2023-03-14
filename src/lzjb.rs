// Source: https://github.com/openzfs/zfs/blob/master/module/zfs/lzjb.c
pub const MATCH_BITS: usize = 6;
pub const MATCH_MIN: usize = 3;
pub const OFFSET_MASK: usize = (1 << (16 - MATCH_BITS)) - 1;

pub fn lzjb_decompress(data: &mut impl Iterator<Item = u8>, output_length: usize) -> Result<Vec<u8>, ()> {
    let mut copymap: u8 = 0;
    let mut copymask: usize = 1 << 7;
    let mut output_buf = Vec::new();

    while output_buf.len() < output_length {
        copymask <<= 1;
        if copymask == (1 << 8) {
            copymask = 1;
            copymap = data.next().ok_or(())?;
        }

        if copymap & (copymask as u8) != 0 {
            let byte0 = data.next().ok_or(())?;
            let byte1 = data.next().ok_or(())?;
            let lookback_size = usize::from(byte0 >> (8-MATCH_BITS)) + MATCH_MIN;
            let lookback = ((((byte0 as u16) << 8) | (byte1 as u16)) as usize) & OFFSET_MASK;
            if lookback > output_buf.len() || lookback == 0 {
                return Err(());
            }
            let mut lookback_pos = output_buf.len() - lookback;
            for _ in 0..lookback_size {
                if output_buf.len() >= output_length { break; }
                output_buf.push(output_buf[lookback_pos]);
                lookback_pos += 1;
            }
        } else {
            output_buf.push(data.next().ok_or(())?);
        }
    }
    Ok(output_buf)
}