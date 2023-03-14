use crate::byte_iter::ByteIter;

// Warning: The size of input is relevant as the lz4 format may not be able to figure out when the stream ends
// due to 00 00 00 being a valid block that means copy the last byte 4 times
pub fn lz4_decompress_blocks(data: &mut impl Iterator<Item = u8>) -> Result<Vec<u8>, ()> {
    let mut output_buf = Vec::new();
    loop {
        let token = data.next().ok_or(())?;
        let mut literal_size: usize = ((token & 0xF0) >> 4).into();
        let mut lookback_size: usize = ((token & 0x0F) >> 0).into();
        // Handle extended literal sizes
        if literal_size == 0xF {
            loop{
                let extended_size: usize = data.next().ok_or(())?.into();
                literal_size += extended_size;
                if extended_size != 0xFF { break; }
            }
        }

        // Copy literal_size bytes to output_buf
        for _ in 0..literal_size {
            output_buf.push(data.next().ok_or(())?);
        }

        let Ok(lookback) = data.read_u16_le().ok_or(()) else {
            if lookback_size == 0 {
                // Reached end of all lz4 blocks
                // This is not an error
                break;
            }else{
                // Stream ended abruptly, since the lookback_size was not 0 this could not have been the last block
                // so it must have a lookback, but we couldn't read it because the stream ended
                return Err(());
            }
        };

        if lookback as usize > output_buf.len() || lookback == 0 {
            // Invalid lz4 block
            return Err(());
        }

        // Handle extended lookback sizes
        if lookback_size == 0xF {
            loop {
                let extended_size: usize = data.next().ok_or(())?.into();
                lookback_size += extended_size;
                if extended_size != 0xFF { break; }
            }
        }

        lookback_size += 4;


        // Repeat lookback_size bytes from lookback bytes ago
        // Note: Yes this can copy more bytes than the lookback because the buffer will grow while we are reading it
        // Ex. lookback_size = 4, lookback = 1, output_buf = [0]
        // will result in output_buf = [0, 0, 0, 0, 0]
        let mut lookback_pos = output_buf.len() /* end */ - usize::from(lookback);
        for _ in 0..lookback_size {
            output_buf.push(output_buf[lookback_pos]);
            lookback_pos += 1;
        }
    }
    Ok(output_buf)
}