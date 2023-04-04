pub trait ByteIter {
    fn read_u8(&mut self) -> Option<u8>;
    fn read_i16_be(&mut self) -> Option<i16>;
    fn read_i16_le(&mut self) -> Option<i16>;
    fn read_i32_be(&mut self) -> Option<i32>;
    fn read_i32_le(&mut self) -> Option<i32>;
    fn read_i64_be(&mut self) -> Option<i64>;
    fn read_i64_le(&mut self) -> Option<i64>;

    fn read_u16_be(&mut self) -> Option<u16>;
    fn read_u16_le(&mut self) -> Option<u16>;
    fn read_u32_be(&mut self) -> Option<u32>;
    fn read_u32_le(&mut self) -> Option<u32>;
    fn read_u64_be(&mut self) -> Option<u64>;
    fn read_u64_le(&mut self) -> Option<u64>;

    #[must_use]
    fn skip_n_bytes(&mut self, n_bytes: usize) -> Option<()>;
}

impl<T> ByteIter for T
where
    T: Iterator<Item = u8>,
{
    fn skip_n_bytes(&mut self, n_bytes: usize) -> Option<()> {
        if n_bytes > 0 {
            self.nth(n_bytes-1)?;
        }    
        Some(())
    }

    fn read_u8(&mut self) -> Option<u8> {
        self.next()
    }

    fn read_i16_be(&mut self) -> Option<i16> {
        Some(i16::from_be_bytes([self.next()?, self.next()?]))
    }

    fn read_u16_be(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes([self.next()?, self.next()?]))
    }

    fn read_i32_be(&mut self) -> Option<i32> {
        Some(i32::from_be_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }

    fn read_u32_be(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }

    fn read_i64_be(&mut self) -> Option<i64> {
        Some(i64::from_be_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }

    fn read_u64_be(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }

    fn read_i16_le(&mut self) -> Option<i16> {
        Some(i16::from_le_bytes([self.next()?, self.next()?]))
    }

    fn read_u16_le(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes([self.next()?, self.next()?]))
    }

    fn read_i32_le(&mut self) -> Option<i32> {
        Some(i32::from_le_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }

    fn read_u32_le(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }

    fn read_i64_le(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }

    fn read_u64_le(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes([
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
            self.next()?,
        ]))
    }
}
