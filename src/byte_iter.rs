pub trait FromBytesLE<It>
where
    Self: Sized,
    It: Iterator<Item = u8>,
{
    fn from_bytes_le(it: &mut It) -> Option<Self>;
}

pub trait FromBytesBE<It>
where
    Self: Sized,
    It: Iterator<Item = u8>,
{
    fn from_bytes_be(it: &mut It) -> Option<Self>;
}

pub trait FromBytes<It>
where
    Self: Sized,
    It: Iterator<Item = u8>,
{
    fn from_bytes(it: &mut It) -> Option<Self>;
}

impl<It> FromBytes<It> for u8
where
    It: Iterator<Item = u8>,
{
    fn from_bytes(it: &mut It) -> Option<Self> {
        it.next()
    }
}

macro_rules! impl_from_bytes_be_for {
    ($name: ident) => {
        impl<It> FromBytesBE<It> for $name
        where
            It: Iterator<Item = u8>,
        {
            fn from_bytes_be(it: &mut It) -> Option<Self> {
                let mut buf = [0u8; core::mem::size_of::<Self>()];
                for byte in buf.iter_mut() {
                    *byte = it.next()?;
                }
                Some(Self::from_be_bytes(buf))
            }
        }
    };
}

macro_rules! impl_from_bytes_le_for {
    ($name: ident) => {
        impl<It> FromBytesLE<It> for $name
        where
            It: Iterator<Item = u8>,
        {
            fn from_bytes_le(it: &mut It) -> Option<Self> {
                let mut buf = [0u8; core::mem::size_of::<Self>()];
                for b in buf.iter_mut() {
                    *b = it.next()?;
                }
                Some(Self::from_le_bytes(buf))
            }
        }
    };
}

impl_from_bytes_be_for!(i16);
impl_from_bytes_be_for!(u16);
impl_from_bytes_be_for!(i32);
impl_from_bytes_be_for!(u32);
impl_from_bytes_be_for!(i64);
impl_from_bytes_be_for!(u64);

impl_from_bytes_le_for!(i16);
impl_from_bytes_le_for!(u16);
impl_from_bytes_le_for!(i32);
impl_from_bytes_le_for!(u32);
impl_from_bytes_le_for!(i64);
impl_from_bytes_le_for!(u64);

pub trait ByteIter {
    #[must_use]
    fn skip_n_bytes(&mut self, n_bytes: usize) -> Option<()>;
}

impl<T> ByteIter for T
where
    T: Iterator<Item = u8>,
{
    fn skip_n_bytes(&mut self, n_bytes: usize) -> Option<()> {
        if n_bytes > 0 {
            self.nth(n_bytes - 1)?;
        }

        Some(())
    }
}
