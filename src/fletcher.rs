pub fn do_fletcher4(data: &[u8]) -> [u64; 4] {
    let (mut s1, mut s2, mut s3, mut s4): (u64, u64, u64, u64) = (0, 0, 0, 0);
    for block in data.chunks_exact(core::mem::size_of::<u32>()){
        let n = u32::from_le_bytes(block.try_into().unwrap());
        s1 += u64::from(n);
        s2 = s2.wrapping_add(s1);
        s3 = s3.wrapping_add(s2);
        s4 = s4.wrapping_add(s3);
    }
    [s1, s2, s3, s4]
}