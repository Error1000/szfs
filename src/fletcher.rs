pub fn do_fletcher4(data: &[u8]) -> [u64; 4] {
    let (mut s1, mut s2, mut s3, mut s4): (u64, u64, u64, u64) = (0, 0, 0, 0);
    // zfs ignores partial chunks due to the ipend calculation using flooring division
    // https://github.com/openzfs/zfs/blob/master/module/zcommon/zfs_fletcher.c#L323
    for block in data.chunks_exact(core::mem::size_of::<u32>()) {
        let n = u32::from_le_bytes(block.try_into().unwrap()); // unwrap won't fail thanks to chunks_exact
        s1 = s1.wrapping_add(u64::from(n));
        s2 = s2.wrapping_add(s1);
        s3 = s3.wrapping_add(s2);
        s4 = s4.wrapping_add(s3);
    }
    [s1, s2, s3, s4]
}

pub fn do_fletcher2(data: &[u8]) -> [u64; 4] {
    let (mut s1, mut s2, mut s3, mut s4): (u64, u64, u64, u64) = (0, 0, 0, 0);
    // zfs ignores partial chunks due to the ipend calculation
    // https://github.com/openzfs/zfs/blob/master/module/zcommon/zfs_fletcher.c#L236
    let mut blocks = data.chunks_exact(core::mem::size_of::<u64>());
    loop {
        let (Some(block0), Some(block1)) = (blocks.next(), blocks.next()) else { break; };
        let n0 = u64::from_le_bytes(block0.try_into().unwrap()); // unwrap won't fail thanks to chunks_exact
        let n1 = u64::from_le_bytes(block1.try_into().unwrap()); // unwrap won't fail thanks to chunks_exact
        s1 = s1.wrapping_add(n0);
        s2 = s2.wrapping_add(n1);
        s3 = s3.wrapping_add(s1);
        s4 = s4.wrapping_add(s2);
    }
    [s1, s2, s3, s4]
}
