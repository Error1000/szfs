use std::{
    env,
    fs::{File, OpenOptions},
    os::unix::prelude::FileExt,
};

fn main() {
    let usage = format!(
        "Usage: {} (target path) (patch path)",
        env::args().next().unwrap()
    );

    let target = OpenOptions::new()
        .write(true)
        .create(false)
        .open(env::args().nth(1).expect(&usage))
        .unwrap();
    let patch = File::open(env::args().nth(2).expect(&usage)).unwrap();
    let patch_size = patch.metadata().unwrap().len();
    let mut patch_offset = 0;
    let mut buf;
    let mut data_buf = Vec::new();
    let mut last_log_offset = 0;
    while patch_offset < patch_size {
        if patch_offset - last_log_offset > 512 * 1024 * 1024 {
            // Every ~512 mb
            println!(
                "{}% done ...",
                (patch_offset as f32 / patch_size as f32) * 100.0
            );
            last_log_offset = patch_offset;
        }
        buf = [0u8; core::mem::size_of::<u64>()];
        patch.read_exact_at(&mut buf, patch_offset).unwrap();
        let target_offset = u64::from_le_bytes(buf);
        patch_offset += u64::try_from(core::mem::size_of::<u64>()).unwrap();

        buf = [0u8; core::mem::size_of::<u64>()];
        patch.read_exact_at(&mut buf, patch_offset).unwrap();
        let amount_to_copy = usize::try_from(u64::from_le_bytes(buf)).unwrap();
        patch_offset += u64::try_from(core::mem::size_of::<u64>()).unwrap();

        data_buf.clear();
        data_buf.resize(amount_to_copy, 0);
        patch.read_exact_at(&mut data_buf, patch_offset).unwrap();
        patch_offset += u64::try_from(amount_to_copy).unwrap();

        target.write_all_at(&data_buf, target_offset).unwrap();
    }
}
