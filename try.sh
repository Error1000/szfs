#!/bin/bash
rm dva_data.raw
rm dva_data_ours.raw
cargo run --release --bin undelete -- $1 $2
zdb -R chonk 0:$1:$2:r > dva_data.raw
cmp dva_data.raw dva_data_ours.raw
exit $?
