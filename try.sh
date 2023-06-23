#!/bin/bash

# Basic end to end test, can be fuzzed

rm dva_data.raw
rm dva_data_ours.raw
cargo run --release --bin read-dva -- ./test/vdev0.bin ./test/vdev1.bin ./test/vdev2.bin ./test/vdev3.bin $1 $2
zdb -R chonk 0:$1:$2:r > dva_data.raw
cmp dva_data.raw dva_data_ours.raw
exit $?
