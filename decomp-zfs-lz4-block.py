#!/usr/bin/python
import lz4.block
import os
import struct
import sys
with open(sys.argv[1], 'rb') as f:
    input = f.read()
input_len_bytes = input[:4]
input_len = struct.unpack('>I', input_len_bytes)[0]
input_no_len = input[4:][:input_len]
#Note: Usually the uncompressed_size is the data block size, or 128 kb = 131072 bytes by default
# This is important as lz4 WILL fail if not given the right output size
output = lz4.block.decompress(input_no_len, uncompressed_size=int(sys.argv[2]))
sys.stdout.buffer.write(output)