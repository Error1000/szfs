#!/usr/bin/python
import struct
import sys

def fletcher4(data):
    # Reference: https://github.com/openzfs/zfs/blob/master/module/zcommon/zfs_fletcher.c#L323
    (a, b, c, d) = (0, 0, 0, 0)
    for i in range(0, len(data), 4):
        if len(data[i:i+4]) < 4: break
        value = struct.unpack('<I', data[i:i+4])[0]
        a += value
        a %= 2**64

        b += a
        b %= 2**64

        c += b
        c %= 2**64

        d += c
        d %= 2**64
    return (a, b, c, d)
with open(sys.argv[1], 'rb') as f:
    input = f.read()
print(fletcher4(input))