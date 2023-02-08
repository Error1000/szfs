// Sources:
// https://github.com/nkchenz/zfspy/blob/master/zfspy/nvpair.py#L189
// https://cgit.freebsd.org/src/commit/?id=2d9cf57e18654edda53bcb460ca66641ba69ed75 (nvlist_check_features_for_read)
// https://medium.com/@tedsta/xdr-encoded-nvpairs-in-rust-aa56173f5e74
// https://github.com/openzfs/zfs/blob/master/module/nvpair/nvpair.c#L3608 (nvs_xdr_nvpair)
// https://github.com/openzfs/zfs/blob/master/module/nvpair/nvpair.c#L3200 (nvs_xdr_nvlist)
// https://github.com/openzfs/zfs/blob/master/module/nvpair/nvpair.c#L3291
// https://github.com/nkchenz/zfspy/blob/master/zfspy/nvpair.py#L137

use std::collections::HashMap;
use std::fmt::Debug;

use crate::byte_iter::ByteIter;

pub type Name = String;

enum ValueType {
    Boolean = 1,
    Byte = 2, // char in c
    I16 = 3,
    U16 = 4,
    I32 = 5,
    U32 = 6,
    I64 = 7,
    U64 = 8,
    String = 9,
    ByteArray = 10, // char* in c
    I16Array = 11,
    U16Array = 12,
    I32Array = 13,
    U32Array = 14, 
    I64Array = 15,
    U64Array = 16, 
    StringArray = 17,
    HRTime = 18,
    NVList = 19,
    NVListArray = 20,
    BooleanValue = 21,
    I8 = 22,
    U8 = 23,
    BooleanArray = 24,
    I8Array = 25,
    U8Array = 26
}

impl ValueType {
    pub fn from_value(val: u32) -> Option<ValueType> {
        Some(match val {
            1 => ValueType::Boolean,
            2 => ValueType::Byte,
            3 => ValueType::I16,
            4 => ValueType::U16,
            5 => ValueType::I32,
            6 => ValueType::U32,
            7 => ValueType::I64,
            8 => ValueType::U64,
            9 => ValueType::String,
            10 => ValueType::ByteArray,
            11 => ValueType::I16Array,
            12 => ValueType::U16Array,
            13 => ValueType::I32Array,
            14 => ValueType::U32Array,
            15 => ValueType::I64Array,
            16 => ValueType::U64Array,
            17 => ValueType::StringArray,
            18 => ValueType::HRTime,
            19 => ValueType::NVList,
            20 => ValueType::NVListArray,
            21 => ValueType::BooleanValue,
            22 => ValueType::I8,
            23 => ValueType::U8,
            24 => ValueType::BooleanArray,
            25 => ValueType::I8Array,
            26 => ValueType::U8Array,
            _ => return None
        })
    }

}

pub enum Value {
    Unknown,
    Boolean(bool),
    Byte(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    String(String),
    NVList(NVList)
}

impl TryInto<NVList> for Value {
    type Error = ();

    fn try_into(self) -> Result<NVList, Self::Error> {
        match self {
            Self::NVList(val) => Ok(val),
            _ => Err(())
        }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "null"),
            Self::Boolean(arg0) => write!(f, "{:?}", arg0),
            Self::Byte(arg0) => write!(f, "{:?}", arg0),
            Self::I16(arg0) => write!(f, "{:?}", arg0),
            Self::U16(arg0) => write!(f, "{:?}", arg0),
            Self::I32(arg0) => write!(f, "{:?}", arg0),
            Self::U32(arg0) => write!(f, "{:?}", arg0),
            Self::I64(arg0) => write!(f, "{:?}", arg0),
            Self::U64(arg0) => write!(f, "{:?}", arg0),
            Self::String(arg0) => write!(f, "{:?}", arg0),
            Self::NVList(arg0) => write!(f, "{:?}", arg0),
        }
    }
}

pub type NVList = HashMap<Name, Value>;


fn read_string_raw(data: &mut impl Iterator<Item = u8>, size: usize) -> Option<String> {
    let result: Vec<u8> = data.take(size).collect();
    if result.len() != size { return None; }
    String::from_utf8(result).ok()
}

// Returns: The string and the amount of bytes read including the bytes of the size
fn read_string_and_size(data: &mut impl Iterator<Item = u8>) -> Option<(String, usize)> {
    let result_size = data.read_u32_be()?;
    let result_size_aligned = if result_size % 4 == 0 { result_size } else { ((result_size/4)+1)*4 };
    let result = read_string_raw(data, result_size as usize);
    let padding_bytes = result_size_aligned - result_size;
    if padding_bytes > 0 {
        let _ = data.skip_n_bytes(padding_bytes as usize)?; // Consume the padding bytes
    }
    result.map(|res|(res, result_size_aligned as usize+4))
}


pub fn from_bytes_xdr(data: &mut impl Iterator<Item = u8>) -> Option<NVList> {
    // first byte is the encoding, second byte is the endianness, and the last two are reserved
    let xdr_encoding = data.next()?; 
    let xdr_endian = data.next()?;
    let _ = data.skip_n_bytes(2); // Consume reserved bytes
    // println!("NVList xdr encoding: {}, xdr endianness: {}", xdr_encoding, xdr_endian);
    if xdr_endian != 1 || xdr_encoding != 1 { 
        println!("Expected xdr encoding 1, and endian 1 (a.k.a big-endian)!");
        return None; 
    }
    from_bytes(data, 0)
}

// TODO: 
// 1. Support arrays as values and other esoteric value types
// 2. Support writing nvlists

fn from_bytes(data: &mut impl Iterator<Item = u8>, recursion_depth: usize) -> Option<NVList> {
    if recursion_depth >= 128 {
        println!("NVList recursion limit of 128 nvlists nested in the main nvlist reached, i will not be parsing any more, deal with it!");
        return None;
    }

    let mut nv_list: NVList = NVList::new();
    
    let _nvl_version = data.read_u32_be()?;
    let _nvl_flag = data.read_u32_be()?;

    // Parse pairs
    loop { 
        let encode_size = data.read_u32_be()?;
        let decode_size = data.read_u32_be()?;
        if encode_size == 0 && decode_size == 0 { break; } // The nv_list has 8 bytes of zeroes at the end

        // decode_size = 4(for the size of the size itself) + 4(size of string) + size of string with padding + 4(size of value type) + 4(size of the number of values) + n(size of value(s))
        let (name, bytes_read) = read_string_and_size(data)?;

        let Some(value_type) = ValueType::from_value(data.read_u32_be()?) else {
            println!("Unknown nvlist value type with name: \"{}\", skipping entry, which was {} bytes in size!", name, decode_size);
            let value_size = decode_size-(
                bytes_read as u32
                +4 /*size of decode_size*/
                +4 /*size of value_type*/
            );
            let _ = data.skip_n_bytes(value_size as usize)?; // Consume value bytes

            continue;
        };

        let nvalues = data.read_u32_be()?;

        if nvalues == 0 { 
            nv_list.insert(name, Value::Unknown);
            continue;
        }

        let nvpair_name_repeated = || {
            panic!("NVPair Name was repeated, this is not supported!");
        };

        match value_type {
            ValueType::Boolean => {
                let value = data.read_u8()?;
                if nv_list.insert(name, Value::Boolean(value != 0)).is_some() {nvpair_name_repeated()}
            },
            ValueType::Byte => { if nv_list.insert(name, Value::Byte(data.read_u8()?)).is_some() {nvpair_name_repeated()} },
            ValueType::I16  => { if nv_list.insert(name, Value::I16(data.read_i16_be()?)).is_some() {nvpair_name_repeated()} },
            ValueType::U16  => { if nv_list.insert(name, Value::U16(data.read_u16_be()?)).is_some() {nvpair_name_repeated()} },
            ValueType::I32  => { if nv_list.insert(name, Value::I32(data.read_i32_be()?)).is_some() {nvpair_name_repeated()} },
            ValueType::U32  => { if nv_list.insert(name, Value::U32(data.read_u32_be()?)).is_some() {nvpair_name_repeated()} },
            ValueType::I64  => { if nv_list.insert(name, Value::I64(data.read_i64_be()?)).is_some() {nvpair_name_repeated()} },
            ValueType::U64  => { if nv_list.insert(name, Value::U64(data.read_u64_be()?)).is_some() {nvpair_name_repeated()} },
            ValueType::String => {
               let (value, _) = read_string_and_size(data)?;
               nv_list.insert(name, Value::String(value));
            },
            ValueType::ByteArray => todo!(),
            ValueType::I16Array => todo!(),
            ValueType::U16Array => todo!(),
            ValueType::I32Array => todo!(),
            ValueType::U32Array => todo!(),
            ValueType::I64Array => todo!(),
            ValueType::U64Array => todo!(),
            ValueType::StringArray => todo!(),
            ValueType::HRTime => todo!(),
            ValueType::NVList => { if nv_list.insert(name, Value::NVList(from_bytes(data, recursion_depth+1)?)).is_some() {nvpair_name_repeated()} },
            ValueType::NVListArray => todo!(),
            ValueType::BooleanValue => todo!(),
            ValueType::I8 => todo!(),
            ValueType::U8 => todo!(),
            ValueType::BooleanArray => todo!(),
            ValueType::I8Array => todo!(),
            ValueType::U8Array => todo!(),
        }
    }
    Some(nv_list)
}