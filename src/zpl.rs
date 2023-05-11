use std::collections::HashMap;

use crate::{
    byte_iter::ByteIter,
    dmu::{DNode, ObjSet},
    zap,
    zio::Vdevs,
    zpl,
};
use std::fmt::Debug;

// https://github.com/openzfs/zfs/blob/master/module/zfs/sa.c#L49

#[derive(Debug)]
pub struct SystemAttributesRegistration {
    attribute_id: u16,
    bswap: u8,
    len: u16,
}

impl SystemAttributesRegistration {
    pub fn from_value(value: u64) -> SystemAttributesRegistration {
        SystemAttributesRegistration {
            attribute_id: ((value >> 0) & 0xFFFF) as u16,
            bswap: ((value >> 16) & 0xFF) as u8,
            len: ((value >> 24) & 0xFFFF) as u16,
        }
    }
}

#[derive(Debug)]
pub struct SystemAttributesHeader {
    layout_id: u16,
    lengths: Vec<u16>,
}

pub const SYSTEM_ATTRIBUTES_MAGIC: u32 = 0x2F505A;

impl SystemAttributesHeader {
    pub fn get_ondisk_size(&self) -> usize {
        core::mem::size_of::<u32>()
            + core::mem::size_of::<u16>()
            + self.lengths.len() * core::mem::size_of::<u16>()
    }

    pub fn from_bytes_le(data: &mut impl Iterator<Item = u8>) -> Option<SystemAttributesHeader> {
        let magic = data.read_u32_le()?;
        if magic != SYSTEM_ATTRIBUTES_MAGIC {
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Tried to parse a system attributes header with invalid magic!");
            return None;
        }

        let layout_info = data.read_u16_le()?;
        let mut header_size = (layout_info >> 10) & 0b1111_11;
        header_size *= 8;

        if header_size == 0 {
            use crate::ansi_color::*;
            println!("{YELLOW}Warning{WHITE}: Tried to parse a system attributes header with invalid size!");
            return None;
        }

        let layout_id = (layout_info >> 0) & 0b11_1111_1111;
        let mut nlengths =
            usize::from(header_size) - (core::mem::size_of::<u32>() + core::mem::size_of::<u16>());
        nlengths /= core::mem::size_of::<u16>();
        let mut lengths = Vec::new();
        for _ in 0..nlengths {
            lengths.push(data.read_u16_le()?);
        }
        Some(SystemAttributesHeader { layout_id, lengths })
    }
}

pub enum Value {
    U64(u64),
    U64Array(Vec<u64>),
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U64(arg0) => write!(f, "{:?}", arg0),
            Self::U64Array(arg0) => write!(f, "{:?}", arg0),
        }
    }
}

pub struct SystemAttribute {
    name: String,
    byteswap_function: u8,
    len: u16,
}

impl Debug for SystemAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{\"{}\", bswap: {}, len: {}}}",
            self.name, self.byteswap_function, self.len
        )
    }
}

#[derive(Debug)]
pub struct SystemAttributes {
    layouts: HashMap<usize, Vec<u16>>,
    attributes: HashMap<u16, SystemAttribute>,
}

impl SystemAttributes {
    pub fn from_attributes_node_number(
        system_attributes_info_number: usize,
        dataset_object_set: &mut ObjSet,
        vdevs: &mut Vdevs,
    ) -> Option<SystemAttributes> {
        use crate::ansi_color::*;

        let DNode::SystemAttributesMasterNode(mut sa_info) = dataset_object_set.get_dnode_at(system_attributes_info_number, vdevs)? else {
            println!("{YELLOW}Warning{WHITE}: System attributes master node is of the wrong type!");
            return None;
        };

        let sa_info_zap_data = sa_info.dump_zap_contents(vdevs)?;
        println!(
            "{CYAN}Info{WHITE}: System attributes master node zap: {:?}",
            sa_info_zap_data
        );

        let mut system_attributes_layouts_zap_data = {
            let zap::Value::U64(system_attributes_layouts_number) = sa_info_zap_data["LAYOUTS"] else {
                println!("{YELLOW}Warning{WHITE}: System attributes layouts node number is not a number!");
                return None;
            };

            let DNode::SystemAttributesLayouts(mut system_attributes_layouts) = dataset_object_set.get_dnode_at(system_attributes_layouts_number as usize, vdevs)? else {
                println!("{YELLOW}Warning{WHITE}: System attributes layouts node is of the wrong type!");
                return None;
            };

            system_attributes_layouts
                .dump_zap_contents(vdevs)?
                .into_iter()
                .map(|(key, value)| {
                    let zap::Value::U16Array(value) = value else {
                    panic!("Layout is not of the right type (a u16 array) in the zap data!");
                };
                    (str::parse(&key).unwrap(), value)
                })
                .collect::<HashMap<usize, Vec<u16>>>()
        };

        // Legacy layout
        system_attributes_layouts_zap_data.insert(
            0,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        );

        let system_attributes_registrations = {
            let zap::Value::U64(system_attributes_registrations_number) = sa_info_zap_data["REGISTRY"] else {
                panic!("System attributes registrations node number is not a number!");
            };

            let DNode::SystemAttributesRegistrations(mut system_attributes_registrations) = dataset_object_set.get_dnode_at(system_attributes_registrations_number as usize, vdevs).unwrap() else {
                panic!("System attributes registrations node is of the wrong type!");
            };

            system_attributes_registrations
            .dump_zap_contents(vdevs)?
            .into_iter()
            .map(|(key, value)| {
                let zap::Value::U64(val) = value else { panic!("System attributes registration is invalid!"); };
                let registration = zpl::SystemAttributesRegistration::from_value(val);
                (registration.attribute_id, SystemAttribute{
                    name: key,
                    byteswap_function: registration.bswap,
                    len: registration.len,
                })
            })
            .collect::<HashMap<u16, SystemAttribute>>()
        };

        Some(SystemAttributes {
            layouts: system_attributes_layouts_zap_data,
            attributes: system_attributes_registrations,
        })
    }

    pub fn parse_system_attributes_bytes_le(
        &mut self,
        data: &mut impl Iterator<Item = u8>,
    ) -> Option<HashMap<String, Value>> {
        let system_attributes_header = zpl::SystemAttributesHeader::from_bytes_le(data)?;
        let layout = &self.layouts[&system_attributes_header.layout_id.into()];
        let mut attributes: HashMap<String, Value> = HashMap::new();

        use crate::ansi_color::*;
        for (attribute_index, attribute_id) in layout.iter().enumerate() {
            let attribute_info = &self.attributes[attribute_id];
            match attribute_info.name.as_str() {
                // All of these are u64 array or single u64 system attributes with known sizes
                "ZPL_ATIME" | "ZPL_MTIME" | "ZPL_CTIME" | "ZPL_CRTIME" | "ZPL_GEN" | "ZPL_MODE"
                | "ZPL_SIZE" | "ZPL_PARENT" | "ZPL_LINKS" | "ZPL_XATTR" | "ZPL_RDEV"
                | "ZPL_FLAGS" | "ZPL_UID" | "ZPL_GID" | "ZPL_PAD" | "ZPL_DACL_COUNT"
                | "ZPL_PROJID" => {
                    if attribute_info.len == 0 {
                        panic!("System Attribute \"{}\" does not have a variable size according to the zfs source code (the scond column contains the size of the attribute in bytes, it's 0 for variable size): (https://github.com/openzfs/zfs/blob/master/module/zfs/zfs_sa.c#L34), but was read from disk as having a variable size!", attribute_info.name);
                    }
                    if attribute_info.byteswap_function != 0 {
                        println!("{YELLOW}Warning{WHITE}: Unsupported byte swap function on attribute \"{}\", ignoring!", attribute_info.name);
                        // NOTE: If it's the last attribute, even if we don't know how much to skip, it doesn't matter
                        if attribute_info.len == 0 && attribute_index != layout.len() - 1 {
                            panic!("Unsupported system attribute \"{}\" has variable size, can't ignore it if we don't know how much to ignore!", attribute_info.name);
                        }

                        data.skip_n_bytes(attribute_info.len as usize)?;
                        continue;
                    }

                    let nvalues = attribute_info.len / 8;
                    if nvalues == 1 {
                        let attribute_value = data.read_u64_le()?;
                        attributes.insert(attribute_info.name.clone(), Value::U64(attribute_value));
                    } else {
                        let mut attribute_values = Vec::<u64>::new();
                        for _ in 0..nvalues {
                            attribute_values.push(data.read_u64_le()?);
                        }
                        attributes.insert(
                            attribute_info.name.clone(),
                            Value::U64Array(attribute_values),
                        );
                    }
                }

                _ => {
                    println!(
                        "{YELLOW}Warning{WHITE}: Unsupported system attribute \"{}\", ignoring!",
                        attribute_info.name
                    );
                    // NOTE: If it's the last attribute, even if we don't know how much to skip, it doesn't matter
                    if attribute_info.len == 0 && attribute_index != layout.len() - 1 {
                        panic!("Unsupported system attribute \"{}\" has variable size, can't ignore it if we don't know how much to ignore!", attribute_info.name);
                    }
                    data.skip_n_bytes(attribute_info.len as usize)?;
                }
            }
        }

        Some(attributes)
    }
}
