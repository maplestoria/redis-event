use std::io::{Cursor, Read, Result};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::{CommandHandler, RdbHandler, to_string};
use crate::rdb::Data::{Bytes, Empty};
use crate::io::Conn;

// 读取、解析rdb
pub(crate) fn parse(input: &mut Conn,
                    _: isize,
                    rdb_handlers: &mut dyn RdbHandler,
                    _: &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    rdb_handlers.handle(Object::BOR);
    let mut bytes = vec![0; 5];
    // 开头5个字节: REDIS
    input.read_exact(&mut bytes)?;
    // 4个字节: rdb版本
    input.read_exact(&mut bytes[..=3])?;
    let rdb_version = String::from_utf8_lossy(&bytes[..=3]);
    let rdb_version = rdb_version.parse::<isize>().unwrap();
    
    let mut meta = Meta {
        db: 0,
        expired_type: None,
        expired_time: None,
    };
    
    loop {
        let data_type = input.read_u8()?;
        match data_type {
            RDB_OPCODE_AUX => {
                let field_name = input.read_string()?;
                let field_val = input.read_string()?;
                let field_name = to_string(field_name);
                let field_val = to_string(field_val);
                println!("{}:{}", field_name, field_val);
            }
            RDB_OPCODE_SELECTDB => {
                let (db, _) = input.read_length()?;
                meta.db = db;
            }
            RDB_OPCODE_RESIZEDB => {
                let (db, _) = input.read_length()?;
                println!("db total keys: {}", db);
                let (db, _) = input.read_length()?;
                println!("db expired keys: {}", db);
            }
            RDB_OPCODE_EXPIRETIME | RDB_OPCODE_EXPIRETIME_MS => {
                if data_type == RDB_OPCODE_EXPIRETIME_MS {
                    let expired_time = input.read_integer(8, false)?;
                    meta.expired_time = Option::Some(expired_time as i64);
                    meta.expired_type = Option::Some(ExpireType::Millisecond);
                } else {
                    let expired_time = input.read_integer(4, false)?;
                    meta.expired_time = Option::Some(expired_time as i64);
                    meta.expired_type = Option::Some(ExpireType::Second);
                }
                let value_type = input.read_u8()?;
                match value_type {
                    RDB_OPCODE_FREQ => {
                        input.read_u8()?;
                        let value_type = input.read_u8()?;
                        input.read_object(value_type, rdb_handlers, &meta)?;
                    }
                    RDB_OPCODE_IDLE => {
                        input.read_length()?;
                        let value_type = input.read_u8()?;
                        input.read_object(value_type, rdb_handlers, &meta)?;
                    }
                    _ => {
                        input.read_object(value_type, rdb_handlers, &meta)?;
                    }
                }
            }
            RDB_OPCODE_FREQ => {
                input.read_u8()?;
                let value_type = input.read_u8()?;
                input.read_object(value_type, rdb_handlers, &meta)?;
            }
            RDB_OPCODE_IDLE => {
                input.read_length()?;
                let value_type = input.read_u8()?;
                input.read_object(value_type, rdb_handlers, &meta)?;
            }
            RDB_OPCODE_MODULE_AUX => {
                // TODO
            }
            RDB_OPCODE_EOF => {
                if rdb_version >= 5 {
                    input.read_integer(8, true)?;
                }
                break;
            }
            _ => {
                input.read_object(data_type, rdb_handlers, &meta)?;
            }
        };
    };
    rdb_handlers.handle(Object::EOR);
    Ok(Empty)
}

// 跳过rdb的字节
pub(crate) fn skip(input: &mut Conn,
                   length: isize,
                   _: &mut dyn RdbHandler,
                   _: &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    std::io::copy(&mut input.input.as_mut().take(length as u64), &mut std::io::sink())?;
    Ok(Data::Empty)
}

// 当redis响应的数据是Bulk string时，使用此方法读取指定length的字节, 并返回
pub(crate) fn read_bytes(input: &mut Conn, length: isize,
                         _: &mut dyn RdbHandler,
                         _: &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    if length > 0 {
        let mut bytes = vec![0; length as usize];
        input.read_exact(&mut bytes)?;
        let end = &mut [0; 2];
        input.read_exact(end)?;
        if end == b"\r\n" {
            return Ok(Bytes(bytes));
        } else {
            panic!("Expect CRLF after bulk string, but got: {:?}", end);
        }
    } else if length == 0 {
        // length == 0 代表空字符，后面还有CRLF
        input.read_exact(&mut [0; 2])?;
        return Ok(Empty);
    } else {
        // length < 0 代表null
        return Ok(Empty);
    }
}

pub(crate) fn read_zm_len(cursor: &mut Cursor<&Vec<u8>>) -> Result<usize> {
    let len = cursor.read_u8()?;
    if len <= 253 {
        return Ok(len as usize);
    } else if len == 254 {
        let value = cursor.read_u32::<BigEndian>()?;
        return Ok(value as usize);
    }
    Ok(len as usize)
}

pub(crate) fn read_zip_list_entry(cursor: &mut Cursor<Vec<u8>>) -> Result<Vec<u8>> {
    if cursor.read_u8()? >= 254 {
        cursor.read_u32::<LittleEndian>()?;
    }
    let flag = cursor.read_u8()?;
    match flag >> 6 {
        0 => {
            let length = flag & 0x3F;
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        1 => {
            let next_byte = cursor.read_u8()?;
            let length = (((flag as u16) & 0x3F) << 8) | (next_byte as u16);
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        2 => {
            let length = cursor.read_u32::<BigEndian>()?;
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        _ => {}
    }
    match flag {
        ZIP_INT_8BIT => {
            let int = cursor.read_i8()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_16BIT => {
            let int = cursor.read_i16::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_24BIT => {
            let int = cursor.read_i24::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_32BIT => {
            let int = cursor.read_i32::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_64BIT => {
            let int = cursor.read_i64::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        _ => {
            let result = (flag - 0xF1) as isize;
            return Ok(result.to_string().into_bytes());
        }
    }
}

/// Redis中的各个数据类型
pub enum Object<'a> {
    /// String:
    String(KeyValue<'a>),
    
    /// List:
    List(List<'a>),
    
    /// Set:
    Set(Set<'a>),
    
    /// SortedSet:
    SortedSet(SortedSet<'a>),
    
    /// Hash:
    Hash(Hash<'a>),
    
    /// being of rdb
    BOR,
    
    /// end of rdb
    EOR,
}

#[derive(Debug)]
pub struct Meta {
    pub db: isize,
    pub expired_type: Option<ExpireType>,
    pub expired_time: Option<i64>,
}

#[derive(Debug)]
pub enum ExpireType {
    Second,
    Millisecond,
}

#[derive(Debug)]
pub struct KeyValue<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub meta: &'a Meta,
}

pub struct List<'a> {
    pub key: &'a [u8],
    pub values: &'a [Vec<u8>],
    pub meta: &'a Meta,
}

pub struct Set<'a> {
    pub key: &'a [u8],
    pub members: &'a [Vec<u8>],
    pub meta: &'a Meta,
}

pub struct SortedSet<'a> {
    pub key: &'a [u8],
    pub items: &'a [Item],
    pub meta: &'a Meta,
}

#[derive(Debug)]
pub struct Item {
    pub member: Vec<u8>,
    pub score: f64,
}

pub struct Hash<'a> {
    pub key: &'a [u8],
    pub fields: &'a [Field],
    pub meta: &'a Meta,
}

#[derive(Debug)]
pub struct Field {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

/// Map object types to RDB object types.
///
pub(crate) const RDB_TYPE_STRING: u8 = 0;
pub(crate) const RDB_TYPE_LIST: u8 = 1;
pub(crate) const RDB_TYPE_SET: u8 = 2;
pub(crate) const RDB_TYPE_ZSET: u8 = 3;
pub(crate) const RDB_TYPE_HASH: u8 = 4;
/// ZSET version 2 with doubles stored in binary.
pub(crate) const RDB_TYPE_ZSET_2: u8 = 5;
pub(crate) const RDB_TYPE_MODULE: u8 = 6;
/// Module value with annotations for parsing without
/// the generating module being loaded.
pub(crate) const RDB_TYPE_MODULE_2: u8 = 7;

/// Object types for encoded objects.
///
pub(crate) const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub(crate) const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub(crate) const RDB_TYPE_SET_INTSET: u8 = 11;
pub(crate) const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub(crate) const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
pub(crate) const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub(crate) const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;

/// Special RDB opcodes
///
// Module auxiliary data.
pub(crate) const RDB_OPCODE_MODULE_AUX: u8 = 247;
// LRU idle time.
pub(crate) const RDB_OPCODE_IDLE: u8 = 248;
// LFU frequency.
pub(crate) const RDB_OPCODE_FREQ: u8 = 249;
// RDB aux field.
pub(crate) const RDB_OPCODE_AUX: u8 = 250;
// Hash table resize hint.
pub(crate) const RDB_OPCODE_RESIZEDB: u8 = 251;
// Expire time in milliseconds.
pub(crate) const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
// Old expire time in seconds.
pub(crate) const RDB_OPCODE_EXPIRETIME: u8 = 253;
// DB number of the following keys.
pub(crate) const RDB_OPCODE_SELECTDB: u8 = 254;
// End of the RDB file.
pub(crate) const RDB_OPCODE_EOF: u8 = 255;

pub(crate) const ZIP_INT_8BIT: u8 = 254;
pub(crate) const ZIP_INT_16BIT: u8 = 192;
pub(crate) const ZIP_INT_24BIT: u8 = 240;
pub(crate) const ZIP_INT_32BIT: u8 = 208;
pub(crate) const ZIP_INT_64BIT: u8 = 224;

/// Defines related to the dump file format. To store 32 bits lengths for short
/// keys requires a lot of space, so we check the most significant 2 bits of
/// the first byte to interpreter the length:
///
/// 00|XXXXXX => if the two MSB are 00 the len is the 6 bits of this byte
/// 01|XXXXXX XXXXXXXX =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
/// 10|000000 [32 bit integer] => A full 32 bit len in net byte order will follow
/// 10|000001 [64 bit integer] => A full 64 bit len in net byte order will follow
/// 11|OBKIND this means: specially encoded object will follow. The six bits
///           number specify the kind of object that follows.
///           See the RDB_ENC_* defines.
///
/// Lengths up to 63 are stored using a single byte, most DB keys, and may
/// values, will fit inside.
pub(crate) const RDB_ENCVAL: u8 = 3;
pub(crate) const RDB_6BITLEN: u8 = 0;
pub(crate) const RDB_14BITLEN: u8 = 1;
pub(crate) const RDB_32BITLEN: u8 = 0x80;
pub(crate) const RDB_64BITLEN: u8 = 0x81;

/// When a length of a string object stored on disk has the first two bits
/// set, the remaining six bits specify a special encoding for the object
/// accordingly to the following defines:
///
/// 8 bit signed integer
pub(crate) const RDB_ENC_INT8: isize = 0;
/// 16 bit signed integer
pub(crate) const RDB_ENC_INT16: isize = 1;
/// 32 bit signed integer
pub(crate) const RDB_ENC_INT32: isize = 2;
/// string compressed with FASTLZ
pub(crate) const RDB_ENC_LZF: isize = 3;
pub(crate) const BATCH_SIZE: usize = 64;

// 用于包装redis的返回值
pub(crate) enum Data<B, V> {
    // 包装Vec<u8>
    Bytes(B),
    // 包装Vec<Vec<u8>>
    BytesVec(V),
    // 空返回
    Empty,
}
