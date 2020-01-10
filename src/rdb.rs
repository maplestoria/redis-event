use std::f64::{INFINITY, NAN, NEG_INFINITY};
use std::io::{Cursor, Read, Result};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::{CommandHandler, lzf, RdbHandler, to_string};
use crate::iter::{IntSetIter, Iter, QuickListIter, SortedSetIter, StrValIter, ZipListIter, ZipMapIter};
use crate::rdb::Data::{Bytes, Empty};

pub(crate) trait ReadResp: Read + Sized {
    // 读取redis响应中下一条数据的长度
    fn read_length(&mut self) -> Result<(isize, bool)> {
        let byte = self.read_u8()?;
        let _type = (byte & 0xC0) >> 6;
        
        let mut result = -1;
        let mut is_encoded = false;
        
        if _type == RDB_ENCVAL {
            result = (byte & 0x3F) as isize;
            is_encoded = true;
        } else if _type == RDB_6BITLEN {
            result = (byte & 0x3F) as isize;
        } else if _type == RDB_14BITLEN {
            let next_byte = self.read_u8()?;
            result = (((byte as u16 & 0x3F) << 8) | next_byte as u16) as isize;
        } else if byte == RDB_32BITLEN {
            result = self.read_integer(4, true)?;
        } else if byte == RDB_64BITLEN {
            result = self.read_integer(8, true)?;
        };
        Ok((result, is_encoded))
    }
    
    // 从流中读取一个Integer
    fn read_integer(&mut self, size: isize, is_big_endian: bool) -> Result<isize> {
        let mut buff = vec![0; size as usize];
        self.read_exact(&mut buff)?;
        let mut cursor = Cursor::new(&buff);
        
        if is_big_endian {
            if size == 2 {
                return Ok(cursor.read_i16::<BigEndian>()? as isize);
            } else if size == 4 {
                return Ok(cursor.read_i32::<BigEndian>()? as isize);
            } else if size == 8 {
                return Ok(cursor.read_i64::<BigEndian>()? as isize);
            };
        } else {
            if size == 2 {
                return Ok(cursor.read_i16::<LittleEndian>()? as isize);
            } else if size == 4 {
                return Ok(cursor.read_i32::<LittleEndian>()? as isize);
            } else if size == 8 {
                return Ok(cursor.read_i64::<LittleEndian>()? as isize);
            };
        }
        panic!("Invalid integer size: {}", size)
    }
    
    // 从流中读取一个string
    fn read_string(&mut self) -> Result<Vec<u8>> {
        let (length, is_encoded) = self.read_length()?;
        if is_encoded {
            match length {
                RDB_ENC_INT8 => {
                    let int = self.read_i8()?;
                    return Ok(int.to_string().into_bytes());
                }
                RDB_ENC_INT16 => {
                    let int = self.read_integer(2, false)?;
                    return Ok(int.to_string().into_bytes());
                }
                RDB_ENC_INT32 => {
                    let int = self.read_integer(4, false)?;
                    return Ok(int.to_string().into_bytes());
                }
                RDB_ENC_LZF => {
                    let (compressed_len, _) = self.read_length()?;
                    let (origin_len, _) = self.read_length()?;
                    let mut compressed = vec![0; compressed_len as usize];
                    self.read_exact(&mut compressed)?;
                    let mut origin = vec![0; origin_len as usize];
                    lzf::decompress(&mut compressed, compressed_len, &mut origin, origin_len);
                    return Ok(origin);
                }
                _ => panic!("Invalid string length: {}", length)
            };
        };
        let mut buff = vec![0; length as usize];
        self.read_exact(&mut buff)?;
        Ok(buff)
    }
    
    // 从流中读取一个double
    fn read_double(&mut self) -> Result<f64> {
        let len = self.read_u8()?;
        match len {
            255 => {
                return Ok(NEG_INFINITY);
            }
            254 => {
                return Ok(INFINITY);
            }
            253 => {
                return Ok(NAN);
            }
            _ => {
                let mut buff = Vec::with_capacity(len as usize);
                self.read_exact(&mut buff)?;
                let score_str = to_string(buff);
                let score = score_str.parse::<f64>().unwrap();
                return Ok(score);
            }
        }
    }
    
    // 根据传入的数据类型，从流中读取对应类型的数据
    fn read_object(&mut self, value_type: u8,
                   rdb_handlers: &mut dyn RdbHandler,
                   meta: &Meta) -> Result<()> {
        match value_type {
            RDB_TYPE_STRING => {
                let key = self.read_string()?;
                let value = self.read_string()?;
                rdb_handlers.handle(Object::String(KeyValue { key: &key, value: &value, meta }));
            }
            RDB_TYPE_LIST | RDB_TYPE_SET => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count, input: Box::new(self) };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if value_type == RDB_TYPE_LIST {
                        rdb_handlers.handle(Object::List(List { key: &key, values: &val, meta }));
                    } else {
                        rdb_handlers.handle(Object::Set(Set { key: &key, members: &val, meta }));
                    }
                }
            }
            RDB_TYPE_ZSET => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 1, input: Box::new(self) };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::SortedSet(SortedSet { key: &key, items: &val, meta }));
                }
            }
            RDB_TYPE_ZSET_2 => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 2, input: Box::new(self) };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::SortedSet(SortedSet { key: &key, items: &val, meta }));
                }
            }
            RDB_TYPE_HASH => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count: count * 2, input: Box::new(self) };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let name;
                        let value;
                        if let Ok(next_val) = iter.next() {
                            name = next_val;
                            value = iter.next().expect("missing hash field value");
                            val.push(Field { name, value });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::Hash(Hash { key: &key, fields: &val, meta }));
                }
            }
            RDB_TYPE_HASH_ZIPMAP => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(&bytes);
                cursor.set_position(1);
                let mut iter = ZipMapIter { has_more: true, read_val: false, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let name;
                        let value;
                        if let Ok(next_val) = iter.next() {
                            name = next_val;
                            value = iter.next().expect("missing hash field value");
                            val.push(Field { name, value });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::Hash(Hash { key: &key, fields: &val, meta }));
                }
            }
            RDB_TYPE_LIST_ZIPLIST => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let count = cursor.read_u16::<LittleEndian>()? as isize;
                let mut iter = ZipListIter { count, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::List(List { key: &key, values: &val, meta }));
                }
            }
            RDB_TYPE_HASH_ZIPLIST => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let count = cursor.read_u16::<LittleEndian>()? as isize;
                let mut iter = ZipListIter { count, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let name;
                        let value;
                        if let Ok(next_val) = iter.next() {
                            name = next_val;
                            value = iter.next().expect("missing hash field value");
                            val.push(Field { name, value });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::Hash(Hash { key: &key, fields: &val, meta }));
                }
            }
            RDB_TYPE_ZSET_ZIPLIST => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let count = cursor.read_u16::<LittleEndian>()? as isize;
                let mut iter = ZipListIter { count, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let member;
                        let score: f64;
                        if let Ok(next_val) = iter.next() {
                            member = next_val;
                            let score_str = to_string(iter.next()
                                .expect("missing sorted set element's score"));
                            score = score_str.parse::<f64>().unwrap();
                            val.push(Item { member, score });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::SortedSet(SortedSet { key: &key, items: &val, meta }));
                }
            }
            RDB_TYPE_SET_INTSET => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let mut cursor = Cursor::new(&bytes);
                let encoding = cursor.read_i32::<LittleEndian>()?;
                let length = cursor.read_u32::<LittleEndian>()?;
                let mut iter = IntSetIter { encoding, count: length as isize, cursor: &mut cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::Set(Set { key: &key, members: &val, meta }));
                }
            }
            RDB_TYPE_LIST_QUICKLIST => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = QuickListIter { len: -1, count, input: Box::new(self), cursor: Option::None };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::List(List { key: &key, values: &val, meta }));
                }
            }
            RDB_TYPE_MODULE => {
                // TODO
            }
            RDB_TYPE_MODULE_2 => {
                // TODO
            }
            RDB_TYPE_STREAM_LISTPACKS => {
                // TODO
            }
            _ => panic!("unknown data type: {}", value_type)
        }
        Ok(())
    }
}

impl<R: Read + Sized> ReadResp for R {}

// 读取、解析rdb
pub(crate) fn parse(mut input: &mut dyn Read,
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
pub(crate) fn skip(input: &mut dyn Read,
                   length: isize,
                   _: &mut dyn RdbHandler,
                   _: &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    std::io::copy(&mut input.take(length as u64), &mut std::io::sink())?;
    Ok(Data::Empty)
}

// 当redis响应的数据是Bulk string时，使用此方法读取指定length的字节, 并返回
pub(crate) fn read_bytes(input: &mut dyn Read, length: isize,
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
const RDB_OPCODE_MODULE_AUX: u8 = 247;
// LRU idle time.
const RDB_OPCODE_IDLE: u8 = 248;
// LFU frequency.
const RDB_OPCODE_FREQ: u8 = 249;
// RDB aux field.
const RDB_OPCODE_AUX: u8 = 250;
// Hash table resize hint.
const RDB_OPCODE_RESIZEDB: u8 = 251;
// Expire time in milliseconds.
const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
// Old expire time in seconds.
const RDB_OPCODE_EXPIRETIME: u8 = 253;
// DB number of the following keys.
const RDB_OPCODE_SELECTDB: u8 = 254;
// End of the RDB file.
const RDB_OPCODE_EOF: u8 = 255;

const ZIP_INT_8BIT: u8 = 254;
const ZIP_INT_16BIT: u8 = 192;
const ZIP_INT_24BIT: u8 = 240;
const ZIP_INT_32BIT: u8 = 208;
const ZIP_INT_64BIT: u8 = 224;

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
const RDB_ENCVAL: u8 = 3;
const RDB_6BITLEN: u8 = 0;
const RDB_14BITLEN: u8 = 1;
const RDB_32BITLEN: u8 = 0x80;
const RDB_64BITLEN: u8 = 0x81;

/// When a length of a string object stored on disk has the first two bits
/// set, the remaining six bits specify a special encoding for the object
/// accordingly to the following defines:
///
/// 8 bit signed integer
const RDB_ENC_INT8: isize = 0;
/// 16 bit signed integer
const RDB_ENC_INT16: isize = 1;
/// 32 bit signed integer
const RDB_ENC_INT32: isize = 2;
/// string compressed with FASTLZ
const RDB_ENC_LZF: isize = 3;
const BATCH_SIZE: usize = 64;

// 用于包装redis的返回值
pub(crate) enum Data<B, V> {
    // 包装Vec<u8>
    Bytes(B),
    // 包装Vec<Vec<u8>>
    BytesVec(V),
    // 空返回
    Empty,
}
