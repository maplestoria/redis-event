use std::f64::{INFINITY, NAN, NEG_INFINITY};
use std::io::{Cursor, Read, Result};
use std::net::TcpStream;

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use crate::{lzf, RdbHandler, to_string};
use crate::iter::{IntSetIter, Iter, QuickListIter, SortedSetIter, StrValIter, ZipListIter, ZipMapIter};
use crate::rdb::*;

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

pub(crate) struct Conn {
    pub(crate) stream: Box<TcpStream>,
    len: i64,
    marked: bool,
}

impl Conn {
    pub(crate) fn new(stream: Box<TcpStream>) -> Conn {
        Conn { stream, len: 0, marked: false }
    }
    
    pub(crate) fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.stream.read_exact(&mut buf)?;
        if self.marked {
            self.len += 1;
        };
        Ok(buf[0])
    }
    
    pub(crate) fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.stream.read_exact(buf)?;
        if self.marked {
            self.len += buf.len() as i64;
        };
        Ok(())
    }
    
    pub(crate) fn read_u64<T: ByteOrder>(&mut self) -> Result<u64> {
        let int = self.stream.read_u64::<T>()?;
        if self.marked {
            self.len += 8;
        };
        Ok(int)
    }
    
    pub(crate) fn read_i8(&mut self) -> Result<i8> {
        let int = self.stream.read_i8()?;
        if self.marked {
            self.len += 1;
        };
        Ok(int)
    }
    
    pub(crate) fn mark(&mut self) {
        self.marked = true;
    }
    
    pub(crate) fn unmark(&mut self) -> Result<i64> {
        if self.marked {
            let len = self.len;
            self.len = 0;
            self.marked = false;
            return Ok(len);
        }
        panic!("Reader not marked");
    }
    
    // 读取redis响应中下一条数据的长度
    pub(crate) fn read_length(&mut self) -> Result<(isize, bool)> {
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
    pub(crate) fn read_integer(&mut self, size: isize, is_big_endian: bool) -> Result<isize> {
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
    pub(crate) fn read_string(&mut self) -> Result<Vec<u8>> {
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
    pub(crate) fn read_double(&mut self) -> Result<f64> {
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
    pub(crate) fn read_object(&mut self, value_type: u8, rdb_handlers: &Vec<Box<dyn RdbHandler>>) -> Result<()> {
        match value_type {
            RDB_TYPE_STRING => {
                let key = self.read_string()?;
                let value = self.read_string()?;
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(Object::String(KeyValue { key: &key, value: &value }))
                );
            }
            RDB_TYPE_LIST | RDB_TYPE_SET => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count, input: self };
    
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
                        rdb_handlers.iter().for_each(|handler|
                            handler.handle(Object::List(List { key: &key, values: &val })));
                    } else {
                        rdb_handlers.iter().for_each(|handler|
                            handler.handle(Object::Set(Set { key: &key, members: &val })));
                    }
                }
            }
            RDB_TYPE_ZSET => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 1, input: self };
    
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::SortedSet(SortedSet { key: &key, items: &val })));
                }
            }
            RDB_TYPE_ZSET_2 => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 2, input: self };
    
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::SortedSet(SortedSet { key: &key, items: &val })));
                }
            }
            RDB_TYPE_HASH => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count, input: self };
    
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::Hash(Hash { key: &key, fields: &val })));
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::Hash(Hash { key: &key, fields: &val })));
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::List(List { key: &key, values: &val })));
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::Hash(Hash { key: &key, fields: &val })));
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::SortedSet(SortedSet { key: &key, items: &val })));
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::Set(Set { key: &key, members: &val })));
                }
            }
            RDB_TYPE_LIST_QUICKLIST => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = QuickListIter { len: -1, count, input: self, cursor: Option::None };
    
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
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(Object::List(List { key: &key, values: &val })));
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