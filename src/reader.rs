use std::f64::{INFINITY, NAN, NEG_INFINITY};
use std::io::{Cursor, Error, ErrorKind, Read, Result};
use std::net::TcpStream;

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use crate::{lzf, RdbHandler};
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

pub(crate) struct Reader {
    pub(crate) stream: Box<TcpStream>,
    len: i64,
    marked: bool,
}

impl Reader {
    pub(crate) fn new(stream: Box<TcpStream>) -> Reader {
        Reader { stream, len: 0, marked: false }
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
    
    pub(crate) fn read_i64<T: ByteOrder>(&mut self) -> Result<i64> {
        let int = self.stream.read_i64::<T>()?;
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
        return Err(Error::new(ErrorKind::Other, "Reader not marked"));
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
        Err(Error::new(ErrorKind::InvalidData, "Invalid integer size"))
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
                _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid string length"))
            };
        };
        let mut buff = vec![0; length as usize];
        self.read_exact(&mut buff)?;
        Ok(buff)
    }
    
    // 从流中读取一个double
    pub(crate) fn read_double(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u8()?;
        match len {
            255 => {
                return Ok(NEG_INFINITY.to_string().into_bytes());
            }
            254 => {
                return Ok(INFINITY.to_string().into_bytes());
            }
            253 => {
                return Ok(NAN.to_string().into_bytes());
            }
            _ => {
                let mut buff = Vec::with_capacity(len as usize);
                self.read_exact(&mut buff)?;
                return Ok(buff);
            }
        }
    }
    
    // 根据传入的数据类型，从流中读取对应类型的数据
    pub(crate) fn read_object(&mut self, value_type: u8, rdb_handlers: &Vec<Box<dyn RdbHandler>>) -> Result<()> {
        match value_type {
            RDB_TYPE_STRING => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
                let val = self.read_string()?;
                let val = String::from_utf8(val).unwrap();
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&Object::String(&key, &val))
                );
            }
            RDB_TYPE_LIST | RDB_TYPE_SET => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count, input: self };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            let next_val = String::from_utf8(next_val).unwrap();
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if value_type == RDB_TYPE_LIST {
                        rdb_handlers.iter().for_each(|handler|
                            handler.handle(&Object::List(&key, &val)));
                    } else {
                        rdb_handlers.iter().for_each(|handler|
                            handler.handle(&Object::Set(&key, &val)));
                    }
                }
            }
            RDB_TYPE_ZSET => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 1, read_score: false, input: self };
                
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
                        handler.handle(&Object::SortedSet(&key, &val)));
                }
            }
            RDB_TYPE_ZSET_2 => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 2, read_score: false, input: self };
                
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
                        handler.handle(&Object::SortedSet(&key, &val)));
                }
            }
            RDB_TYPE_HASH => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count, input: self };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let field;
                        let field_val;
                        if let Ok(next_val) = iter.next() {
                            field = String::from_utf8(next_val).unwrap();
                            if let Ok(next_val) = iter.next() {
                                field_val = String::from_utf8(next_val).unwrap();
                            } else {
                                return Err(Error::new(ErrorKind::InvalidData, "Except hash field value after field name"));
                            }
                            val.push((field, field_val));
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(&Object::Hash(&key, &val)));
                }
            }
            RDB_TYPE_HASH_ZIPMAP => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(&bytes);
                cursor.set_position(1);
                let mut iter = ZipMapIter { has_more: true, read_val: false, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let field;
                        let field_val;
                        if let Ok(next_val) = iter.next() {
                            field = String::from_utf8(next_val).unwrap();
                            if let Ok(next_val) = iter.next() {
                                field_val = String::from_utf8(next_val).unwrap();
                            } else {
                                return Err(Error::new(ErrorKind::InvalidData, "Except hash field value after field name"));
                            }
                            val.push((field, field_val));
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(&Object::Hash(&key, &val)));
                }
            }
            RDB_TYPE_LIST_ZIPLIST => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
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
                            let next_val = String::from_utf8(next_val).unwrap();
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(&Object::List(&key, &val)));
                }
            }
            RDB_TYPE_HASH_ZIPLIST => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
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
                        let field;
                        let field_val;
                        if let Ok(next_val) = iter.next() {
                            field = String::from_utf8(next_val).unwrap();
                            if let Ok(next_val) = iter.next() {
                                field_val = String::from_utf8(next_val).unwrap();
                            } else {
                                return Err(Error::new(ErrorKind::InvalidData, "Except hash field value after field name"));
                            }
                            val.push((field, field_val));
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(&Object::Hash(&key, &val)));
                }
            }
            RDB_TYPE_ZSET_ZIPLIST => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
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
                        handler.handle(&Object::SortedSet(&key, &val)));
                }
            }
            RDB_TYPE_SET_INTSET => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
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
                            let next_val = String::from_utf8(next_val).unwrap();
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(&Object::Set(&key, &val)));
                }
            }
            RDB_TYPE_LIST_QUICKLIST => {
                let key = self.read_string()?;
                let key = String::from_utf8(key).unwrap();
                let (count, _) = self.read_length()?;
                let mut iter = QuickListIter { len: -1, count, input: self, cursor: Option::None };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            let next_val = String::from_utf8(next_val).unwrap();
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.iter().for_each(|handler|
                        handler.handle(&Object::List(&key, &val)));
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
            _ => {
                let err = format!("unknown data type: {}", value_type);
                return Err(Error::new(ErrorKind::InvalidData, err));
            }
        }
        Ok(())
    }
}