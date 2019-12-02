use std::io::{Cursor, Error, ErrorKind, Read};
use std::io;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::{CommandHandler, OBJ_HASH, OBJ_LIST, OBJ_SET, OBJ_STRING, OBJ_ZSET, RdbHandler};
use crate::rdb::Data::{Bytes, Empty};
use crate::reader::Reader;

// 回车换行，在redis响应中一般表示终结符，或用作分隔符以分隔数据
pub(crate) const CR: u8 = b'\r';
pub(crate) const LF: u8 = b'\n';
// 代表array响应
pub(crate) const STAR: u8 = b'*';
// 代表bulk string响应
pub(crate) const DOLLAR: u8 = b'$';
// 代表simple string响应
pub(crate) const PLUS: u8 = b'+';
// 代表error响应
pub(crate) const MINUS: u8 = b'-';
// 代表integer响应
pub(crate) const COLON: u8 = b':';

/// Map object types to RDB object types.
///
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
/// ZSET version 2 with doubles stored in binary.
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_MODULE: u8 = 6;
/// Module value with annotations for parsing without
/// the generating module being loaded.
const RDB_TYPE_MODULE_2: u8 = 7;

/// Object types for encoded objects.
///
const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;

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

// 用于包装redis的返回值
pub(crate) enum Data<B, V> {
    // 包装Vec<u8>
    Bytes(B),
    // 包装Vec<Vec<u8>>
    BytesVec(V),
    // 空返回
    Empty,
}

// 读取、解析rdb
pub(crate) fn parse(input: &mut Reader,
                    length: isize,
                    rdb_handlers: &Vec<Box<dyn RdbHandler>>,
                    cmd_handler: &Vec<Box<dyn CommandHandler>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    println!("rdb size: {} bytes", length);
    let mut bytes = vec![0; 5];
    // 开头5个字节: REDIS
    input.read_exact(&mut bytes)?;
    // 4个字节: rdb版本
    input.read_exact(&mut bytes[..=3])?;
    let rdb_version = String::from_utf8(bytes[..=3].to_vec()).unwrap();
    let rdb_version = rdb_version.parse::<isize>().unwrap();
    loop {
        let data_type = input.read_u8()?;
        match data_type {
            RDB_OPCODE_AUX => {
                let field_name = input.read_string()?;
                let field_val = input.read_string()?;
                let field_name = String::from_utf8(field_name).unwrap();
                let field_val = String::from_utf8(field_val).unwrap();
                println!("{}:{}", field_name, field_val);
            }
            RDB_OPCODE_SELECTDB => {
                let (db, _) = input.read_length()?;
                println!("db: {}", db);
            }
            RDB_OPCODE_RESIZEDB => {
                let (db, _) = input.read_length()?;
                println!("db total keys: {}", db);
                let (db, _) = input.read_length()?;
                println!("db expired keys: {}", db);
            }
            RDB_OPCODE_EXPIRETIME => {}
            RDB_OPCODE_EXPIRETIME_MS => {
                let expire_times = input.read_integer(8, false)?;
                let _type = input.read_u8()?;
                match _type {
                    RDB_OPCODE_FREQ => {
                        let frq = input.read_u8()?;
                        let value_type = input.read_u8()?;
                    }
                    RDB_OPCODE_IDLE => {}
                    _ => {}
                    // TODO
                }
            }
            RDB_OPCODE_FREQ => {
                // TODO
            }
            RDB_OPCODE_IDLE => {
                // TODO
            }
            RDB_OPCODE_MODULE_AUX => {
                // TODO
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
            RDB_TYPE_STRING => {
                let key = input.read_string()?;
                let mut iter = StrValIter { count: 1, input };
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, OBJ_STRING)
                );
            }
            RDB_TYPE_HASH_ZIPLIST | RDB_TYPE_ZSET_ZIPLIST | RDB_TYPE_LIST_ZIPLIST => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
                let cursor = &mut Cursor::new(bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let count = cursor.read_u16::<LittleEndian>()? as isize;
                let mut iter = ZipListIter { count, cursor };
                let _type;
                if data_type == RDB_TYPE_HASH_ZIPLIST {
                    _type = OBJ_HASH;
                } else if data_type == RDB_TYPE_ZSET_ZIPLIST {
                    _type = OBJ_ZSET;
                } else {
                    _type = OBJ_LIST;
                }
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, _type)
                );
            }
            RDB_TYPE_LIST_QUICKLIST => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = QuickListIter { len: -1, count, input, cursor: Option::None };
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, OBJ_LIST)
                );
            }
            RDB_TYPE_LIST | RDB_TYPE_SET => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = StrValIter { count, input };
                let _type;
                if data_type == RDB_TYPE_LIST {
                    _type = OBJ_LIST;
                } else {
                    _type = OBJ_SET;
                }
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, _type));
            }
            RDB_TYPE_ZSET => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = SortedSetIter { count, v: 1, read_score: false, input };
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, OBJ_ZSET));
            }
            RDB_TYPE_ZSET_2 => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = SortedSetIter { count, v: 2, read_score: false, input };
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, OBJ_ZSET));
            }
            RDB_TYPE_HASH => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = StrValIter { count, input };
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, OBJ_HASH));
            }
            RDB_TYPE_HASH_ZIPMAP => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
                let cursor = &mut Cursor::new(&bytes);
                cursor.set_position(1);
                let mut iter = ZipMapIter { has_more: true, read_val: false, cursor };
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, OBJ_HASH));
            }
            RDB_TYPE_SET_INTSET => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
                let mut cursor = Cursor::new(&bytes);
                let encoding = cursor.read_i32::<LittleEndian>()?;
                let length = cursor.read_u32::<LittleEndian>()?;
                let mut iter = IntSetIter { encoding, count: length as isize, cursor: &mut cursor };
                rdb_handlers.iter().for_each(|handler|
                    handler.handle(&key, &mut iter, OBJ_SET));
            }
            RDB_OPCODE_EOF => {
                if rdb_version >= 5 {
                    input.read_integer(8, true)?;
                }
                break;
            }
            _ => break
        };
    };
    Ok(Empty)
}

// 当redis响应的数据是Bulk string时，使用此方法读取指定length的字节, 并返回
pub(crate) fn read_bytes(input: &mut Reader, length: isize, _: &Vec<Box<dyn RdbHandler>>, _: &Vec<Box<dyn CommandHandler>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    if length > 0 {
        let mut bytes = vec![0; length as usize];
        input.read_exact(&mut bytes)?;
        let end = &mut [0; 2];
        input.read_exact(end)?;
        if end == b"\r\n" {
            return Ok(Bytes(bytes));
        } else {
            let err = format!("Expect CRLF after bulk string, but got: {:?}", end);
            return Err(Error::new(ErrorKind::Other, err));
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

fn read_zm_len(cursor: &mut Cursor<&Vec<u8>>) -> Result<usize, Error> {
    let len = cursor.read_u8()?;
    if len <= 253 {
        return Ok(len as usize);
    } else if len == 254 {
        let value = cursor.read_u32::<BigEndian>()?;
        return Ok(value as usize);
    }
    Ok(len as usize)
}

fn read_zip_list_entry(cursor: &mut Cursor<Vec<u8>>) -> Result<Vec<u8>, Error> {
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

pub trait Iter {
    fn next(&mut self) -> io::Result<Vec<u8>>;
}

// 字符串类型的值迭代器
struct StrValIter<'a> {
    count: isize,
    input: &'a mut Reader,
}

impl Iter for StrValIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        while self.count > 0 {
            let val = self.input.read_string()?;
            self.count -= 1;
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// ListQuickList的值迭代器
struct QuickListIter<'a> {
    len: isize,
    count: isize,
    input: &'a mut Reader,
    cursor: Option<Cursor<Vec<u8>>>,
}

impl Iter for QuickListIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        // TODO fix error: failed to fill whole buffer
        if self.len == -1 && self.count > 0 {
            let data = self.input.read_string()?;
            self.cursor = Option::Some(Cursor::new(data));
            // 跳过ZL_BYTES和ZL_TAIL
            let cursor = self.cursor.as_mut().unwrap();
            cursor.set_position(8);
            self.len = cursor.read_i16::<LittleEndian>()? as isize;
            if self.len == 0 {
                self.len = -1;
                self.count -= 1;
            }
            if self.has_more() {
                return self.next();
            }
        } else {
            let val = read_zip_list_entry(self.cursor.as_mut().unwrap())?;
            self.len -= 1;
            if self.len == 0 {
                self.len = -1;
                self.count -= 1;
            }
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

impl QuickListIter<'_> {
    fn has_more(&self) -> bool {
        self.len > 0 || self.count > 0
    }
}

// ZipList的值迭代器
struct ZipListIter<'a> {
    count: isize,
    cursor: &'a mut Cursor<Vec<u8>>,
}

impl Iter for ZipListIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val = read_zip_list_entry(self.cursor)?;
            self.count -= 1;
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// SortedSet的值迭代器
struct SortedSetIter<'a> {
    count: isize,
    /// v = 1, zset
    /// v = 2, zset2
    v: u8,
    read_score: bool,
    input: &'a mut Reader,
}

impl Iter for SortedSetIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val;
            if self.read_score {
                if self.v == 1 {
                    val = self.input.read_double()?;
                } else {
                    // TODO zset2 score处理
                    let score = self.input.read_i64::<LittleEndian>()?;
                    val = score.to_string().into_bytes();
                }
                self.count -= 1;
                self.read_score = false;
            } else {
                val = self.input.read_string()?;
                self.read_score = true;
            }
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// HashZipMap的值迭代器
struct ZipMapIter<'a> {
    has_more: bool,
    read_val: bool,
    cursor: &'a mut Cursor<&'a Vec<u8>>,
}

impl Iter for ZipMapIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if !self.has_more {
            return Err(Error::new(ErrorKind::NotFound, "No element left"));
        }
        if self.read_val {
            let zm_len = read_zm_len(self.cursor)?;
            if zm_len == 255 {
                self.has_more = false;
                return Ok(Vec::new());
            }
            let free = self.cursor.read_i8()?;
            let mut value = Vec::with_capacity(zm_len as usize);
            self.cursor.read_exact(&mut value)?;
            self.cursor.set_position(self.cursor.position() + free as u64);
            return Ok(value);
        } else {
            let zm_len = read_zm_len(self.cursor)?;
            if zm_len == 255 {
                self.has_more = false;
                return Err(Error::new(ErrorKind::NotFound, "No element left"));
            }
            let mut field = Vec::with_capacity(zm_len as usize);
            self.cursor.read_exact(&mut field)?;
            return Ok(field);
        }
    }
}

// IntSet的值迭代器
struct IntSetIter<'a> {
    encoding: i32,
    count: isize,
    cursor: &'a mut Cursor<&'a Vec<u8>>,
}

impl Iter for IntSetIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val;
            match self.encoding {
                2 => {
                    let member = self.cursor.read_i16::<LittleEndian>()?;
                    let member = member.to_string().into_bytes();
                    val = member;
                }
                4 => {
                    let member = self.cursor.read_i32::<LittleEndian>()?;
                    let member = member.to_string().into_bytes();
                    val = member;
                }
                8 => {
                    let member = self.cursor.read_i64::<LittleEndian>()?;
                    let member = member.to_string().into_bytes();
                    val = member;
                }
                _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid integer size")),
            }
            self.count -= 1;
            return Ok(val);
        }
        return Err(Error::new(ErrorKind::NotFound, "No element left"));
    }
}