/*!
 处理redis的响应数据
*/

use std::any::Any;
use std::f64::{INFINITY, NAN, NEG_INFINITY};
use std::fs::File;
use std::io::{BufWriter, Cursor, Error, ErrorKind, Read, Result, Write};
use std::net::TcpStream;

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use crate::{CommandHandler, io, lzf, RdbHandler, to_string};
use crate::iter::{IntSetIter, Iter, QuickListIter, SortedSetIter, StrValIter, ZipListIter, ZipMapIter};
use crate::rdb::*;
use crate::rdb::Data::{Bytes, BytesVec, Empty};

pub(crate) trait ReadWrite: Read + Write {
    fn as_any(&self) -> &dyn Any;
}

impl ReadWrite for TcpStream {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReadWrite for File {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) struct Conn {
    pub(crate) input: Box<dyn ReadWrite>,
    len: i64,
    marked: bool,
}

#[cfg(test)]
pub(crate) fn from_file(file: File) -> Conn {
    Conn { input: Box::new(file), len: 0, marked: false }
}

pub(crate) fn new(input: TcpStream) -> Conn {
    Conn { input: Box::new(input), len: 0, marked: false }
}

impl Conn {
    pub(crate) fn reply(&mut self,
                        func: fn(&mut Conn, isize,
                                 &mut dyn RdbHandler,
                                 &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>>,
                        rdb_handler: &mut dyn RdbHandler,
                        cmd_handler: &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
        loop {
            let response_type = self.read_u8()?;
            match response_type {
                // Plus: Simple String
                // Minus: Error
                // Colon: Integer
                PLUS | MINUS | COLON => {
                    let mut bytes = vec![];
                    loop {
                        let byte = self.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = self.read_u8()?;
                    if byte == LF {
                        if response_type == PLUS || response_type == COLON {
                            return Ok(Bytes(bytes));
                        } else {
                            let message = to_string(bytes);
                            return Err(Error::new(ErrorKind::InvalidInput, message));
                        }
                    } else {
                        panic!("Expect LF after CR");
                    }
                }
                DOLLAR => { // Bulk String
                    let mut bytes = vec![];
                    loop {
                        let byte = self.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = self.read_u8()?;
                    if byte == LF {
                        let length = to_string(bytes);
                        let length = length.parse::<isize>().unwrap();
                        return func(self, length, rdb_handler, cmd_handler);
                    } else {
                        panic!("Expect LF after CR");
                    }
                }
                STAR => { // Array
                    let mut bytes = vec![];
                    loop {
                        let byte = self.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = self.read_u8()?;
                    if byte == LF {
                        let length = to_string(bytes);
                        let length = length.parse::<isize>().unwrap();
                        if length <= 0 {
                            return Ok(Empty);
                        } else {
                            let mut result = Vec::with_capacity(length as usize);
                            for _ in 0..length {
                                match self.reply(io::read_bytes, rdb_handler, cmd_handler)? {
                                    Bytes(resp) => {
                                        result.push(resp);
                                    }
                                    BytesVec(mut resp) => {
                                        result.append(&mut resp);
                                    }
                                    Empty => panic!("Expect Redis response, but got empty")
                                }
                            }
                            return Ok(BytesVec(result));
                        }
                    } else {
                        panic!("Expect LF after CR");
                    }
                }
                LF => {
                    // 无需处理
                }
                _ => {
                    panic!("错误的响应类型: {}", response_type);
                }
            }
        }
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
        return Err(Error::new(ErrorKind::Other, "not marked"));
    }
    
    pub(crate) fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.input.read_exact(&mut buf)?;
        if self.marked {
            self.len += 1;
        };
        Ok(buf[0])
    }
    
    pub(crate) fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.input.read_exact(buf)?;
        if self.marked {
            self.len += buf.len() as i64;
        };
        Ok(())
    }
    
    pub(crate) fn read_u64<O: ByteOrder>(&mut self) -> Result<u64> {
        let int = self.input.read_u64::<O>()?;
        if self.marked {
            self.len += 8;
        };
        Ok(int)
    }
    
    pub(crate) fn read_i8(&mut self) -> Result<i8> {
        let int = self.input.read_i8()?;
        if self.marked {
            self.len += 1;
        };
        Ok(int)
    }
    
    pub(crate) fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<()> {
        send(&mut self.input, command, args)?;
        Ok(())
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
                let mut buff = vec![0; len as usize];
                self.read_exact(&mut buff)?;
                let score_str = to_string(buff);
                let score = score_str.parse::<f64>().unwrap();
                return Ok(score);
            }
        }
    }
    
    // 根据传入的数据类型，从流中读取对应类型的数据
    pub(crate) fn read_object(&mut self, value_type: u8,
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
                        rdb_handlers.handle(Object::List(List { key: &key, values: &val, meta }));
                    } else {
                        rdb_handlers.handle(Object::Set(Set { key: &key, members: &val, meta }));
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
                    rdb_handlers.handle(Object::SortedSet(SortedSet { key: &key, items: &val, meta }));
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
                    rdb_handlers.handle(Object::SortedSet(SortedSet { key: &key, items: &val, meta }));
                }
            }
            RDB_TYPE_HASH => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count: count * 2, input: self };
                
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
                let mut iter = ZipMapIter { has_more: true, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut fields = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(field) = iter.next() {
                            fields.push(field);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    rdb_handlers.handle(Object::Hash(Hash { key: &key, fields: &fields, meta }));
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
                    rdb_handlers.handle(Object::List(List { key: &key, values: &val, meta }));
                }
            }
            RDB_TYPE_MODULE => {
                // TODO
                unimplemented!("RDB_TYPE_MODULE");
            }
            RDB_TYPE_MODULE_2 => {
                // TODO
                unimplemented!("RDB_TYPE_MODULE_2");
            }
            RDB_TYPE_STREAM_LISTPACKS => {
                // TODO
                unimplemented!("RDB_TYPE_STREAM_LISTPACKS");
            }
            _ => panic!("unknown data type: {}", value_type)
        }
        Ok(())
    }
}

pub(crate) fn send<T: Write>(output: &mut T, command: &[u8], args: &[&[u8]]) -> Result<()> {
    let mut writer = BufWriter::new(output);
    writer.write(&[STAR])?;
    let args_len = args.len() + 1;
    writer.write(&args_len.to_string().into_bytes())?;
    writer.write(&[CR, LF, DOLLAR])?;
    writer.write(&command.len().to_string().into_bytes())?;
    writer.write(&[CR, LF])?;
    writer.write(command)?;
    writer.write(&[CR, LF])?;
    for arg in args {
        writer.write(&[DOLLAR])?;
        writer.write(&arg.len().to_string().into_bytes())?;
        writer.write(&[CR, LF])?;
        writer.write(arg)?;
        writer.write(&[CR, LF])?;
    }
    writer.flush()
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

// 跳过rdb的字节
pub(crate) fn skip(input: &mut Conn,
                   length: isize,
                   _: &mut dyn RdbHandler,
                   _: &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    std::io::copy(&mut input.input.as_mut().take(length as u64), &mut std::io::sink())?;
    Ok(Data::Empty)
}

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
