use std::io::{BufRead, Cursor, Error, ErrorKind, Read};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::{lzf, OBJ_HASH, OBJ_LIST, OBJ_SET, OBJ_STRING, OBJ_ZSET, RdbEventHandler};
use crate::rdb::Data::{Bytes, Empty};

/// 回车换行，在redis响应中一般表示终结符，或用作分隔符以分隔数据
pub(crate) const CR: u8 = b'\r';
pub(crate) const LF: u8 = b'\n';
/// 代表array响应
pub(crate) const STAR: u8 = b'*';
/// 代表bulk string响应
pub(crate) const DOLLAR: u8 = b'$';
/// 代表simple string响应
pub(crate) const PLUS: u8 = b'+';
/// 代表error响应
pub(crate) const MINUS: u8 = b'-';
/// 代表integer响应
pub(crate) const COLON: u8 = b':';

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
/// RDB aux field.
const RDB_OPCODE_AUX: u8 = 250;
/// Hash table resize hint.
const RDB_OPCODE_RESIZEDB: u8 = 251;
/// DB number of the following keys.
const RDB_OPCODE_SELECTDB: u8 = 254;
/// End of the RDB file.
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
pub(crate) fn parse(input: &mut dyn Read,
                    length: isize,
                    handlers: &Vec<Box<dyn RdbEventHandler>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
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
                let field_name = read_string(input)?;
                let field_val = read_string(input)?;
                let field_name = String::from_utf8(field_name).unwrap();
                let field_val = String::from_utf8(field_val).unwrap();
                println!("{}:{}", field_name, field_val);
            }
            RDB_OPCODE_SELECTDB => {
                let (db, _) = read_length(input)?;
                println!("db: {}", db);
            }
            RDB_OPCODE_RESIZEDB => {
                let (db, _) = read_length(input)?;
                println!("db total keys: {}", db);
                let (db, _) = read_length(input)?;
                println!("db expired keys: {}", db);
            }
            RDB_TYPE_STRING => {
                let key = read_string(input)?;
                let value = read_string(input)?;
                let mut values = Vec::with_capacity(1);
                values.push(value);
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, OBJ_STRING)
                );
            }
            RDB_TYPE_HASH_ZIPLIST | RDB_TYPE_ZSET_ZIPLIST | RDB_TYPE_LIST_ZIPLIST => {
                let key = read_string(input)?;
                let bytes = read_string(input)?;
                let cursor = &mut Cursor::new(&bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let mut count = cursor.read_u16::<LittleEndian>()? as usize;
                let mut values = Vec::with_capacity(count + 1);
                
                while count > 0 {
                    let field_name = read_zip_list_entry(cursor)?;
                    let field_val = read_zip_list_entry(cursor)?;
                    values.push(field_name);
                    values.push(field_val);
                    count -= 2;
                }
                let _type;
                if data_type == RDB_TYPE_HASH_ZIPLIST {
                    _type = OBJ_HASH;
                } else if data_type == RDB_TYPE_ZSET_ZIPLIST {
                    _type = OBJ_ZSET;
                } else {
                    _type = OBJ_LIST;
                }
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, _type)
                );
            }
            RDB_TYPE_LIST_QUICKLIST => {
                let key = read_string(input)?;
                let (count, _) = read_length(input)?;
                let mut values = vec![];
                
                let mut index = 0;
                while index < count {
                    let data = read_string(input)?;
                    let cursor = &mut Cursor::new(&data);
                    // 跳过ZL_BYTES和ZL_TAIL
                    cursor.set_position(8);
                    let mut count = cursor.read_u16::<LittleEndian>()? as usize;
                    while count > 0 {
                        values.push(read_zip_list_entry(cursor)?);
                        count -= 1;
                    }
                    index += 1;
                }
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, OBJ_LIST));
            }
            RDB_TYPE_LIST | RDB_TYPE_SET => {
                let key = read_string(input)?;
                let (mut count, _) = read_length(input)?;
                let mut values = Vec::with_capacity((count as usize) + 1);
                while count > 0 {
                    values.push(read_string(input)?);
                    count -= 1;
                }
                let _type;
                if data_type == RDB_TYPE_LIST {
                    _type = OBJ_LIST;
                } else {
                    _type = OBJ_SET;
                }
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, _type));
            }
            RDB_TYPE_ZSET => {
                let key = read_string(input)?;
                let (mut count, _) = read_length(input)?;
                let mut values = Vec::new();
                while count > 0 {
                    let element = read_string(input)?;
                    let score = read_double(input)?;
                    values.push(element);
                    values.push(score);
                    count -= 1;
                }
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, OBJ_ZSET));
            }
            RDB_TYPE_ZSET_2 => {
                let key = read_string(input)?;
                let (mut count, _) = read_length(input)?;
                let mut values = Vec::new();
                while count > 0 {
                    let element = read_string(input)?;
                    let score = input.read_i64::<LittleEndian>()?;
                    let score_str = score.to_string().into_bytes();
                    values.push(element);
                    values.push(score_str);
                    count -= 1;
                }
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, OBJ_ZSET));
            }
            RDB_TYPE_HASH => {
                let key = read_string(input)?;
                let (mut count, _) = read_length(input)?;
                let mut values = Vec::new();
                while count > 0 {
                    let field = read_string(input)?;
                    let value = read_string(input)?;
                    values.push(field);
                    values.push(value);
                    count -= 1;
                }
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, OBJ_HASH));
            }
            RDB_TYPE_HASH_ZIPMAP => {
                let key = read_string(input)?;
                let bytes = read_string(input)?;
                let mut values = Vec::new();
                let cursor = &mut Cursor::new(&bytes);
                cursor.set_position(1);
                loop {
                    let zm_len = read_zm_len(cursor)?;
                    if zm_len == 255 {
                        break;
                    }
                    let mut field = Vec::with_capacity(zm_len as usize);
                    cursor.read_exact(&mut field)?;
                    values.push(field);
                    let zm_len = read_zm_len(cursor)?;
                    if zm_len == 255 {
                        values.push([].to_vec());
                        break;
                    }
                    let free = cursor.read_i8()?;
                    let mut value = Vec::with_capacity(zm_len as usize);
                    cursor.read_exact(&mut value)?;
                    cursor.set_position(cursor.position() + free as u64);
                    values.push(value);
                }
                handlers.iter().for_each(|handler|
                    handler.handle(&key, &values, OBJ_HASH));
            }
            RDB_TYPE_SET_INTSET => {
                // TODO
            }
            RDB_OPCODE_EOF => {
                if rdb_version >= 5 {
                    read_integer(input, 8, true)?;
                }
                break;
            }
            _ => break
        };
    };
    Ok(Empty)
}

// 当redis响应的数据是Bulk string时，使用此方法读取指定length的字节, 并返回
pub(crate) fn read_bytes(input: &mut dyn Read, length: isize, _: &Vec<Box<dyn RdbEventHandler>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    if length > 0 {
        let mut bytes = vec![];
        for _ in 0..length {
            bytes.push(input.read_u8()?);
        }
        let end = &mut [0; 2];
        input.read_exact(end)?;
        if end == b"\r\n" {
            return Ok(Bytes(bytes));
        } else {
            return Err(Error::new(ErrorKind::Other, "Expect CRLF after bulk string"));
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

// 读取一个double
fn read_double(input: &mut dyn Read) -> Result<Vec<u8>, Error> {
    let len = input.read_u8()?;
    match len {
        255 => {
            // TODO NEGATIVE_INFINITY
        }
        254 => {
            // TODO POSITIVE_INFINITY
        }
        253 => {
            // TODO NaN
        }
        _ => {
            let mut buff = Vec::with_capacity(len as usize);
            input.read_exact(&mut buff)?;
            return Ok(buff);
        }
    }
    Ok(Vec::new())
}

// 读取一个string
fn read_string(input: &mut dyn Read) -> Result<Vec<u8>, Error> {
    let (length, is_encoded) = read_length(input)?;
    if is_encoded {
        match length {
            RDB_ENC_INT8 => {
                let int = input.read_i8()?;
                return Ok(int.to_string().into_bytes());
            }
            RDB_ENC_INT16 => {
                let int = read_integer(input, 2, false)?;
                return Ok(int.to_string().into_bytes());
            }
            RDB_ENC_INT32 => {
                let int = read_integer(input, 4, false)?;
                return Ok(int.to_string().into_bytes());
            }
            RDB_ENC_LZF => {
                let (compressed_len, _) = read_length(input)?;
                let (origin_len, _) = read_length(input)?;
                let mut compressed = vec![0; compressed_len as usize];
                input.read_exact(&mut compressed)?;
                let mut origin = vec![0; origin_len as usize];
                lzf::decompress(&mut compressed, compressed_len, &mut origin, origin_len);
                return Ok(origin);
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid string length"))
        };
    };
    let mut buff = vec![0; length as usize];
    input.read_exact(&mut buff)?;
    Ok(buff)
}

// 读取redis响应中下一条数据的长度
fn read_length(input: &mut dyn Read) -> Result<(isize, bool), Error> {
    let byte = input.read_u8()?;
    let _type = (byte & 0xC0) >> 6;
    
    let mut result = -1;
    let mut is_encoded = false;
    
    if _type == RDB_ENCVAL {
        result = (byte & 0x3F) as isize;
        is_encoded = true;
    } else if _type == RDB_6BITLEN {
        result = (byte & 0x3F) as isize;
    } else if _type == RDB_14BITLEN {
        let next_byte = input.read_u8()?;
        result = (((byte as u16 & 0x3F) << 8) | next_byte as u16) as isize;
    } else if byte == RDB_32BITLEN {
        result = read_integer(input, 4, true)?;
    } else if byte == RDB_64BITLEN {
        result = read_integer(input, 8, true)?;
    };
    Ok((result, is_encoded))
}

fn read_integer(input: &mut dyn Read, size: isize, is_big_endian: bool) -> Result<isize, Error> {
    let mut buff = vec![0; size as usize];
    input.read_exact(&mut buff)?;
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

fn read_zm_len(cursor: &mut Cursor<&Vec<u8>>) -> Result<usize, Error> {
    let len = cursor.read_u8()?;
    if len >= 0 && len <= 253 {
        return Ok(len as usize);
    } else if len == 254 {
        let value = cursor.read_u32::<BigEndian>()?;
        return Ok(value as usize);
    }
    Ok(len as usize)
}

fn read_zip_list_entry(cursor: &mut Cursor<&Vec<u8>>) -> Result<Vec<u8>, Error> {
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
