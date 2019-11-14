use std::io::{Cursor, Error, ErrorKind, Read};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::{Data, EventListener, lzf};
use crate::Data::Empty;

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

// 代表 aux field
const AUX: u8 = 0xFA;
// 当前redis db
const SELECT_DB: u8 = 0xFE;
// db的key数量
const DB_SIZE: u8 = 0xFB;
// 代表字符串的数据
const STRING: u8 = 0;
const LIST: u8 = 1;
const SET: u8 = 2;
const ZSET_ZIP_LIST: u8 = 12;
const HASH_ZIP_LIST: u8 = 13;
const LIST_QUICK_LIST: u8 = 14;
// end of file
const EOF: u8 = 0xFF;
const ZIP_INT_8BIT: u8 = 0xFE;
const ZIP_INT_16BIT: u8 = 0xC0;
const ZIP_INT_24BIT: u8 = 0xF0;
const ZIP_INT_32BIT: u8 = 0xD0;
const ZIP_INT_64BIT: u8 = 0xE0;

// 读取、解析rdb
pub(crate) fn parse(socket: &mut dyn Read,
                    length: isize,
                    rdb_listeners: &mut Vec<Box<dyn EventListener>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    println!("rdb size: {} bytes", length);
    let mut bytes = vec![0; 5];
    // 开头5个字节: REDIS
    socket.read_exact(&mut bytes)?;
    // 4个字节: rdb版本
    socket.read_exact(&mut bytes[..=3])?;
    let rdb_version = String::from_utf8(bytes[..=3].to_vec()).unwrap();
    let rdb_version = rdb_version.parse::<isize>().unwrap();
    loop {
        let data_type = socket.read_u8()?;
        match data_type {
            AUX => {
                let field_name = read_string(socket)?;
                let field_val = read_string(socket)?;
                let field_name = String::from_utf8(field_name).unwrap();
                let field_val = String::from_utf8(field_val).unwrap();
                println!("{}:{}", field_name, field_val);
            }
            SELECT_DB => {
                let db = read_length(socket)?;
                println!("db: {}", db.val);
            }
            DB_SIZE => {
                let db = read_length(socket)?;
                println!("db total keys: {}", db.val);
                let db = read_length(socket)?;
                println!("db expired keys: {}", db.val);
            }
            STRING => {
                let key = read_string(socket)?;
                let value = read_string(socket)?;
            }
            HASH_ZIP_LIST | ZSET_ZIP_LIST => {
                let key = read_string(socket)?;
                let bytes = read_string(socket)?;
                let cursor = &mut Cursor::new(&bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let mut count = cursor.read_u16::<LittleEndian>()? as usize;
                let mut args: Vec<Vec<u8>> = Vec::with_capacity(count + 1);
                args.push(key);
                
                while count > 0 {
                    let field_name = read_zip_list_entry(cursor)?;
                    let field_val = read_zip_list_entry(cursor)?;
                    args.push(field_name);
                    args.push(field_val);
                    count -= 2;
                }
            }
            LIST_QUICK_LIST => {
                let key = read_string(socket)?;
                let count = read_length(socket)?;
                let mut args: Vec<Vec<u8>> = vec![];
                args.insert(0, key);
                
                let mut index = 0;
                while index < count.val {
                    let data = read_string(socket)?;
                    let cursor = &mut Cursor::new(&data);
                    // 跳过ZL_BYTES和ZL_TAIL
                    cursor.set_position(8);
                    let mut count = cursor.read_u16::<LittleEndian>()? as usize;
                    while count > 0 {
                        let list_item = read_zip_list_entry(cursor)?;
                        args.push(list_item);
                        count -= 1;
                    }
                    index += 1;
                }
            }
            LIST | SET => {
                let key = read_string(socket)?;
                let mut count = read_length(socket)?;
                let mut args: Vec<Vec<u8>> = Vec::with_capacity((count.val as usize) + 1);
                args.push(key);
                while count.val > 0 {
                    args.push(read_string(socket)?);
                    count.val -= 1;
                }
            }
            EOF => {
                if rdb_version >= 5 {
                    read_integer(socket, 8, true)?;
                }
                break;
            }
            _ => break
        };
    };
    Ok(Empty)
}

// 读取一个string
fn read_string(socket: &mut dyn Read) -> Result<Vec<u8>, Error> {
    let length = read_length(socket)?;
    if length.is_special {
        match length.val {
            0 => {
                let int = socket.read_i8()?;
                return Ok(int.to_string().into_bytes());
            }
            1 => {
                let int = read_integer(socket, 2, false)?;
                return Ok(int.to_string().into_bytes());
            }
            2 => {
                let int = read_integer(socket, 4, false)?;
                return Ok(int.to_string().into_bytes());
            }
            3 => {
                let compressed_len = read_length(socket)?;
                let origin_len = read_length(socket)?;
                let mut compressed = vec![0; compressed_len.val as usize];
                socket.read_exact(&mut compressed)?;
                let mut origin = vec![0; origin_len.val as usize];
                lzf::decompress(&mut compressed, compressed_len.val, &mut origin, origin_len.val);
                return Ok(origin);
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid string length"))
        };
    };
    let mut buff = vec![0; length.val as usize];
    socket.read_exact(&mut buff)?;
    Ok(buff)
}

struct Length {
    val: isize,
    is_special: bool,
}

// 读取redis响应中下一条数据的长度
fn read_length(socket: &mut dyn Read) -> Result<Length, Error> {
    let byte = socket.read_u8()?;
    
    let byte_and = byte & 0xFF;
    
    let _type = (byte & 0xC0) >> 6;
    
    let mut result = -1;
    let mut is_special = false;
    
    if _type == 3 {
        result = (byte & 0x3F) as isize;
        is_special = true;
    } else if _type == 0 {
        result = (byte & 0x3F) as isize;
    } else if _type == 1 {
        let next_byte = socket.read_u8()?;
        result = (((byte as u16 & 0x3F) << 8) | next_byte as u16) as isize;
    } else if byte_and == 0x80 {
        result = read_integer(socket, 4, true)?;
    } else if byte_and == 0x81 {
        result = read_integer(socket, 8, true)?;
    };
    Ok(Length { val: result, is_special })
}

fn read_integer(socket: &mut dyn Read, size: isize, is_big_endian: bool) -> Result<isize, Error> {
    let mut buff = vec![0; size as usize];
    socket.read_exact(&mut buff)?;
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
