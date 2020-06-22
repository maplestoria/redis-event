use std::io::{Cursor, Error, ErrorKind, Read, Result};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::rdb::*;
use crate::{lzf, to_string};
use std::f64::{INFINITY, NAN, NEG_INFINITY};

pub trait RespDecode: Read {
    fn decode_resp(&mut self) -> Result<Resp> {
        match self.decode_type()? {
            Type::String => Ok(Resp::String(self.decode_string()?)),
            Type::Int => self.decode_int(),
            Type::Error => Err(Error::new(ErrorKind::InvalidData, self.decode_string()?)),
            Type::BulkBytes => self.decode_bulk_bytes(),
            Type::Array => self.decode_array(),
        }
    }

    fn decode_type(&mut self) -> Result<Type> {
        loop {
            let b = self.read_u8()?;
            if b == LF {
                continue;
            } else {
                match b {
                    PLUS => return Ok(Type::String),
                    MINUS => return Ok(Type::Error),
                    COLON => return Ok(Type::Int),
                    DOLLAR => return Ok(Type::BulkBytes),
                    STAR => return Ok(Type::Array),
                    _ => panic!("Unexpected Data Type: {}", b),
                }
            }
        }
    }

    fn decode_string(&mut self) -> Result<String> {
        let mut buf = vec![];
        loop {
            let byte = self.read_u8()?;
            if byte != CR {
                buf.push(byte);
            } else {
                break;
            }
        }
        if self.read_u8()? == LF {
            Ok(to_string(buf))
        } else {
            panic!("Expect LF after CR");
        }
    }

    fn decode_int(&mut self) -> Result<Resp> {
        let s = self.decode_string()?;
        let i = s.parse::<i64>().unwrap();
        return Ok(Resp::Int(i));
    }

    fn decode_bulk_bytes(&mut self) -> Result<Resp> {
        let r = self.decode_int()?;
        if let Resp::Int(i) = r {
            if i > 0 {
                let mut buf = vec![0; i as usize];
                self.read_exact(&mut buf)?;let mut end = vec![0; 2];
                self.read_exact(&mut end)?;
                if !end.eq(&[CR, LF]) {
                    panic!("Expected CRLF");
                } else {
                    return Ok(Resp::BulkBytes(buf));
                }
            } else {
                self.read_exact(&mut [0; 2])?;
                return Ok(Resp::BulkBytes(vec![0;0]));
            }
        } else {
            panic!("Expected Int Response");
        }
    }

    fn decode_array(&mut self) -> Result<Resp> {
        let r = self.decode_int()?;
        if let Resp::Int(i) = r {
            let mut arr = Vec::with_capacity(i as usize);
            for _ in 0..i {
                let resp = self.decode_resp()?;
                arr.push(resp);
            }
            return Ok(Resp::Array(arr));
        } else {
            panic!("Expected Int Response");
        }
    }

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
                _ => panic!("Invalid string length: {}", length),
            };
        };
        let mut buff = vec![0; length as usize];
        self.read_exact(&mut buff)?;
        Ok(buff)
    }

    // 从流中读取一个double
    fn read_double(&mut self) -> Result<f64> {
        let len = self.read_u8()?;
        return match len {
            255 => Ok(NEG_INFINITY),
            254 => Ok(INFINITY),
            253 => Ok(NAN),
            _ => {
                let mut buff = vec![0; len as usize];
                self.read_exact(&mut buff)?;
                let score_str = to_string(buff);
                let score = score_str.parse::<f64>().unwrap();
                Ok(score)
            }
        };
    }
}

impl<R: Read + ?Sized> RespDecode for R {}

pub enum Type {
    String,
    Error,
    Int,
    BulkBytes,
    Array,
}

#[derive(Debug)]
pub enum Resp {
    String(String),
    Int(i64),
    BulkBytes(Vec<u8>),
    Array(Vec<Resp>),
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

pub const MODULE_SET: [char; 64] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '-', '_',
];

#[cfg(test)]
mod test {
    use crate::resp::{Resp, RespDecode};
    use std::io::Cursor;

    #[test]
    fn test_decode_array() {
        let b = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n";
        let mut cursor = Cursor::new(b);
        let r = cursor.decode_resp();
        match r {
            Ok(resp) => match resp {
                Resp::Array(arr) => {
                    let mut data = Vec::new();
                    for x in arr {
                        match x {
                            Resp::BulkBytes(bytes) => data.push(bytes),
                            _ => panic!("wrong type"),
                        }
                    }
                    assert!(b"SELECT".eq(data.get(0).unwrap().as_slice()));
                    assert!(b"0".eq(data.get(1).unwrap().as_slice()));
                }
                _ => panic!("wrong type"),
            },
            Err(err) => panic!(err),
        }
    }
}
