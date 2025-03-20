/*!
Redis Serialization Protocol相关的解析代码
*/

use std::io::{Read, Result};

use byteorder::ReadBytesExt;

use crate::to_string;

/// Redis Serialization Protocol解析
pub trait RespDecode: Read {
    /// 读取并解析Redis响应
    fn decode_resp(&mut self) -> Result<Resp> {
        match self.decode_type()? {
            Type::String => Ok(Resp::String(self.decode_string()?)),
            Type::Int => self.decode_int(),
            Type::Error => Ok(Resp::Error(self.decode_string()?)),
            Type::BulkString => self.decode_bulk_string(),
            Type::Array => self.decode_array(),
        }
    }
    /// 读取解析Redis响应的类型
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
                    DOLLAR => return Ok(Type::BulkString),
                    STAR => return Ok(Type::Array),
                    _ => panic!("Unexpected Data Type: {}", b),
                }
            }
        }
    }
    /// 解析Simple String响应
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

    /// 解析Integer响应
    fn decode_int(&mut self) -> Result<Resp> {
        let s = self.decode_string()?;
        let i = s.parse::<i64>().unwrap();
        return Ok(Resp::Int(i));
    }

    /// 解析Bulk String响应
    fn decode_bulk_string(&mut self) -> Result<Resp> {
        let r = self.decode_int()?;
        if let Resp::Int(i) = r {
            if i > 0 {
                let mut buf = vec![0; i as usize];
                self.read_exact(&mut buf)?;
                let mut end = vec![0; 2];
                self.read_exact(&mut end)?;
                if !end.eq(&[CR, LF]) {
                    panic!("Expected CRLF");
                } else {
                    return Ok(Resp::BulkBytes(buf));
                }
            } else {
                self.read_exact(&mut [0; 2])?;
                return Ok(Resp::BulkBytes(vec![0; 0]));
            }
        } else {
            panic!("Expected Int Response");
        }
    }

    /// 解析Array响应
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
}

impl<R: Read + ?Sized> RespDecode for R {}

pub enum Type {
    String,
    Error,
    Int,
    BulkString,
    Array,
}

#[derive(Debug)]
pub enum Resp {
    String(String),
    Int(i64),
    Error(String),
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
            Err(err) => panic!("{}", err),
        }
    }
}
