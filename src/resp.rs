/*!
 处理redis的响应数据
*/

use std::io::{Error, ErrorKind, Read, Result, Write};
use std::net::TcpStream;

use byteorder::{ByteOrder, ReadBytesExt};

use crate::{CommandHandler, rdb, RdbHandler, to_string};
use crate::rdb::Data;
use crate::rdb::Data::{Bytes, BytesVec, Empty};

pub(crate) struct Conn {
    pub(crate) input: TcpStream,
    len: i64,
    marked: bool,
}

impl Conn {
    pub(crate) fn new(input: TcpStream) -> Conn {
        Conn { input, len: 0, marked: false }
    }
    
    pub(crate) fn reply(&mut self,
                        func: fn(input: &mut TcpStream, isize,
                                 &mut dyn RdbHandler,
                                 &mut dyn CommandHandler,
                        ) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>>,
                        rdb_handler: &mut dyn RdbHandler,
                        cmd_handler: &mut dyn CommandHandler) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
        loop {
            let conn = &mut self.input;
            let response_type = conn.read_u8()?;
            match response_type {
                // Plus: Simple String
                // Minus: Error
                // Colon: Integer
                PLUS | MINUS | COLON => {
                    let mut bytes = vec![];
                    loop {
                        let byte = conn.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = conn.read_u8()?;
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
                        let byte = conn.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = conn.read_u8()?;
                    if byte == LF {
                        let length = to_string(bytes);
                        let length = length.parse::<isize>().unwrap();
                        return func(&mut self.input, length, rdb_handler, cmd_handler);
                    } else {
                        panic!("Expect LF after CR");
                    }
                }
                STAR => { // Array
                    let mut bytes = vec![];
                    loop {
                        let byte = conn.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = conn.read_u8()?;
                    if byte == LF {
                        let length = to_string(bytes);
                        let length = length.parse::<isize>().unwrap();
                        if length <= 0 {
                            return Ok(Empty);
                        } else {
                            let mut result = Vec::with_capacity(length as usize);
                            for _ in 0..length {
                                match self.reply(rdb::read_bytes, rdb_handler, cmd_handler)? {
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
    
    pub(crate) fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<()> {
        let writer = &mut self.input;
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
        writer.flush()?;
        Ok(())
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
