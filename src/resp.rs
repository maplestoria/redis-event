/*!
 处理redis的响应数据
*/

use std::io::{BufWriter, Error, ErrorKind, Read, Result, Write};
use std::net::TcpStream;

use byteorder::ReadBytesExt;

use crate::{CommandHandler, rdb, RdbHandler, to_string};
use crate::rdb::Data;
use crate::rdb::Data::{Bytes, BytesVec, Empty};

pub(crate) struct Conn<T> {
    pub(crate) input: Box<T>,
}

pub(crate) fn new(input: TcpStream) -> Conn<TcpStream> {
    Conn { input: Box::new(input) }
}

impl<T: Read + Write> Conn<T> {
    pub(crate) fn reply(&mut self,
                        func: fn(input: &mut dyn Read, isize,
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
    
    pub(crate) fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<()> {
        send(&mut self.input, command, args)?;
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
