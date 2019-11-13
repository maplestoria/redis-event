use std::io::{BufWriter, Error, ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::result::Result;
use std::result::Result::Ok;

use byteorder::ReadBytesExt;

use crate::config::Config;
use crate::Data::{Bytes, Empty};

mod config;

pub trait RedisEventListener {
    fn open(&mut self) -> Result<(), Error>;

    fn close(&self);
}

// 用于监听Redis单点的事件
pub struct StandaloneEventListener {
    addr: SocketAddr,
    password: &'static str,
    config: Config,
    stream: Option<TcpStream>,
    id: &'static str,
    offset: i64,
}

impl StandaloneEventListener {
    fn connect(&mut self) -> Result<(), Error> {
        let stream = TcpStream::connect(self.addr)?;
        println!("connected to server!");
        self.stream = Option::Some(stream);
        Ok(())
    }

    fn auth(&mut self) -> Result<(), Error> {
        if !self.password.is_empty() {
            self.send(b"AUTH", &[self.password.as_bytes()])?;
            self.response(read_bytes)?;
        }
        Ok(())
    }

    fn send_port(&mut self) -> Result<(), Error> {
        let stream = self.stream.as_ref().unwrap();
        let port = stream.local_addr()?.port().to_string();
        let port = port.as_bytes();
        self.send(b"REPLCONF", &[b"listening-port", port])?;
        self.response(read_bytes)?;
        Ok(())
    }

    fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<(), Error> {
        let stream = self.stream.as_ref().unwrap();
        let mut writer = BufWriter::new(stream);
        writer.write(&[STAR])?;
        let args_len = args.len() + 1;
        writer.write(args_len.to_string().as_bytes())?;
        writer.write(&[CR, LF, DOLLAR])?;
        writer.write(command.len().to_string().as_bytes())?;
        writer.write(&[CR, LF])?;
        writer.write(command)?;
        writer.write(&[CR, LF])?;
        for arg in args {
            writer.write(&[DOLLAR])?;
            writer.write(arg.len().to_string().as_bytes())?;
            writer.write(&[CR, LF])?;
            writer.write(arg)?;
            writer.write(&[CR, LF])?;
        }
        writer.flush()
    }

    fn response(&mut self, func: fn(&mut dyn Read, isize) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error>)
                -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
        let mut socket = self.stream.as_ref().unwrap();
        let response_type = socket.read_u8()?;
        match response_type {
            // Plus: Simple String
            // Minus: Error
            // Colon: Integer
            PLUS | MINUS | COLON => {
                let mut bytes = vec![];
                loop {
                    let byte = socket.read_u8()?;
                    if byte != CR {
                        bytes.push(byte);
                    } else {
                        break;
                    }
                }
                let byte = socket.read_u8()?;
                if byte == LF {
                    if response_type == PLUS || response_type == COLON {
                        return Ok(Bytes(bytes));
                    } else {
                        let message = String::from_utf8(bytes).unwrap();
                        return Err(Error::new(ErrorKind::Other, message));
                    }
                } else {
                    return Err(Error::new(ErrorKind::Other, "Expect LF after CR"));
                }
            }
            DOLLAR => { // Bulk String
                let mut bytes = vec![];
                loop {
                    let byte = socket.read_u8()?;
                    if byte != CR {
                        bytes.push(byte);
                    } else {
                        break;
                    }
                }
                let byte = socket.read_u8()?;
                if byte == LF {
                    let length = String::from_utf8(bytes).unwrap();
                    let length = length.parse::<isize>().unwrap();
                    let stream = self.stream.as_mut().unwrap();
                    func(stream, length)
                } else {
                    return Err(Error::new(ErrorKind::Other, "Expect LF after CR"));
                }
            }
            STAR => { // Array
                Ok(Empty)
            }
            _ => {
                Ok(Empty)
            }
        }
    }

    fn new(addr: SocketAddr, password: &'static str) -> StandaloneEventListener {
        StandaloneEventListener {
            addr,
            password,
            config: config::default(),
            stream: Option::None,
            id: "?",
            offset: -1,
        }
    }
}

impl RedisEventListener for StandaloneEventListener {
    fn open(&mut self) -> Result<(), Error> {
        self.connect()?;
        self.auth()?;
        self.send_port()?;

        let offset = self.offset.to_string();
        let replica_offset = offset.as_bytes();

        self.send(b"PSYNC", &[self.id.as_bytes(), replica_offset])?;
        let data = self.response(read_bytes)?;
        if let Bytes(resp) = data {
            let resp = String::from_utf8(resp).unwrap();
            if resp.starts_with("FULLRESYNC") {
                panic!("{}", resp);
            }
        } else {
            return Err(Error::new(ErrorKind::Other, "Expect Redis string response"));
        }
        Ok(())
    }

    fn close(&self) {
        let option = self.stream.as_ref();
        if self.stream.is_some() {
            println!("close connection with server...");
            option.unwrap().shutdown(Shutdown::Both).unwrap();
        }
    }
}

// 用于包装redis的返回值
enum Data<B, V> {
    // 包装Vec<u8>
    Bytes(B),
    // 包装Vec<Vec<u8>>
    BytesVec(V),
    // 空返回
    Empty,
}

// 当redis响应的数据是Bulk string时，使用此方法读取指定length的字节, 并返回
fn read_bytes(socket: &mut dyn Read, length: isize) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    if length > 0 {
        let mut bytes = vec![];
        for _ in 0..length {
            bytes.push(socket.read_u8()?);
        }
        let end = &mut [0; 2];
        socket.read_exact(end)?;
        if end == b"\r\n" {
            return Ok(Bytes(bytes));
        } else {
            return Err(Error::new(ErrorKind::Other, "Expect CRLF after bulk string"));
        }
    } else if length == 0 {
        // length == 0 代表空字符，后面还有CRLF
        socket.read_exact(&mut [0; 2])?;
        return Ok(Empty);
    } else {
        // length < 0 代表null
        return Ok(Empty);
    }
}

// 读取指定length的字节, 按照rdb的规则解析后再返回数据
fn parse_rdb(socket: &mut dyn Read, length: isize) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    Ok(Empty)
}

// 以下是redis响应中的常量字符
// 回车换行，在redis响应中一般表示终结符，或用作分隔符以分隔数据
const CR: u8 = b'\r';
const LF: u8 = b'\n';
// 代表array响应
const STAR: u8 = b'*';
// 代表bulk string响应
const DOLLAR: u8 = b'$';
// 代表simple string响应
const PLUS: u8 = b'+';
// 代表error响应
const MINUS: u8 = b'-';
// 代表integer响应
const COLON: u8 = b':';

// 测试用例
#[cfg(test)]
mod tests {
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;

    use crate::{RedisEventListener, StandaloneEventListener};

    #[test]
    fn open() {
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let mut redis_listener = StandaloneEventListener::new(SocketAddr::new(ip, 6379), "123456");
        if let Err(error) = redis_listener.open() {
            panic!("couldn't connect to server: {}", error)
        }
        redis_listener.close();
    }
}
