use std::io::{BufWriter, Error, ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::result::Result;
use std::result::Result::Ok;

use byteorder::ReadBytesExt;

use crate::config::Config;
use crate::constants::*;

mod constants;
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
            self.response()?;
        }
        Ok(())
    }

    fn send_port(&mut self) -> Result<(), Error> {
        let stream = self.stream.as_ref().unwrap();
        let port = stream.local_addr()?.port().to_string();
        let port = port.as_bytes();
        self.send(b"REPLCONF", &[b"listening-port", port]);
        self.response()?;
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

    fn response(&mut self) -> Result<Vec<u8>, Error> {
        let mut socket = self.stream.as_ref().unwrap();
        let response_type = socket.read_u8()?;
        match response_type {
            PLUS | MINUS => { // Plus: Simple String; Minus: Error
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
                    if response_type == PLUS {
                        return Ok(bytes);
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
                    if length > 0 {
                        let mut bytes = vec![];
                        for _ in 0..length {
                            bytes.push(socket.read_u8()?);
                        }
                        let end = &mut [0; 2];
                        socket.read_exact(end)?;
                        if end == b"\r\n" {
                            return Ok(bytes);
                        } else {
                            return Err(Error::new(ErrorKind::Other, "Expect CRLF after bulk string"));
                        }
                    } else if length == 0 {
                        // length == 0 代表空字符，后面还有CRLF
                        socket.read_exact(&mut [0; 2])?;
                        return Ok(Vec::default());
                    } else {
                        // length < 0 代表null
                        return Ok(Vec::default());
                    }
                } else {
                    return Err(Error::new(ErrorKind::Other, "Expect LF after CR"));
                }
            }
            _ => {
                Ok(Vec::default())
            }
        }
    }
}

impl RedisEventListener for StandaloneEventListener {
    fn open(&mut self) -> Result<(), Error> {
        self.connect()?;
        self.auth()?;
        self.send_port();

        let offset = self.offset.to_string();
        let replica_offset = offset.as_bytes();

        self.send(b"PSYNC", &[self.id.as_bytes(), replica_offset]);
        let resp = self.response()?;
        let resp = String::from_utf8(resp).unwrap();
        panic!("resp: {}", resp);


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

pub fn new(addr: SocketAddr, password: &'static str) -> StandaloneEventListener {
    StandaloneEventListener {
        addr,
        password,
        config: config::default(),
        stream: Option::None,
        id: "?",
        offset: -1,
    }
}

// 测试用例
#[cfg(test)]
mod tests {
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;

    use crate::{new, RedisEventListener};

    #[test]
    fn open() {
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let mut redis_listener = new(SocketAddr::new(ip, 6379), "123456");
        if let Err(error) = redis_listener.open() {
            panic!("couldn't connect to server: {}", error)
        }
        redis_listener.close();
    }
}
