pub mod standalone {
    use std::io::{BufWriter, Error, ErrorKind, Read, Write};
    use std::net::{Shutdown, SocketAddr, TcpStream};
    use std::result::Result;
    use std::result::Result::Ok;
    
    use byteorder::ReadBytesExt;
    
    use crate::{config, EventHandler, rdb, RedisListener};
    use crate::config::Config;
    use crate::rdb::{COLON, CR, Data, DOLLAR, LF, MINUS, PLUS, read_bytes, STAR};
    use crate::rdb::Data::{Bytes, BytesVec, Empty};
    
    // 用于监听单个Redis实例的事件
    pub struct Listener {
        addr: SocketAddr,
        password: &'static str,
        config: Config,
        stream: Option<TcpStream>,
        id: &'static str,
        offset: i64,
        rdb_listener: Vec<Box<dyn EventHandler>>,
        command_listener: Vec<Box<dyn EventHandler>>,
    }
    
    impl Listener {
        fn connect(&mut self) -> Result<(), Error> {
            let stream = TcpStream::connect(self.addr)?;
            println!("connected to server!");
            self.stream = Option::Some(stream);
            Ok(())
        }
        
        fn auth(&mut self) -> Result<(), Error> {
            if !self.password.is_empty() {
                self.send(b"AUTH", &[self.password.as_bytes()])?;
                self.response(rdb::read_bytes)?;
            }
            Ok(())
        }
        
        fn send_port(&mut self) -> Result<(), Error> {
            let stream = self.stream.as_ref().unwrap();
            let port = stream.local_addr()?.port().to_string();
            let port = port.as_bytes();
            self.send(b"REPLCONF", &[b"listening-port", port])?;
            self.response(rdb::read_bytes)?;
            Ok(())
        }
        
        fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<(), Error> {
            let stream = self.stream.as_ref().unwrap();
            let mut writer = BufWriter::new(stream);
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
        
        fn response(&mut self, func: fn(&mut dyn Read, isize, &mut Vec<Box<dyn EventHandler>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error>)
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
                            return Err(Error::new(ErrorKind::InvalidInput, message));
                        }
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Expect LF after CR"));
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
                        return func(stream, length, &mut self.rdb_listener);
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Expect LF after CR"));
                    }
                }
                STAR => { // Array
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
                        if length <= 0 {
                            return Ok(Empty);
                        } else {
                            let mut result = vec![];
                            for _ in 0..length {
                                match self.response(read_bytes)? {
                                    Bytes(resp) => {
                                        result.push(resp);
                                    }
                                    BytesVec(mut resp) => {
                                        result.append(&mut resp);
                                    }
                                    Empty => {
                                        return Err(Error::new(ErrorKind::InvalidData, "Expect Redis response, but got empty"));
                                    }
                                }
                            }
                            return Ok(BytesVec(result));
                        }
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Expect LF after CR"));
                    }
                }
                _ => {}
            }
            Ok(Empty)
        }
        
        fn add_rdb_listener(&mut self, listener: Box<dyn EventHandler>) {
            self.rdb_listener.push(listener)
        }
        
        fn add_command_listener(&mut self, listener: Box<dyn EventHandler>) {
            self.command_listener.push(listener)
        }
    }
    
    impl RedisListener for Listener {
        fn open(&mut self) -> Result<(), Error> {
            self.connect()?;
            self.auth()?;
            self.send_port()?;
            
            let offset = self.offset.to_string();
            let replica_offset = offset.as_bytes();
            
            self.send(b"PSYNC", &[self.id.as_bytes(), replica_offset])?;
            if let Bytes(resp) = self.response(read_bytes)? {
                let resp = String::from_utf8(resp).unwrap();
                if resp.starts_with("FULLRESYNC") {
                    self.response(rdb::parse)?;
                }
            } else {
                return Err(Error::new(ErrorKind::InvalidData, "Expect Redis string response"));
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
    
    pub(crate) fn new(addr: SocketAddr, password: &'static str) -> Listener {
        Listener {
            addr,
            password,
            config: config::default(),
            stream: Option::None,
            id: "?",
            offset: -1,
            rdb_listener: Vec::new(),
            command_listener: Vec::new(),
        }
    }
}

pub mod cluster {}

pub mod sentinel {}