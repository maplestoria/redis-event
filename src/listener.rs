pub mod standalone {
    use std::io::{BufWriter, Error, ErrorKind, Read, Write};
    use std::net::{Shutdown, SocketAddr, TcpStream};
    use std::result::Result;
    use std::result::Result::Ok;
    
    use byteorder::ReadBytesExt;
    
    use crate::{CommandHandler, config, rdb, RdbEventHandler, RedisListener};
    use crate::config::Config;
    use crate::listener::standalone::SyncMode::PSync;
    use crate::rdb::{COLON, CR, Data, DOLLAR, LF, MINUS, PLUS, read_bytes, STAR};
    use crate::rdb::Data::{Bytes, BytesVec, Empty};
    
    // 用于监听单个Redis实例的事件
    pub struct Listener<'a> {
        addr: SocketAddr,
        password: &'a str,
        config: Config,
        stream: Option<TcpStream>,
        repl_id: String,
        repl_offset: i64,
        rdb_listeners: Vec<Box<dyn RdbEventHandler>>,
        cmd_listeners: Vec<Box<dyn CommandHandler>>,
    }
    
    impl Listener<'_> {
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
        
        fn send(&self, command: &[u8], args: &[&[u8]]) -> Result<(), Error> {
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
        
        fn response(&mut self, func: fn(&mut dyn Read, isize, &Vec<Box<dyn RdbEventHandler>>, &Vec<Box<dyn CommandHandler>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error>)
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
                        return func(stream, length, &self.rdb_listeners, &self.cmd_listeners);
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
                                match self.response(rdb::read_bytes)? {
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
        
        pub fn add_rdb_listener(&mut self, listener: Box<dyn RdbEventHandler>) {
            self.rdb_listeners.push(listener)
        }
        
        pub fn add_command_listener(&mut self, listener: Box<dyn CommandHandler>) {
            self.cmd_listeners.push(listener)
        }
        
        fn start_sync(&mut self) -> Result<SyncMode, Error> {
            let offset = self.repl_offset.to_string();
            let repl_offset = offset.as_bytes();
            let repl_id = self.repl_id.as_bytes();
            
            self.send(b"PSYNC", &[repl_id, repl_offset])?;
            if let Bytes(resp) = self.response(rdb::read_bytes)? {
                let resp = String::from_utf8(resp).unwrap();
                if resp.starts_with("FULLRESYNC") {
                    self.response(rdb::parse)?;
                    let mut iter = resp.split_whitespace();
                    if let Some(repl_offset) = iter.nth(1) {
                        self.repl_id = repl_offset.to_owned();
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Expect replication offset"));
                    }
                    if let Some(string) = iter.nth(2) {
                        self.repl_offset = string.parse::<i64>().unwrap();
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Expect replication id"));
                    }
                }
                // TODO 其他返回信息的处理
            } else {
                return Err(Error::new(ErrorKind::InvalidData, "Expect Redis string response"));
            }
            Ok(PSync)
        }
        
        fn start_heartbeat(&mut self) {
            // TODO
        }
        
        fn receive_cmd(&mut self) -> Result<Vec<Vec<u8>>, Error> {
            // read begin
            self.response(rdb::read_bytes)?;
            // read end, and get total bytes read
            // first TODO
            Ok(Vec::new())
        }
    }
    
    impl RedisListener for Listener<'_> {
        fn open(&mut self) -> Result<(), Error> {
            self.connect()?;
            self.auth()?;
            self.send_port()?;
            let mode = self.start_sync()?;
            // TODO check sync mode return
            loop {
                self.receive_cmd()?;
            }
        }
        
        fn close(&self) {
            let option = self.stream.as_ref();
            if self.stream.is_some() {
                println!("close connection with server...");
                option.unwrap().shutdown(Shutdown::Both).unwrap();
            }
        }
    }
    
    pub(crate) fn new(addr: SocketAddr, password: &str) -> Listener {
        Listener {
            addr,
            password,
            config: config::default(),
            stream: Option::None,
            repl_id: String::from("?"),
            repl_offset: -1,
            rdb_listeners: Vec::new(),
            cmd_listeners: Vec::new(),
        }
    }
    
    pub(crate) enum SyncMode {
        PSync,
        Sync,
        SyncLater,
    }
}

pub mod cluster {}

pub mod sentinel {}