pub mod standalone {
    use std::io::{BufWriter, Error, ErrorKind, Write};
    use std::net::{Shutdown, SocketAddr, TcpStream};
    use std::ops::Deref;
    use std::result::Result;
    use std::result::Result::Ok;
    use std::sync::{Arc, mpsc, Mutex};
    use std::thread;
    use std::time::Duration;
    
    use crate::{CommandHandler, config, rdb, RdbEventHandler, RedisListener};
    use crate::config::Config;
    use crate::listener::standalone::SyncMode::PSync;
    use crate::rdb::{COLON, CR, Data, DOLLAR, LF, MINUS, PLUS, STAR};
    use crate::rdb::Data::{Bytes, BytesVec, Empty};
    use crate::reader::Reader;
    
    // 用于监听单个Redis实例的事件
    pub struct Listener<'a> {
        addr: SocketAddr,
        password: &'a str,
        config: Config,
        reader: Option<Reader>,
        repl_id: String,
        repl_offset: i64,
        rdb_listeners: Vec<Box<dyn RdbEventHandler>>,
        cmd_listeners: Vec<Box<dyn CommandHandler>>,
        t_heartbeat: HeartbeatWorker,
        sender: Option<mpsc::Sender<Message>>,
    }
    
    impl Listener<'_> {
        fn connect(&mut self) -> Result<(), Error> {
            let stream = TcpStream::connect(self.addr)?;
            println!("connected to server!");
            let stream = Arc::new(Mutex::new(stream));
            let stream_clone = Arc::clone(&stream);
    
            let (sender, receiver) = mpsc::channel();
    
            let t = thread::spawn(move || {
                let mut offset = 0;
                loop {
                    match receiver.recv_timeout(Duration::from_millis(2000)) {
                        Ok(Message::Terminate) => break,
                        Ok(Message::Some(new_offset)) => {
                            offset = new_offset;
                        }
                        Err(_) => {}
                    }
                    if let Ok(addr) = stream_clone.lock().unwrap().local_addr() {
                        println!("{:?}", addr);
                    }
                    println!("offset: {}", offset);
                }
                println!("terminated");
            });
    
            self.t_heartbeat = HeartbeatWorker { thread: Some(t) };
            self.sender = Some(sender);
            self.reader = Option::Some(Reader::new(Arc::clone(&stream)));
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
            let reader = self.reader.as_ref().unwrap();
    
            let port = reader.stream.lock().unwrap().local_addr()?.port().to_string();
            let port = port.as_bytes();
            self.send(b"REPLCONF", &[b"listening-port", port])?;
            self.response(rdb::read_bytes)?;
            Ok(())
        }
        
        fn send(&self, command: &[u8], args: &[&[u8]]) -> Result<(), Error> {
            let reader = self.reader.as_ref().unwrap();
            let guard = reader.stream.lock().unwrap();
            let mut writer = BufWriter::new(guard.deref());
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
        
        fn response(&mut self, func: fn(&mut Reader, isize, &Vec<Box<dyn RdbEventHandler>>, &Vec<Box<dyn CommandHandler>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error>)
                    -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
            let socket = self.reader.as_mut().unwrap();
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
                        let stream = self.reader.as_mut().unwrap();
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
                    if let Some(repl_id) = iter.nth(1) {
                        self.repl_id = repl_id.to_owned();
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Expect replication id, bot got None"));
                    }
                    if let Some(repl_offset) = iter.next() {
                        self.repl_offset = repl_offset.parse::<i64>().unwrap();
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Expect replication offset, bot got None"));
                    }
                }
                // TODO 其他返回信息的处理
            } else {
                return Err(Error::new(ErrorKind::InvalidData, "Expect Redis string response"));
            }
            Ok(PSync)
        }
        
        fn receive_cmd(&mut self) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
            // read begin
            self.reader.as_mut().unwrap().mark();
            let cmd = self.response(rdb::read_bytes);
            let read_len = self.reader.as_mut().unwrap().unmark()?;
            self.repl_offset += read_len;
            self.sender.as_ref().unwrap().send(Message::Some(self.repl_offset)).unwrap();
            return cmd;
            // read end, and get total bytes read
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
                match self.receive_cmd() {
                    Ok(Data::Bytes(_)) => return Err(Error::new(ErrorKind::InvalidData, "Expect BytesVec response, but got Bytes")),
                    Ok(Data::BytesVec(cmd)) => {
                        println!("cmd: {:?}", cmd);
                    }
                    Err(err) => return Err(err),
                    Ok(Empty) => {}
                }
            }
        }
    }
    
    impl Drop for Listener<'_> {
        fn drop(&mut self) {
            self.sender.as_ref().unwrap().send(Message::Terminate).unwrap();
            if let Some(reader) = self.reader.take() {
                if let Err(_) = reader.stream.lock().unwrap().shutdown(Shutdown::Both) {}
            };
            if let Some(thread) = self.t_heartbeat.thread.take() {
                if let Err(_) = thread.join() {}
            }
        }
    }
    
    pub(crate) fn new(addr: SocketAddr, password: &str) -> Listener {
        Listener {
            addr,
            password,
            config: config::default(),
            reader: Option::None,
            repl_id: String::from("?"),
            repl_offset: -1,
            rdb_listeners: Vec::new(),
            cmd_listeners: Vec::new(),
            t_heartbeat: HeartbeatWorker { thread: None },
            sender: None,
        }
    }
    
    struct HeartbeatWorker {
        thread: Option<thread::JoinHandle<()>>
    }
    
    enum Message {
        Terminate,
        Some(i64),
    }
    
    pub(crate) enum SyncMode {
        PSync,
        Sync,
        SyncLater,
    }
}

pub mod cluster {}

pub mod sentinel {}