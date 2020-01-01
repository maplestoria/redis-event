pub mod standalone {
    use std::io::{BufWriter, Error, ErrorKind, Result, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::result::Result::Ok;
    use std::sync::mpsc;
    use std::thread;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    
    use crate::{cmd, CommandHandler, config, rdb, RdbHandler, RedisListener, to_string};
    use crate::config::Config;
    use crate::conn::Conn;
    use crate::rdb::{COLON, CR, Data, DOLLAR, LF, MINUS, PLUS, STAR};
    use crate::rdb::Data::{Bytes, BytesVec, Empty};
    
    // 用于监听单个Redis实例的事件
    pub struct Listener<'a> {
        addr: SocketAddr,
        password: &'a str,
        config: Config,
        conn: Option<Conn>,
        repl_id: String,
        repl_offset: i64,
        rdb_listeners: Vec<Box<dyn RdbHandler>>,
        cmd_listeners: Vec<Box<dyn CommandHandler>>,
        t_heartbeat: HeartbeatWorker,
        sender: Option<mpsc::Sender<Message>>,
    }
    
    impl Listener<'_> {
        fn connect(&mut self) -> Result<()> {
            let stream = TcpStream::connect(self.addr)?;
            println!("connected to server!");
            let stream_boxed = Box::new(stream.try_clone());
            let stream = Box::new(stream);
            let (sender, receiver) = mpsc::channel();
            
            let t = thread::spawn(move || {
                let mut offset = 0;
                let output = stream_boxed.as_ref().as_ref().unwrap();
                let mut timer = Instant::now();
                let half_sec = Duration::from_millis(500);
                loop {
                    match receiver.recv_timeout(half_sec) {
                        Ok(Message::Terminate) => break,
                        Ok(Message::Some(new_offset)) => {
                            offset = new_offset;
                        }
                        Err(_) => {}
                    };
                    let elapsed = timer.elapsed();
                    if elapsed.ge(&half_sec) {
                        let offset_str = offset.to_string();
                        let offset_bytes = offset_str.as_bytes();
                        if let Err(error) = send(output, b"REPLCONF", &[b"ACK", offset_bytes]) {
                            println!("heartbeat error: {}", error);
                            break;
                        }
                        timer = Instant::now();
                    }
                }
                println!("terminated");
            });
            
            self.t_heartbeat = HeartbeatWorker { thread: Some(t) };
            self.sender = Some(sender);
            self.conn = Option::Some(Conn::new(stream));
            Ok(())
        }
        
        fn auth(&mut self) -> Result<()> {
            if !self.password.is_empty() {
                let conn = self.conn.as_ref().unwrap();
                let conn = conn.stream.as_ref();
                send(conn, b"AUTH", &[self.password.as_bytes()])?;
                self.response(rdb::read_bytes)?;
            }
            Ok(())
        }
        
        fn send_port(&mut self) -> Result<()> {
            let conn = self.conn.as_ref().unwrap();
            let port = conn.stream.local_addr()?.port().to_string();
            let port = port.as_bytes();
            
            let conn = self.conn.as_ref().unwrap();
            let conn = conn.stream.as_ref();
            
            send(conn, b"REPLCONF", &[b"listening-port", port])?;
            self.response(rdb::read_bytes)?;
            Ok(())
        }
        
        fn response(&mut self,
                    func: fn(&mut Conn, isize,
                             &Vec<Box<dyn RdbHandler>>, &Vec<Box<dyn CommandHandler>>,
                    ) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>>,
        ) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
            loop {
                let conn = self.conn.as_mut().unwrap();
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
                            let stream = self.conn.as_mut().unwrap();
                            return func(stream, length, &self.rdb_listeners, &self.cmd_listeners);
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
                                    match self.response(rdb::read_bytes)? {
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
        
        pub fn add_rdb_listener(&mut self, listener: Box<dyn RdbHandler>) {
            self.rdb_listeners.push(listener)
        }
        
        pub fn add_command_listener(&mut self, listener: Box<dyn CommandHandler>) {
            self.cmd_listeners.push(listener)
        }
        
        fn start_sync(&mut self) -> Result<bool> {
            let offset = self.repl_offset.to_string();
            let repl_offset = offset.as_bytes();
            let repl_id = self.repl_id.as_bytes();
            
            let conn = self.conn.as_ref().unwrap();
            let conn = conn.stream.as_ref();
            send(conn, b"PSYNC", &[repl_id, repl_offset])?;
            
            if let Bytes(resp) = self.response(rdb::read_bytes)? {
                let resp = to_string(resp);
                if resp.starts_with("FULLRESYNC") {
                    self.response(rdb::parse)?;
                    let mut iter = resp.split_whitespace();
                    if let Some(repl_id) = iter.nth(1) {
                        self.repl_id = repl_id.to_owned();
                    } else {
                        panic!("Expect replication id, bot got None");
                    }
                    if let Some(repl_offset) = iter.next() {
                        self.repl_offset = repl_offset.parse::<i64>().unwrap();
                    } else {
                        panic!("Expect replication offset, bot got None");
                    }
                    return Ok(true);
                } else if resp.starts_with("CONTINUE") {
                    // PSYNC 继续之前的offset
                    let mut iter = resp.split_whitespace();
                    if let Some(repl_id) = iter.nth(1) {
                        if !repl_id.eq(&self.repl_id) {
                            self.repl_id = repl_id.to_owned();
                        }
                    }
                    return Ok(true);
                } else if resp.starts_with("NOMASTERLINK") {
                    // redis丢失了master
                    return Ok(false);
                } else if resp.starts_with("LOADING") {
                    // redis正在启动，加载rdb中
                    return Ok(false);
                } else {
                    // 不支持PSYNC命令，改用SYNC命令
                    let conn = self.conn.as_ref().unwrap();
                    let conn = conn.stream.as_ref();
                    send(conn, b"SYNC", &Vec::new())?;
                    self.response(rdb::parse)?;
                    return Ok(true);
                }
            } else {
                panic!("Expect Redis string response");
            }
        }
        
        fn receive_cmd(&mut self) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
            // read begin
            self.conn.as_mut().unwrap().mark();
            let cmd = self.response(rdb::read_bytes);
            let read_len = self.conn.as_mut().unwrap().unmark()?;
            self.repl_offset += read_len;
            self.sender.as_ref().unwrap().send(Message::Some(self.repl_offset)).unwrap();
            return cmd;
            // read end, and get total bytes read
        }
    }
    
    fn send<T: Write>(output: T, command: &[u8], args: &[&[u8]]) -> Result<()> {
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
    
    impl RedisListener for Listener<'_> {
        fn open(&mut self) -> Result<()> {
            self.connect()?;
            self.auth()?;
            self.send_port()?;
            while !self.start_sync()? {
                sleep(Duration::from_secs(5));
            }
            if !self.config.aof {
                return Ok(());
            }
            loop {
                match self.receive_cmd() {
                    Ok(Data::Bytes(_)) => panic!("Expect BytesVec response, but got Bytes"),
                    Ok(Data::BytesVec(data)) => {
                        cmd::parse(data, &self.cmd_listeners);
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
            
            if let Some(thread) = self.t_heartbeat.thread.take() {
                if let Err(_) = thread.join() {}
            }
        }
    }
    
    pub fn new(addr: SocketAddr, password: &str) -> Listener {
        Listener {
            addr,
            password,
            config: config::default(),
            conn: Option::None,
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
}

pub mod cluster {}

pub mod sentinel {}