pub mod standalone {
    use std::borrow::Borrow;
    use std::io::Result;
    use std::net::TcpStream;
    use std::result::Result::Ok;
    use std::sync::mpsc;
    use std::thread;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    
    use crate::{cmd, CommandHandler, io, NoOpCommandHandler, NoOpRdbHandler, rdb, RdbHandler, RedisListener, to_string};
    use crate::config::Config;
    use crate::io::{Conn, send};
    use crate::rdb::Data;
    use crate::rdb::Data::Bytes;
    
    // 用于监听单个Redis实例的事件
    pub struct Listener {
        config: Config,
        conn: Option<Conn>,
        rdb_listener: Box<dyn RdbHandler>,
        cmd_listener: Box<dyn CommandHandler>,
        t_heartbeat: HeartbeatWorker,
        sender: Option<mpsc::Sender<Message>>,
    }
    
    impl Listener {
        fn connect(&mut self) -> Result<()> {
            let stream = TcpStream::connect(self.config.addr)?;
            println!("connected to server!");
            let mut stream_boxed = stream.try_clone();
            let stream = stream;
            let (sender, receiver) = mpsc::channel();
            
            let t = thread::spawn(move || {
                let mut offset = 0;
                let output = stream_boxed.as_mut().unwrap();
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
            self.conn = Option::Some(io::new(stream));
            Ok(())
        }
        
        fn auth(&mut self) -> Result<()> {
            if !self.config.password.is_empty() {
                let conn = self.conn.as_mut().unwrap();
                conn.send(b"AUTH", &[self.config.password.as_bytes()])?;
                conn.reply(io::read_bytes, self.rdb_listener.as_mut(), self.cmd_listener.as_mut())?;
            }
            Ok(())
        }
        
        fn send_port(&mut self) -> Result<()> {
            let conn = self.conn.as_mut().unwrap();
            let stream: &TcpStream = match conn.input.as_any().borrow().downcast_ref::<TcpStream>() {
                Some(stream) => stream,
                None => panic!("not tcp stream")
            };
            let port = stream.local_addr()?.port().to_string();
            let port = port.as_bytes();
            conn.send(b"REPLCONF", &[b"listening-port", port])?;
            conn.reply(io::read_bytes, self.rdb_listener.as_mut(), self.cmd_listener.as_mut())?;
            Ok(())
        }
        
        pub fn set_rdb_listener(&mut self, listener: Box<dyn RdbHandler>) {
            self.rdb_listener = listener
        }
        
        pub fn set_command_listener(&mut self, listener: Box<dyn CommandHandler>) {
            self.cmd_listener = listener
        }
        
        fn start_sync(&mut self) -> Result<bool> {
            let offset = self.config.repl_offset.to_string();
            let repl_offset = offset.as_bytes();
            let repl_id = self.config.repl_id.as_bytes();
            
            let conn = self.conn.as_mut().unwrap();
            conn.send(b"PSYNC", &[repl_id, repl_offset])?;
            
            if let Bytes(resp) = conn.reply(io::read_bytes, self.rdb_listener.as_mut(), self.cmd_listener.as_mut())? {
                let resp = to_string(resp);
                if resp.starts_with("FULLRESYNC") {
                    if self.config.is_discard_rdb {
                        conn.reply(io::skip, self.rdb_listener.as_mut(), self.cmd_listener.as_mut())?;
                    } else {
                        conn.reply(rdb::parse, self.rdb_listener.as_mut(), self.cmd_listener.as_mut())?;
                    }
                    let mut iter = resp.split_whitespace();
                    if let Some(repl_id) = iter.nth(1) {
                        self.config.repl_id = repl_id.to_owned();
                    } else {
                        panic!("Expect replication id, bot got None");
                    }
                    if let Some(repl_offset) = iter.next() {
                        self.config.repl_offset = repl_offset.parse::<i64>().unwrap();
                    } else {
                        panic!("Expect replication offset, bot got None");
                    }
                    return Ok(true);
                } else if resp.starts_with("CONTINUE") {
                    // PSYNC 继续之前的offset
                    let mut iter = resp.split_whitespace();
                    if let Some(repl_id) = iter.nth(1) {
                        if !repl_id.eq(&self.config.repl_id) {
                            self.config.repl_id = repl_id.to_owned();
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
                    conn.send(b"SYNC", &Vec::new())?;
                    if self.config.is_discard_rdb {
                        conn.reply(io::skip, self.rdb_listener.as_mut(), self.cmd_listener.as_mut())?;
                    } else {
                        conn.reply(rdb::parse, self.rdb_listener.as_mut(), self.cmd_listener.as_mut())?;
                    }
                    return Ok(true);
                }
            } else {
                panic!("Expect Redis string response");
            }
        }
        
        fn receive_cmd(&mut self) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
            let conn = self.conn.as_mut().unwrap();
            conn.mark();
            let cmd = conn.reply(io::read_bytes, self.rdb_listener.as_mut(), self.cmd_listener.as_mut());
            let read_len = conn.unmark()?;
            self.config.repl_offset += read_len;
            self.sender.as_ref().unwrap().send(Message::Some(self.config.repl_offset)).unwrap();
            return cmd;
        }
    }
    
    impl RedisListener for Listener {
        fn open(&mut self) -> Result<()> {
            self.connect()?;
            self.auth()?;
            self.send_port()?;
            while !self.start_sync()? {
                sleep(Duration::from_secs(5));
            }
            if !self.config.is_aof {
                return Ok(());
            }
            loop {
                match self.receive_cmd() {
                    Ok(Data::Bytes(_)) => panic!("Expect BytesVec response, but got Bytes"),
                    Ok(Data::BytesVec(data)) => cmd::parse(data, self.cmd_listener.as_mut()),
                    Err(err) => return Err(err),
                    Ok(Data::Empty) => {}
                }
            }
        }
    }
    
    impl Drop for Listener {
        fn drop(&mut self) {
            if let Some(sender) = self.sender.as_ref() {
                if let Err(err) = sender.send(Message::Terminate) {
                    eprintln!("{}", err)
                }
            }
            if let Some(thread) = self.t_heartbeat.thread.take() {
                if let Err(_) = thread.join() {}
            }
        }
    }
    
    pub fn new(conf: Config) -> Listener {
        Listener {
            config: conf,
            conn: Option::None,
            rdb_listener: Box::new(NoOpRdbHandler {}),
            cmd_listener: Box::new(NoOpCommandHandler {}),
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