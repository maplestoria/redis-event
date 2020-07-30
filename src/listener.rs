/*!
[`RedisListener`]接口的具体实现

[`RedisListener`]: trait.RedisListener.html
*/
use std::cell::RefCell;
use std::io::{Read, Result};
use std::net::TcpStream;
use std::ops::DerefMut;
use std::rc::Rc;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

use log::{error, info};
use native_tls::{Identity, TlsConnector, TlsStream};

use crate::config::Config;
use crate::io::send;
use crate::rdb::DefaultRDBParser;
use crate::resp::{Resp, RespDecode, Type};
use crate::{cmd, io, EventHandler, ModuleParser, NoOpEventHandler, RDBParser, RedisListener};
use std::fs::File;

/// 用于监听单个Redis实例的事件
pub struct Listener {
    pub config: Config,
    conn: Option<Stream>,
    rdb_parser: Rc<RefCell<dyn RDBParser>>,
    event_handler: Rc<RefCell<dyn EventHandler>>,
    heartbeat_thread: HeartbeatWorker,
    sender: Option<mpsc::Sender<Message>>,
    running: Arc<AtomicBool>,
    local_port: Option<u16>,
}

impl Listener {
    /// 连接Redis，创建TCP连接
    fn connect(&mut self) -> Result<()> {
        let addr = format!("{}:{}", &self.config.host, self.config.port);
        let stream = TcpStream::connect(&addr)?;
        stream
            .set_read_timeout(self.config.read_timeout)
            .expect("read timeout set failed");
        stream
            .set_write_timeout(self.config.write_timeout)
            .expect("write timeout set failed");

        let local_port = stream.local_addr().unwrap().port();
        self.local_port = Some(local_port);

        if self.config.is_tls_enabled {
            let mut builder = TlsConnector::builder();
            builder.danger_accept_invalid_hostnames(self.config.is_tls_insecure);
            builder.danger_accept_invalid_certs(self.config.is_tls_insecure);

            if let Some(id) = &self.config.identity {
                let mut file = File::open(id)?;
                let mut buff = Vec::new();
                file.read_to_end(&mut buff)?;
                let identity_passwd = match &self.config.identity_passwd {
                    None => "",
                    Some(passwd) => passwd.as_str(),
                };
                let identity = Identity::from_pkcs12(&buff, identity_passwd).expect("解析key失败");
                builder.identity(identity);
            }

            let connector = builder.build().unwrap();
            let tls_stream = connector
                .connect(&self.config.host, stream)
                .expect("TLS connect failed");
            self.conn = Option::Some(Stream::Tls(tls_stream));
        } else {
            self.conn = Option::Some(Stream::Tcp(stream));
        }
        info!("connected to server {}", &addr);
        Ok(())
    }

    /// 如果有设置密码，将尝试使用此密码进行认证
    fn auth(&mut self) -> Result<()> {
        if !self.config.password.is_empty() {
            let mut args = Vec::with_capacity(2);
            if !self.config.username.is_empty() {
                args.push(self.config.username.as_bytes());
            }
            args.push(self.config.password.as_bytes());
            let conn = self.conn.as_mut().unwrap();
            let conn: &mut dyn Read = match conn {
                Stream::Tcp(tcp_stream) => {
                    send(tcp_stream, b"AUTH", &args)?;
                    tcp_stream
                }
                Stream::Tls(tls_stream) => {
                    send(tls_stream, b"AUTH", &args)?;
                    tls_stream
                }
            };
            conn.decode_resp()?;
        }
        Ok(())
    }

    /// 发送本地所使用的socket端口到redis，此端口展现在`info replication`中
    fn send_port(&mut self) -> Result<()> {
        let port = self.local_port.unwrap().to_string();
        let port = port.as_bytes();
        let conn = self.conn.as_mut().unwrap();
        let conn: &mut dyn Read = match conn {
            Stream::Tcp(tcp_stream) => {
                send(tcp_stream, b"REPLCONF", &[b"listening-port", port])?;
                tcp_stream
            }
            Stream::Tls(tls_stream) => {
                send(tls_stream, b"REPLCONF", &[b"listening-port", port])?;
                tls_stream
            }
        };
        conn.decode_resp()?;
        Ok(())
    }

    /// 开启replication
    /// 默认使用PSYNC命令，若不支持PSYNC则尝试使用SYNC命令
    fn start_sync(&mut self) -> Result<Mode> {
        let (next_step, mut length) = self.psync()?;
        match next_step {
            NextStep::FullSync | NextStep::ChangeMode => {
                let mode;
                if let NextStep::ChangeMode = next_step {
                    info!("源Redis不支持PSYNC命令, 使用SYNC命令再次进行尝试");
                    mode = Mode::Sync;
                    length = self.sync()?;
                } else {
                    mode = Mode::PSync;
                }
                info!("Full Resync, size: {}bytes", length);
                let conn = self.conn.as_mut().unwrap();

                let conn: &mut dyn Read = match conn {
                    Stream::Tcp(tcp_stream) => tcp_stream,
                    Stream::Tls(tls_stream) => tls_stream,
                };
                if self.config.is_discard_rdb {
                    info!("跳过RDB不进行处理");
                    io::skip(conn, length as isize)?;
                } else {
                    let mut event_handler = self.event_handler.borrow_mut();
                    let mut rdb_parser = self.rdb_parser.borrow_mut();
                    rdb_parser.parse(conn, length, event_handler.deref_mut())?;
                }
                Ok(mode)
            }
            NextStep::PartialResync => {
                info!("PSYNC进度恢复");
                Ok(Mode::PSync)
            }
            NextStep::Wait => Ok(Mode::Wait),
        }
    }

    fn psync(&mut self) -> Result<(NextStep, i64)> {
        let offset = self.config.repl_offset.to_string();
        let repl_offset = offset.as_bytes();
        let repl_id = self.config.repl_id.as_bytes();

        let conn = self.conn.as_mut().unwrap();
        let conn: &mut dyn Read = match conn {
            Stream::Tcp(tcp_stream) => {
                send(tcp_stream, b"PSYNC", &[repl_id, repl_offset])?;

                tcp_stream
            }
            Stream::Tls(tls_stream) => {
                send(tls_stream, b"PSYNC", &[repl_id, repl_offset])?;
                tls_stream
            }
        };

        match conn.decode_resp() {
            Ok(response) => {
                if let Resp::String(resp) = &response {
                    info!("{}", resp);
                    if resp.starts_with("FULLRESYNC") {
                        let mut iter = resp.split_whitespace();
                        if let Some(repl_id) = iter.nth(1) {
                            self.config.repl_id = repl_id.to_owned();
                        } else {
                            panic!("Expect replication id, but got None");
                        }
                        if let Some(repl_offset) = iter.next() {
                            self.config.repl_offset = repl_offset.parse::<i64>().unwrap();
                        } else {
                            panic!("Expect replication offset, but got None");
                        }
                        info!("等待Redis dump完成...");
                        if let Type::BulkString = conn.decode_type()? {
                            if let Resp::Int(length) = conn.decode_int()? {
                                return Ok((NextStep::FullSync, length));
                            } else {
                                panic!("Expect int response")
                            }
                        } else {
                            panic!("Expect BulkString response");
                        }
                    } else if resp.starts_with("CONTINUE") {
                        let mut iter = resp.split_whitespace();
                        if let Some(repl_id) = iter.nth(1) {
                            if !repl_id.eq(&self.config.repl_id) {
                                self.config.repl_id = repl_id.to_owned();
                            }
                        }
                        return Ok((NextStep::PartialResync, -1));
                    } else if resp.starts_with("NOMASTERLINK") {
                        return Ok((NextStep::Wait, -1));
                    } else if resp.starts_with("LOADING") {
                        return Ok((NextStep::Wait, -1));
                    }
                }
                panic!("Unexpected Response: {:?}", response);
            }
            Err(error) => {
                if error.to_string().eq("ERR unknown command 'PSYNC'") {
                    return Ok((NextStep::ChangeMode, -1));
                } else {
                    return Err(error);
                }
            }
        }
    }

    fn sync(&mut self) -> Result<i64> {
        let conn = self.conn.as_mut().unwrap();
        let conn: &mut dyn Read = match conn {
            Stream::Tcp(tcp_stream) => {
                send(tcp_stream, b"SYNC", &vec![])?;
                tcp_stream
            }
            Stream::Tls(tls_stream) => {
                send(tls_stream, b"SYNC", &vec![])?;
                tls_stream
            }
        };
        if let Type::BulkString = conn.decode_type()? {
            if let Resp::Int(length) = conn.decode_int()? {
                return Ok(length);
            } else {
                panic!("Expect int response")
            }
        } else {
            panic!("Expect BulkString response");
        }
    }

    /// 开启心跳
    fn start_heartbeat(&mut self, mode: &Mode) {
        if !self.is_running() {
            return;
        }
        if let Mode::Sync = mode {
            return;
        }
        if self.config.is_tls_enabled {
            return;
        }
        let conn = self.conn.as_ref().unwrap();
        let conn = match conn {
            Stream::Tcp(tcp_stream) => tcp_stream,
            Stream::Tls(_) => panic!("Expect TcpStream"),
        }; // TODO tls模式下心跳信息需要额外处理
        let mut conn_clone = conn.try_clone().unwrap();

        let (sender, receiver) = mpsc::channel();

        let t = thread::spawn(move || {
            let mut offset = 0;
            let mut timer = Instant::now();
            let half_sec = Duration::from_millis(500);
            info!("heartbeat thread started");
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
                    if let Err(error) = send(&mut conn_clone, b"REPLCONF", &[b"ACK", offset_bytes])
                    {
                        error!("heartbeat error: {}", error);
                        break;
                    }
                    timer = Instant::now();
                }
            }
            info!("heartbeat thread terminated");
        });
        self.heartbeat_thread = HeartbeatWorker { thread: Some(t) };
        self.sender = Some(sender);
    }

    fn receive_aof(&mut self, mode: &Mode) -> Result<()> {
        let mut handler = self.event_handler.as_ref().borrow_mut();

        let __conn = self.conn.as_mut().unwrap();
        match __conn {
            Stream::Tcp(tcp_stream) => {
                let mut reader = io::CountReader::new(tcp_stream);

                while self.running.load(Ordering::Relaxed) {
                    reader.mark();
                    if let Resp::Array(array) = reader.decode_resp()? {
                        let size = reader.reset()?;
                        let mut vec = Vec::with_capacity(array.len());
                        for x in array {
                            if let Resp::BulkBytes(bytes) = x {
                                vec.push(bytes);
                            } else {
                                panic!("Expected BulkString response");
                            }
                        }
                        self.config.repl_offset += size;
                        if let Mode::PSync = mode {
                            if let Err(error) = self
                                .sender
                                .as_ref()
                                .unwrap()
                                .send(Message::Some(self.config.repl_offset))
                            {
                                error!("repl offset send error: {}", error);
                            }
                        }
                        cmd::parse(vec, handler.deref_mut());
                    } else {
                        panic!("Expected array response");
                    }
                }
            }
            Stream::Tls(tls_stream) => {
                let mut timer = Instant::now();
                let half_sec = Duration::from_millis(500);

                while self.running.load(Ordering::Relaxed) {
                    {
                        let mut reader = io::CountReader::new(tls_stream);
                        reader.mark();
                        if let Resp::Array(array) = reader.decode_resp()? {
                            let size = reader.reset()?;
                            let mut vec = Vec::with_capacity(array.len());
                            for x in array {
                                if let Resp::BulkBytes(bytes) = x {
                                    vec.push(bytes);
                                } else {
                                    panic!("Expected BulkString response");
                                }
                            }
                            self.config.repl_offset += size;

                            cmd::parse(vec, handler.deref_mut());
                        } else {
                            panic!("Expected array response");
                        }
                    }

                    let elapsed = timer.elapsed();
                    if elapsed.ge(&half_sec) {
                        let offset_str = self.config.repl_offset.to_string();
                        let offset_bytes = offset_str.as_bytes();
                        if let Err(error) = send(tls_stream, b"REPLCONF", &[b"ACK", offset_bytes]) {
                            error!("heartbeat error: {}", error);
                            break;
                        }
                        timer = Instant::now();
                    }
                }
            }
        };
        Ok(())
    }

    /// 获取当前运行的状态，若为false，程序将有序退出
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

impl RedisListener for Listener {
    /// 程序运行的整体逻辑都在这个方法里面实现
    ///
    /// 具体的细节体现在各个方法内
    fn start(&mut self) -> Result<()> {
        self.connect()?;
        self.auth()?;
        self.send_port()?;
        let mut mode;
        loop {
            mode = self.start_sync()?;
            match mode {
                Mode::Wait => {
                    if self.is_running() {
                        sleep(Duration::from_secs(5));
                    } else {
                        return Ok(());
                    }
                }
                _ => break,
            }
        }
        if !self.config.is_aof {
            Ok(())
        } else {
            self.start_heartbeat(&mode);
            self.receive_aof(&mode)?;
            Ok(())
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.as_ref() {
            if let Err(err) = sender.send(Message::Terminate) {
                error!("Closing heartbeat thread error: {}", err)
            }
        }
        if let Some(thread) = self.heartbeat_thread.thread.take() {
            if let Err(_) = thread.join() {}
        }
    }
}

struct HeartbeatWorker {
    thread: Option<thread::JoinHandle<()>>,
}

enum Message {
    Terminate,
    Some(i64),
}

enum NextStep {
    FullSync,
    PartialResync,
    ChangeMode,
    Wait,
}

enum Mode {
    PSync,
    Sync,
    Wait,
}

pub struct Builder {
    pub config: Option<Config>,
    pub rdb_parser: Option<Rc<RefCell<dyn RDBParser>>>,
    pub event_handler: Option<Rc<RefCell<dyn EventHandler>>>,
    pub module_parser: Option<Rc<RefCell<dyn ModuleParser>>>,
    pub control_flag: Option<Arc<AtomicBool>>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            config: None,
            rdb_parser: None,
            event_handler: None,
            module_parser: None,
            control_flag: None,
        }
    }

    pub fn with_config(&mut self, config: Config) {
        self.config = Some(config);
    }

    pub fn with_rdb_parser(&mut self, parser: Rc<RefCell<dyn RDBParser>>) {
        self.rdb_parser = Some(parser);
    }

    pub fn with_event_handler(&mut self, handler: Rc<RefCell<dyn EventHandler>>) {
        self.event_handler = Some(handler);
    }

    pub fn with_module_parser(&mut self, parser: Rc<RefCell<dyn ModuleParser>>) {
        self.module_parser = Some(parser);
    }

    pub fn with_control_flag(&mut self, flag: Arc<AtomicBool>) {
        self.control_flag = Some(flag);
    }

    pub fn build(&mut self) -> Listener {
        let config = match &self.config {
            Some(c) => c,
            None => panic!("Parameter Config is required"),
        };

        let module_parser = match &self.module_parser {
            None => None,
            Some(parser) => Some(parser.clone()),
        };

        let running = match &self.control_flag {
            None => panic!("Parameter Control_flag is required"),
            Some(flag) => flag.clone(),
        };

        let rdb_parser = match &self.rdb_parser {
            None => Rc::new(RefCell::new(DefaultRDBParser {
                running: Arc::clone(&running),
                module_parser,
            })),
            Some(parser) => parser.clone(),
        };

        let event_handler = match &self.event_handler {
            None => Rc::new(RefCell::new(NoOpEventHandler {})),
            Some(handler) => handler.clone(),
        };

        Listener {
            config: config.clone(),
            conn: None,
            rdb_parser,
            event_handler,
            heartbeat_thread: HeartbeatWorker { thread: None },
            sender: None,
            running,
            local_port: None,
        }
    }
}

enum Stream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}
