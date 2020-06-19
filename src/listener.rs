/*!
[`RedisListener`]接口的具体实现

[`RedisListener`]: trait.RedisListener.html
*/

/// 此模块针对单节点Redis，实现了`RedisListener`接口
///
/// # 示例
///
/// ```no_run
/// use std::net::{IpAddr, SocketAddr};
/// use std::sync::atomic::AtomicBool;
/// use std::sync::Arc;
/// use std::str::FromStr;
/// use std::rc::Rc;
/// use std::cell::RefCell;
/// use redis_event::listener;
/// use redis_event::config::Config;
/// use redis_event::{NoOpEventHandler, RedisListener};
///
///
/// let ip = IpAddr::from_str("127.0.0.1").unwrap();
/// let port = 6379;
///
/// let conf = Config {
///     is_discard_rdb: false,            // 不跳过RDB
///     is_aof: false,                    // 不处理AOF
///     addr: SocketAddr::new(ip, port),
///     password: String::new(),          // 密码为空
///     repl_id: String::from("?"),       // replication id，若无此id，设置为?即可
///     repl_offset: -1,                  // replication offset，若无此offset，设置为-1即可
///     read_timeout: None,               // None，即读取永不超时
///     write_timeout: None,              // None，即写入永不超时
/// };
/// let running = Arc::new(AtomicBool::new(true));
/// let mut redis_listener = listener::new(conf, running);
/// // 设置事件处理器
/// redis_listener.set_event_handler(Rc::new(RefCell::new(NoOpEventHandler{})));
/// // 启动程序
/// redis_listener.start()?;
/// ```
use std::borrow::Borrow;
use std::cell::{RefCell, RefMut};
use std::io::{ErrorKind, Result};
use std::net::TcpStream;
use std::rc::Rc;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

use log::{error, info, warn};

use crate::config::Config;
use crate::io::{send, Conn};
use crate::rdb::{Data, DefaultRDBParser};
use crate::resp::{Resp, RespDecode, Type};
use crate::{cmd, io, rdb, EventHandler, ModuleParser, NoOpEventHandler, RDBParser, RedisListener};
use std::ops::DerefMut;

/// 用于监听单个Redis实例的事件
pub struct Listener {
    pub config: Config,
    conn: Option<Conn>,
    event_handler: Rc<RefCell<dyn EventHandler>>,
    module_parser: Option<Rc<RefCell<dyn ModuleParser>>>,
    t_heartbeat: HeartbeatWorker,
    sender: Option<mpsc::Sender<Message>>,
    running: Arc<AtomicBool>,
}

impl Listener {
    /// 连接Redis，创建TCP连接
    fn connect(&mut self) -> Result<()> {
        let stream = TcpStream::connect(self.config.addr)?;
        stream
            .set_read_timeout(self.config.read_timeout)
            .expect("read timeout set failed");
        stream
            .set_write_timeout(self.config.write_timeout)
            .expect("write timeout set failed");
        info!("connected to server {}", self.config.addr.to_string());
        let mut conn = io::new(stream, self.running.clone());
        if let Some(parser) = &self.module_parser {
            conn.module_parser = Option::Some(parser.clone());
        }
        self.conn = Option::Some(conn);
        Ok(())
    }

    /// 如果有设置密码，将尝试使用此密码进行认证
    fn auth(&mut self) -> Result<()> {
        if !self.config.password.is_empty() {
            let conn = self.conn.as_mut().unwrap();
            conn.send(b"AUTH", &[self.config.password.as_bytes()])?;
            conn.input.decode_resp()?;
        }
        Ok(())
    }

    /// 发送本地所使用的socket端口到redis，此端口展现在`info replication`中
    fn send_port(&mut self) -> Result<()> {
        let conn = self.conn.as_mut().unwrap();
        let stream: &TcpStream = match conn.input.as_any().borrow().downcast_ref::<TcpStream>() {
            Some(stream) => stream,
            None => panic!("not tcp stream"),
        };
        let port = stream.local_addr()?.port().to_string();
        let port = port.as_bytes();
        conn.send(b"REPLCONF", &[b"listening-port", port])?;
        conn.input.decode_resp()?;
        Ok(())
    }

    /// 设置事件处理器
    pub fn set_event_handler(&mut self, handler: Rc<RefCell<dyn EventHandler>>) {
        self.event_handler = handler
    }

    pub fn set_module_parser(&mut self, parser: Rc<RefCell<dyn ModuleParser>>) {
        self.module_parser = Option::Some(parser)
    }

    /// 开启replication
    /// 默认使用PSYNC命令，若不支持PSYNC则尝试使用SYNC命令
    fn start_sync(&mut self) -> Result<bool> {
        let (next_step, length) = self.psync()?;
        match next_step {
            NextStep::FullSync => {
                println!("Full Resync, length: {}", length);
                let mut parser = DefaultRDBParser {
                    running: Arc::clone(&self.running),
                };
                let conn = self.conn.as_mut().unwrap();
                let mut event_handler: RefMut<dyn EventHandler> = self.event_handler.borrow_mut();
                parser.parse(&mut conn.input, length, event_handler.deref_mut())?;
                Ok(true)
            }
            NextStep::PartialResync => {
                info!("PSYNC进度恢复");
                Ok(true)
            }
            NextStep::ChangeMode => Ok(false),
            NextStep::Wait => Ok(false),
        }
    }

    fn psync(&mut self) -> Result<(NextStep, i64)> {
        let offset = self.config.repl_offset.to_string();
        let repl_offset = offset.as_bytes();
        let repl_id = self.config.repl_id.as_bytes();

        let conn = self.conn.as_mut().unwrap();
        conn.send(b"PSYNC", &[repl_id, repl_offset])?;

        if let Resp::String(resp) = conn.input.decode_resp()? {
            info!("{}", resp);
            let next = if resp.starts_with("FULLRESYNC") {
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
                NextStep::FullSync
            } else if resp.starts_with("CONTINUE") {
                let mut iter = resp.split_whitespace();
                if let Some(repl_id) = iter.nth(1) {
                    if !repl_id.eq(&self.config.repl_id) {
                        self.config.repl_id = repl_id.to_owned();
                    }
                }
                NextStep::PartialResync
            } else if resp.starts_with("NOMASTERLINK") {
                return Ok((NextStep::Wait, -1));
            } else if resp.starts_with("LOADING") {
                return Ok((NextStep::Wait, -1));
            } else {
                return Ok((NextStep::Wait, -1));
            };
            if let Type::BulkBytes = conn.input.decode_type()? {
                if let Resp::Int(length) = conn.input.decode_int()? {
                    return Ok((next, length));
                }
            }
            panic!("Wrong data type");
        } else {
            panic!("Expect Redis string response");
        }
    }

    /// 接收AOF，并处理replication offset发送到心跳线程
    fn receive_cmd(&mut self) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
        unimplemented!()
    }

    /// 开启心跳
    fn start_heartbeat(&mut self) {
        if !self.is_running() {
            return;
        }
        let conn = self.conn.as_ref().unwrap();
        let stream: &TcpStream = match conn.input.as_any().borrow().downcast_ref::<TcpStream>() {
            Some(stream) => stream,
            None => panic!("not tcp stream"),
        };
        let mut stream_clone = stream.try_clone().unwrap();

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
                    if let Err(error) =
                        send(&mut stream_clone, b"REPLCONF", &[b"ACK", offset_bytes])
                    {
                        error!("heartbeat error: {}", error);
                        break;
                    }
                    timer = Instant::now();
                }
            }
            info!("heartbeat thread terminated");
        });
        self.t_heartbeat = HeartbeatWorker { thread: Some(t) };
        self.sender = Some(sender);
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
        while !self.start_sync()? && self.is_running() {
            sleep(Duration::from_secs(5));
        }
        if !self.config.is_aof {
            return Ok(());
        }
        self.start_heartbeat();
        while self.is_running() {
            match self.receive_cmd() {
                Ok(Data::Bytes(_)) => panic!("Expect BytesVec response, but got Bytes"),
                Ok(Data::BytesVec(data)) => {
                    let mut handler: RefMut<dyn EventHandler> = self.event_handler.borrow_mut();
                    cmd::parse(data, &mut handler);
                }
                Err(ref err)
                    if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::TimedOut =>
                {
                    // 不管，连接是好的
                }
                Err(err) => return Err(err),
                Ok(Data::Empty) => {}
            }
        }
        Ok(())
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.as_ref() {
            if let Err(err) = sender.send(Message::Terminate) {
                error!("Closing heartbeat thread error: {}", err)
            }
        }
        if let Some(thread) = self.t_heartbeat.thread.take() {
            if let Err(_) = thread.join() {}
        }
    }
}

/// Listener实例的创建方法。 `running`变量用于在外部中断此crate内部的逻辑，设置为true再传入
pub fn new(conf: Config, running: Arc<AtomicBool>) -> Listener {
    if conf.is_aof && !running.load(Ordering::Relaxed) {
        running.store(true, Ordering::Relaxed);
    }

    Listener {
        config: conf,
        conn: Option::None,
        event_handler: Rc::new(RefCell::new(NoOpEventHandler {})),
        module_parser: None,
        t_heartbeat: HeartbeatWorker { thread: None },
        sender: None,
        running,
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
