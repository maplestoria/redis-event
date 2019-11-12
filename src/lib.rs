use std::alloc::handle_alloc_error;
use std::io;
use std::io::{BufWriter, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};

use crate::config::Config;
use crate::constants::*;

mod constants;
mod config;

pub trait RedisEventListener {
    fn open(&mut self);

    fn close(&self);
}

// 用于监听Redis单点的事件
pub struct StandaloneEventListener {
    host: &'static str,
    port: i32,
    password: &'static str,
    config: Config,
    socket: Option<BufWriter<TcpStream>>,
}

impl StandaloneEventListener {
    fn connect(&mut self) {
        let addr = format!("{}:{}", self.host, self.port);
        println!("connecting to {}", addr);
        let stream = TcpStream::connect(addr).expect("Couldn't connect to server...");
        println!("connected to server!");
        self.socket = Option::Some(BufWriter::new(stream));

        if !self.password.is_empty() {
            self.send("AUTH".as_bytes(), &[self.password.as_bytes()]);
        }
    }

    fn send(&mut self, command: &[u8], args: &[&[u8]]) {
        let socket = self.socket.as_mut().unwrap();
        socket.write(&[STAR]).unwrap();
        let args_len = args.len() + 1;
        socket.write(args_len.to_string().as_bytes()).unwrap();
        socket.write(&[CR, LF, DOLLAR]).unwrap();
        socket.write(command.len().to_string().as_bytes()).unwrap();
        socket.write(&[CR, LF]).unwrap();
        socket.write(command).unwrap();
        socket.write(&[CR, LF]).unwrap();
        for arg in args {
            socket.write(&[DOLLAR]).unwrap();
            socket.write(arg.len().to_string().as_bytes()).unwrap();
            socket.write(&[CR, LF]).unwrap();
            socket.write(arg);
            socket.write(&[CR, LF]).unwrap();
        }
        socket.flush();
    }
}

impl RedisEventListener for StandaloneEventListener {
    fn open(&mut self) {
        self.connect();
    }

    fn close(&self) {
        let option = self.socket.as_ref();
        if self.socket.is_some() {
            println!("close connection with server...");
            option.unwrap().get_ref().shutdown(Shutdown::Both).unwrap();
        }
    }
}

pub fn new(host: &'static str, port: i32, password: &'static str) -> StandaloneEventListener {
    StandaloneEventListener {
        host,
        port,
        password,
        config: config::default(),
        socket: Option::None,
    }
}

// 测试用例
#[cfg(test)]
mod tests {
    use crate::{new, RedisEventListener};

    #[test]
    fn test() {
        let mut redis_listener = new("localhost", 6379, "123");
        redis_listener.open();
        redis_listener.close();
    }
}
