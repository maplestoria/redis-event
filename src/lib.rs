use std::io::{BufWriter, Error, Write};
use std::net::{Shutdown, TcpStream};
use std::result::Result;

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
    host: &'static str,
    port: i32,
    password: &'static str,
    config: Config,
    socket: Option<BufWriter<TcpStream>>,
}

impl StandaloneEventListener {
    fn connect(&mut self) -> Result<(), Error> {
        let addr = format!("{}:{}", self.host, self.port);
        println!("connecting to {}", addr);
        let stream = TcpStream::connect(addr)?;
        println!("connected to server!");
        self.socket = Option::Some(BufWriter::new(stream));

        if !self.password.is_empty() {
            self.send("AUTH".as_bytes(), &[self.password.as_bytes()])?;
        }
        Result::Ok(())
    }

    fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<(), Error> {
        let socket = self.socket.as_mut().unwrap();
        socket.write(&[STAR])?;
        let args_len = args.len() + 1;
        socket.write(args_len.to_string().as_bytes())?;
        socket.write(&[CR, LF, DOLLAR])?;
        socket.write(command.len().to_string().as_bytes())?;
        socket.write(&[CR, LF])?;
        socket.write(command)?;
        socket.write(&[CR, LF])?;
        for arg in args {
            socket.write(&[DOLLAR])?;
            socket.write(arg.len().to_string().as_bytes())?;
            socket.write(&[CR, LF])?;
            socket.write(arg)?;
            socket.write(&[CR, LF])?;
        }
        socket.flush()
    }
}

impl RedisEventListener for StandaloneEventListener {
    fn open(&mut self) -> Result<(), Error> {
        match self.connect() {
            Ok(_) => Result::Ok(()),
            Err(error) => Result::Err(error)
        }
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
    fn open() {
        let mut redis_listener = new("localhost", 6379, "123");
        if let Err(error) = redis_listener.open() {
            panic!("couldn't connect to server: {}", error)
        }
        redis_listener.close();
    }
}
