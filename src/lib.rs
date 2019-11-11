use std::alloc::handle_alloc_error;
use std::io;
use std::io::{BufWriter, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};

use byteorder::{LittleEndian, WriteBytesExt};

use crate::config::Config;

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
        let mut buf: std::vec::Vec<u8> = vec![];

        let writer = self.socket.as_mut().unwrap();

        buf.write_u8(b'*');
//        socket.write(&[b'*']).unwrap();
        let args_len = args.len() + 1;
        buf.write_uint::<LittleEndian>(args_len as u64, 8);
//        socket.write(&args_len).unwrap();
//        socket.write(&[b'\r', b'\n', b'$']).unwrap();
        buf.write(&[b'\r', b'\n']);
        buf.write_u8(b'$');

//        let cmd_len = command.len().to_be_bytes();
        buf.write_uint::<LittleEndian>(command.len() as u64, 8);
//        socket.write(&cmd_len).unwrap();
//        socket.write(&[b'\r', b'\n']).unwrap();
        buf.write(command).unwrap();
        buf.write(&[b'\r', b'\n']).unwrap();
        for arg in args {
            buf.write_u8(b'$');
//            let arg_len = arg.len().to_be_bytes();
//            socket.write(&arg_len).unwrap();
            buf.write_uint::<LittleEndian>(arg.len() as u64, 8);
            buf.write(&[b'\r', b'\n']).unwrap();
            buf.write(arg).unwrap();
            buf.write(&[b'\r', b'\n']).unwrap();
        }
        let x = buf.as_slice();
        println!("cmd size: {}", x.len());
        for x in x {
            print!("{},", x);
        }
        println!();
        writer.write(x);
        writer.flush();
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
