use std::io;
use std::io::Read;
use std::net::TcpStream;

use byteorder::{ByteOrder, ReadBytesExt};

pub(crate) struct Reader {
    pub(crate) stream: TcpStream,
    len: i64,
}

impl Reader {
    pub(crate) fn new(stream: TcpStream) -> Reader {
        Reader { stream, len: 0 }
    }
    
    pub(crate) fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0; 1];
        self.stream.read_exact(&mut buf)?;
        Ok(buf[0])
    }
    
    pub(crate) fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.stream.read_exact(buf)
    }
    
    pub(crate) fn read_i64<T: ByteOrder>(&mut self) -> io::Result<i64> {
        self.stream.read_i64::<T>()
    }
    
    pub(crate) fn read_i8(&mut self) -> io::Result<i8> {
        self.stream.read_i8()
    }
    
    pub(crate) fn mark(&mut self) {}
    
    pub(crate) fn unmark(&mut self) -> i64 {
        self.len
    }
}