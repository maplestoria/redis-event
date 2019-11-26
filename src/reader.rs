use std::io;
use std::io::{Error, ErrorKind, Read};
use std::net::TcpStream;

use byteorder::{ByteOrder, ReadBytesExt};

pub(crate) struct Reader {
    pub(crate) stream: Box<TcpStream>,
    len: i64,
    marked: bool,
}

impl Reader {
    pub(crate) fn new(stream: Box<TcpStream>) -> Reader {
        Reader { stream, len: 0, marked: false }
    }
    
    pub(crate) fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0; 1];
        self.stream.read_exact(&mut buf)?;
        if self.marked {
            self.len += 1;
        };
        Ok(buf[0])
    }
    
    pub(crate) fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.stream.read_exact(buf)?;
        if self.marked {
            self.len += buf.len() as i64;
        };
        Ok(())
    }
    
    pub(crate) fn read_i64<T: ByteOrder>(&mut self) -> io::Result<i64> {
        let int = self.stream.read_i64::<T>()?;
        if self.marked {
            self.len += 8;
        };
        Ok(int)
    }
    
    pub(crate) fn read_i8(&mut self) -> io::Result<i8> {
        let int = self.stream.read_i8()?;
        if self.marked {
            self.len += 1;
        };
        Ok(int)
    }
    
    pub(crate) fn mark(&mut self) {
        self.marked = true;
    }
    
    pub(crate) fn unmark(&mut self) -> io::Result<i64> {
        if self.marked {
            let len = self.len;
            self.len = 0;
            self.marked = false;
            return Ok(len);
        }
        return Err(Error::new(ErrorKind::Other, "Reader not marked"));
    }
}