/*!
 处理redis的响应数据
*/

use crate::resp::*;
use std::io::{Error, ErrorKind, Read, Result, Write};

pub(crate) struct CountReader<'a> {
    input: &'a mut dyn Read,
    len: i64,
    marked: bool,
}

impl Read for CountReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let len = self.input.read(buf)?;
        if self.marked {
            self.len += len as i64;
        };
        Ok(len)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.input.read_exact(buf)?;
        if self.marked {
            self.len += buf.len() as i64;
        };
        Ok(())
    }
}

impl CountReader<'_> {
    pub(crate) fn new(input: &mut dyn Read) -> CountReader {
        CountReader {
            input,
            len: 0,
            marked: false,
        }
    }

    pub(crate) fn mark(&mut self) {
        self.marked = true;
    }

    pub(crate) fn reset(&mut self) -> Result<i64> {
        if self.marked {
            let len = self.len;
            self.len = 0;
            self.marked = false;
            return Ok(len);
        }
        return Err(Error::new(ErrorKind::Other, "not marked"));
    }
}

pub(crate) fn send<T: Write>(output: &mut T, command: &[u8], args: &[&[u8]]) -> Result<()> {
    let mut buf = vec![];
    buf.write(&[STAR])?;
    let args_len = args.len() + 1;
    buf.write(&args_len.to_string().into_bytes())?;
    buf.write(&[CR, LF, DOLLAR])?;
    buf.write(&command.len().to_string().into_bytes())?;
    buf.write(&[CR, LF])?;
    buf.write(command)?;
    buf.write(&[CR, LF])?;
    for arg in args {
        buf.write(&[DOLLAR])?;
        buf.write(&arg.len().to_string().into_bytes())?;
        buf.write(&[CR, LF])?;
        buf.write(arg)?;
        buf.write(&[CR, LF])?;
    }
    output.write_all(&mut buf)?;
    output.flush()
}

// 跳过rdb的字节
pub(crate) fn skip(input: &mut dyn Read, length: isize) -> Result<()> {
    std::io::copy(&mut input.take(length as u64), &mut std::io::sink())?;
    Ok(())
}
