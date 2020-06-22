/*!
 处理redis的响应数据
*/

use crate::resp::*;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
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

    pub(crate) fn read_u8(&mut self) -> Result<u8> {
        let b = self.input.read_u8()?;
        if self.marked {
            self.len += 1;
        };
        Ok(b)
    }

    pub(crate) fn read_u64<O: ByteOrder>(&mut self) -> Result<u64> {
        let int = self.input.read_u64::<O>()?;
        if self.marked {
            self.len += 8;
        };
        Ok(int)
    }

    pub(crate) fn read_i8(&mut self) -> Result<i8> {
        let b = self.input.read_i8()?;
        if self.marked {
            self.len += 1;
        };
        Ok(b)
    }
}

fn read_long(input: &mut dyn Read, length: i32, little_endian: bool) -> Result<i64> {
    let mut r: i64 = 0;
    for i in 0..length {
        let v: i64 = input.read_u8()? as i64;
        if little_endian {
            r |= v << (i << 3) as i64;
        } else {
            r = (r << 8) | v;
        }
    }
    Ok(r)
}

fn read_list_pack_entry(input: &mut dyn Read) -> Result<Vec<u8>> {
    let special = input.read_u8()? as i32;
    let skip: i32;
    let mut bytes;
    if (special & 0x80) == 0 {
        skip = 1;
        let value = special & 0x7F;
        let value = value.to_string();
        bytes = value.into_bytes();
    } else if (special & 0xC0) == 0x80 {
        let len = special & 0x3F;
        skip = 1 + len as i32;
        bytes = vec![0; len as usize];
        input.read_exact(&mut bytes)?;
    } else if (special & 0xE0) == 0xC0 {
        skip = 2;
        let next = input.read_u8()?;
        let value = (((special & 0x1F) << 8) | next as i32) << 19 >> 19;
        let value = value.to_string();
        bytes = value.into_bytes();
    } else if (special & 0xFF) == 0xF1 {
        skip = 3;
        let value = input.read_i16::<LittleEndian>()?;
        let value = value.to_string();
        bytes = value.into_bytes();
    } else if (special & 0xFF) == 0xF2 {
        skip = 4;
        let value = input.read_i24::<LittleEndian>()?;
        let value = value.to_string();
        bytes = value.into_bytes();
    } else if (special & 0xFF) == 0xF3 {
        skip = 5;
        let value = input.read_i32::<LittleEndian>()?;
        let value = value.to_string();
        bytes = value.into_bytes();
    } else if (special & 0xFF) == 0xF4 {
        skip = 9;
        let value = input.read_i64::<LittleEndian>()?;
        let value = value.to_string();
        bytes = value.into_bytes();
    } else if (special & 0xF0) == 0xE0 {
        let next = input.read_u8()?;
        let len = ((special & 0x0F) << 8) | next as i32;
        skip = 2 + len as i32;
        bytes = vec![0; len as usize];
        input.read_exact(&mut bytes)?;
    } else if (special & 0xFF) == 0xF0 {
        let len = input.read_u32::<BigEndian>()?;
        skip = 5 + len as i32;
        bytes = vec![0; len as usize];
        input.read_exact(&mut bytes)?;
    } else {
        panic!("{}", special)
    }
    if skip <= 127 {
        let mut buf = vec![0; 1];
        input.read_exact(&mut buf)?;
    } else if skip < 16383 {
        let mut buf = vec![0; 2];
        input.read_exact(&mut buf)?;
    } else if skip < 2097151 {
        let mut buf = vec![0; 3];
        input.read_exact(&mut buf)?;
    } else if skip < 268435455 {
        let mut buf = vec![0; 4];
        input.read_exact(&mut buf)?;
    } else {
        let mut buf = vec![0; 5];
        input.read_exact(&mut buf)?;
    }
    Ok(bytes)
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
