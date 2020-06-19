/*!
 处理redis的响应数据
*/

use std::any::Any;
use std::cell::{RefCell, RefMut};
use std::collections::BTreeMap;
use std::f64::{INFINITY, NAN, NEG_INFINITY};
use std::fs::File;
use std::io::{BufWriter, Cursor, Error, ErrorKind, Read, Result, Write};
use std::iter::FromIterator;
use std::net::TcpStream;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use crate::iter::{
    IntSetIter, Iter, QuickListIter, SortedSetIter, StrValIter, ZipListIter, ZipMapIter,
};
use crate::rdb::Data::{Bytes, BytesVec, Empty};
use crate::rdb::*;
use crate::{io, lzf, to_string, Event, EventHandler, ModuleParser};

pub(crate) trait ReadWrite: Read + Write {
    fn as_any(&self) -> &dyn Any;
}

impl ReadWrite for TcpStream {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReadWrite for File {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) struct Conn {
    pub(crate) input: Box<dyn ReadWrite>,
    pub(crate) running: Arc<AtomicBool>,
    pub module_parser: Option<Rc<RefCell<dyn ModuleParser>>>,
    len: i64,
    marked: bool,
}

#[cfg(test)]
pub(crate) fn from_file(file: File) -> Conn {
    Conn {
        input: Box::new(file),
        running: Arc::new(AtomicBool::new(true)),
        module_parser: Option::None,
        len: 0,
        marked: false,
    }
}

pub(crate) fn new(input: TcpStream, running: Arc<AtomicBool>) -> Conn {
    Conn {
        input: Box::new(input),
        running,
        module_parser: Option::None,
        len: 0,
        marked: false,
    }
}

impl Conn {
    pub(crate) fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<()> {
        send(&mut self.input, command, args)?;
        Ok(())
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
    let mut writer = BufWriter::new(output);
    writer.write(&[STAR])?;
    let args_len = args.len() + 1;
    writer.write(&args_len.to_string().into_bytes())?;
    writer.write(&[CR, LF, DOLLAR])?;
    writer.write(&command.len().to_string().into_bytes())?;
    writer.write(&[CR, LF])?;
    writer.write(command)?;
    writer.write(&[CR, LF])?;
    for arg in args {
        writer.write(&[DOLLAR])?;
        writer.write(&arg.len().to_string().into_bytes())?;
        writer.write(&[CR, LF])?;
        writer.write(arg)?;
        writer.write(&[CR, LF])?;
    }
    writer.flush()
}

// 当redis响应的数据是Bulk string时，使用此方法读取指定length的字节, 并返回
pub(crate) fn read_bytes(
    input: &mut dyn Read,
    length: isize,
    _: &mut RefMut<dyn EventHandler>,
) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    if length > 0 {
        let mut bytes = vec![0; length as usize];
        input.read_exact(&mut bytes)?;
        let end = &mut [0; 2];
        input.read_exact(end)?;
        if end == b"\r\n" {
            return Ok(Bytes(bytes));
        } else {
            panic!("Expect CRLF after bulk string, but got: {:?}", end);
        }
    }
    return if length == 0 {
        // length == 0 代表空字符，后面还有CRLF
        input.read_exact(&mut [0; 2])?;
        Ok(Empty)
    } else {
        // length < 0 代表null
        Ok(Empty)
    };
}

// 跳过rdb的字节
pub(crate) fn skip(
    input: &mut Conn,
    length: isize,
    _: &mut RefMut<dyn EventHandler>,
) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    std::io::copy(
        &mut input.input.as_mut().take(length as u64),
        &mut std::io::sink(),
    )?;
    Ok(Data::Empty)
}

// 回车换行，在redis响应中一般表示终结符，或用作分隔符以分隔数据
pub(crate) const CR: u8 = b'\r';
pub(crate) const LF: u8 = b'\n';
// 代表array响应
pub(crate) const STAR: u8 = b'*';
// 代表bulk string响应
pub(crate) const DOLLAR: u8 = b'$';
// 代表simple string响应
pub(crate) const PLUS: u8 = b'+';
// 代表error响应
pub(crate) const MINUS: u8 = b'-';
// 代表integer响应
pub(crate) const COLON: u8 = b':';

pub const MODULE_SET: [char; 64] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '-', '_',
];
