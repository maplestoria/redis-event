use std::io;
use std::io::{Cursor, Error, ErrorKind, Read};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::rdb::{read_zip_list_entry, read_zm_len};
use crate::reader::Reader;

/// 迭代器接口的定义（迭代器方便处理大key，减轻内存使用）
///
/// 然后觉得没必要为每个类型封装结构体，而且rust我不熟...
/// 后续再看怎么优化代码

pub trait Iter {
    fn next(&mut self) -> io::Result<Vec<u8>>;
}

// 字符串类型的值迭代器
pub(crate) struct StrValIter<'a> {
    pub(crate) count: isize,
    pub(crate) input: &'a mut Reader,
}

impl Iter for StrValIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        while self.count > 0 {
            let val = self.input.read_string()?;
            self.count -= 1;
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// ListQuickList的值迭代器
pub(crate) struct QuickListIter<'a> {
    pub(crate) len: isize,
    pub(crate) count: isize,
    pub(crate) input: &'a mut Reader,
    pub(crate) cursor: Option<Cursor<Vec<u8>>>,
}

impl Iter for QuickListIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.len == -1 && self.count > 0 {
            let data = self.input.read_string()?;
            self.cursor = Option::Some(Cursor::new(data));
            // 跳过ZL_BYTES和ZL_TAIL
            let cursor = self.cursor.as_mut().unwrap();
            cursor.set_position(8);
            self.len = cursor.read_i16::<LittleEndian>()? as isize;
            if self.len == 0 {
                self.len = -1;
                self.count -= 1;
            }
            if self.has_more() {
                return self.next();
            }
        } else {
            if self.count > 0 {
                let val = read_zip_list_entry(self.cursor.as_mut().unwrap())?;
                self.len -= 1;
                if self.len == 0 {
                    self.len = -1;
                    self.count -= 1;
                }
                return Ok(val);
            }
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

impl QuickListIter<'_> {
    fn has_more(&self) -> bool {
        self.len > 0 || self.count > 0
    }
}

// ZipList的值迭代器
pub(crate) struct ZipListIter<'a> {
    pub(crate) count: isize,
    pub(crate) cursor: &'a mut Cursor<Vec<u8>>,
}

impl Iter for ZipListIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val = read_zip_list_entry(self.cursor)?;
            self.count -= 1;
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// SortedSet的值迭代器
pub(crate) struct SortedSetIter<'a> {
    pub(crate) count: isize,
    /// v = 1, zset
    /// v = 2, zset2
    pub(crate) v: u8,
    pub(crate) read_score: bool,
    pub(crate) input: &'a mut Reader,
}

impl Iter for SortedSetIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val;
            if self.read_score {
                if self.v == 1 {
                    val = self.input.read_double()?;
                } else {
                    // TODO zset2 score处理
                    let score = self.input.read_i64::<LittleEndian>()?;
                    val = score.to_string().into_bytes();
                }
                self.count -= 1;
                self.read_score = false;
            } else {
                val = self.input.read_string()?;
                self.read_score = true;
            }
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// HashZipMap的值迭代器
pub(crate) struct ZipMapIter<'a> {
    pub(crate) has_more: bool,
    pub(crate) read_val: bool,
    pub(crate) cursor: &'a mut Cursor<&'a Vec<u8>>,
}

impl Iter for ZipMapIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if !self.has_more {
            return Err(Error::new(ErrorKind::NotFound, "No element left"));
        }
        if self.read_val {
            let zm_len = read_zm_len(self.cursor)?;
            if zm_len == 255 {
                self.has_more = false;
                return Ok(Vec::new());
            }
            let free = self.cursor.read_i8()?;
            let mut value = Vec::with_capacity(zm_len as usize);
            self.cursor.read_exact(&mut value)?;
            self.cursor.set_position(self.cursor.position() + free as u64);
            return Ok(value);
        } else {
            let zm_len = read_zm_len(self.cursor)?;
            if zm_len == 255 {
                self.has_more = false;
                return Err(Error::new(ErrorKind::NotFound, "No element left"));
            }
            let mut field = Vec::with_capacity(zm_len as usize);
            self.cursor.read_exact(&mut field)?;
            return Ok(field);
        }
    }
}

// IntSet的值迭代器
pub(crate) struct IntSetIter<'a> {
    pub(crate) encoding: i32,
    pub(crate) count: isize,
    pub(crate) cursor: &'a mut Cursor<&'a Vec<u8>>,
}

impl Iter for IntSetIter<'_> {
    fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val;
            match self.encoding {
                2 => {
                    let member = self.cursor.read_i16::<LittleEndian>()?;
                    let member = member.to_string().into_bytes();
                    val = member;
                }
                4 => {
                    let member = self.cursor.read_i32::<LittleEndian>()?;
                    let member = member.to_string().into_bytes();
                    val = member;
                }
                8 => {
                    let member = self.cursor.read_i64::<LittleEndian>()?;
                    let member = member.to_string().into_bytes();
                    val = member;
                }
                _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid integer size")),
            }
            self.count -= 1;
            return Ok(val);
        }
        return Err(Error::new(ErrorKind::NotFound, "No element left"));
    }
}