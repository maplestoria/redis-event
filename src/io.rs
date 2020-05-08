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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use crate::{Event, EventHandler, io, lzf, ModuleParser, to_string};
use crate::iter::{IntSetIter, Iter, QuickListIter, SortedSetIter, StrValIter, ZipListIter, ZipMapIter};
use crate::rdb::*;
use crate::rdb::Data::{Bytes, BytesVec, Empty};

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
    Conn { input: Box::new(file), running: Arc::new(AtomicBool::new(true)), module_parser: Option::None, len: 0, marked: false }
}

pub(crate) fn new(input: TcpStream, running: Arc<AtomicBool>) -> Conn {
    Conn { input: Box::new(input), running, module_parser: Option::None, len: 0, marked: false }
}

impl Conn {
    pub(crate) fn reply(&mut self,
                        func: fn(&mut Conn, isize,
                                 &mut RefMut<dyn EventHandler>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>>,
                        event_handler: &mut RefMut<dyn EventHandler>, ) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
        loop {
            let response_type = self.read_u8()?;
            match response_type {
                // Plus: Simple String
                // Minus: Error
                // Colon: Integer
                PLUS | MINUS | COLON => {
                    let mut bytes = vec![];
                    loop {
                        let byte = self.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = self.read_u8()?;
                    if byte == LF {
                        return if response_type == PLUS || response_type == COLON {
                            Ok(Bytes(bytes))
                        } else {
                            let message = to_string(bytes);
                            Err(Error::new(ErrorKind::InvalidInput, message))
                        };
                    } else {
                        panic!("Expect LF after CR");
                    }
                }
                DOLLAR => { // Bulk String
                    let mut bytes = vec![];
                    loop {
                        let byte = self.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = self.read_u8()?;
                    if byte == LF {
                        let length = to_string(bytes);
                        let length = length.parse::<isize>().unwrap();
                        return func(self, length, event_handler);
                    } else {
                        panic!("Expect LF after CR");
                    }
                }
                STAR => { // Array
                    let mut bytes = vec![];
                    loop {
                        let byte = self.read_u8()?;
                        if byte != CR {
                            bytes.push(byte);
                        } else {
                            break;
                        }
                    }
                    let byte = self.read_u8()?;
                    if byte == LF {
                        let length = to_string(bytes);
                        let length = length.parse::<isize>().unwrap();
                        return if length <= 0 {
                            Ok(Empty)
                        } else {
                            let mut result = Vec::with_capacity(length as usize);
                            for _ in 0..length {
                                match self.reply(io::read_bytes, event_handler)? {
                                    Bytes(resp) => {
                                        result.push(resp);
                                    }
                                    BytesVec(mut resp) => {
                                        result.append(&mut resp);
                                    }
                                    Empty => panic!("Expect Redis response, but got empty")
                                }
                            }
                            Ok(BytesVec(result))
                        };
                    } else {
                        panic!("Expect LF after CR");
                    }
                }
                LF => {
                    // 无需处理
                }
                _ => {
                    panic!("错误的响应类型: {}", response_type);
                }
            }
        }
    }
    
    pub(crate) fn mark(&mut self) {
        self.marked = true;
    }
    
    pub(crate) fn unmark(&mut self) -> Result<i64> {
        if self.marked {
            let len = self.len;
            self.len = 0;
            self.marked = false;
            return Ok(len);
        }
        return Err(Error::new(ErrorKind::Other, "not marked"));
    }
    
    pub(crate) fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.input.read_exact(&mut buf)?;
        if self.marked {
            self.len += 1;
        };
        Ok(buf[0])
    }
    
    pub(crate) fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.input.read_exact(buf)?;
        if self.marked {
            self.len += buf.len() as i64;
        };
        Ok(())
    }
    
    pub(crate) fn read_u64<O: ByteOrder>(&mut self) -> Result<u64> {
        let int = self.input.read_u64::<O>()?;
        if self.marked {
            self.len += 8;
        };
        Ok(int)
    }
    
    pub(crate) fn read_i8(&mut self) -> Result<i8> {
        let int = self.input.read_i8()?;
        if self.marked {
            self.len += 1;
        };
        Ok(int)
    }
    
    pub(crate) fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<()> {
        send(&mut self.input, command, args)?;
        Ok(())
    }
    
    // 读取redis响应中下一条数据的长度
    pub(crate) fn read_length(&mut self) -> Result<(isize, bool)> {
        let byte = self.read_u8()?;
        let _type = (byte & 0xC0) >> 6;
        
        let mut result = -1;
        let mut is_encoded = false;
        
        if _type == RDB_ENCVAL {
            result = (byte & 0x3F) as isize;
            is_encoded = true;
        } else if _type == RDB_6BITLEN {
            result = (byte & 0x3F) as isize;
        } else if _type == RDB_14BITLEN {
            let next_byte = self.read_u8()?;
            result = (((byte as u16 & 0x3F) << 8) | next_byte as u16) as isize;
        } else if byte == RDB_32BITLEN {
            result = self.read_integer(4, true)?;
        } else if byte == RDB_64BITLEN {
            result = self.read_integer(8, true)?;
        };
        Ok((result, is_encoded))
    }
    
    // 从流中读取一个Integer
    pub(crate) fn read_integer(&mut self, size: isize, is_big_endian: bool) -> Result<isize> {
        let mut buff = vec![0; size as usize];
        self.read_exact(&mut buff)?;
        let mut cursor = Cursor::new(&buff);
        
        if is_big_endian {
            if size == 2 {
                return Ok(cursor.read_i16::<BigEndian>()? as isize);
            } else if size == 4 {
                return Ok(cursor.read_i32::<BigEndian>()? as isize);
            } else if size == 8 {
                return Ok(cursor.read_i64::<BigEndian>()? as isize);
            };
        } else {
            if size == 2 {
                return Ok(cursor.read_i16::<LittleEndian>()? as isize);
            } else if size == 4 {
                return Ok(cursor.read_i32::<LittleEndian>()? as isize);
            } else if size == 8 {
                return Ok(cursor.read_i64::<LittleEndian>()? as isize);
            };
        }
        panic!("Invalid integer size: {}", size)
    }
    
    // 从流中读取一个string
    pub(crate) fn read_string(&mut self) -> Result<Vec<u8>> {
        let (length, is_encoded) = self.read_length()?;
        if is_encoded {
            match length {
                RDB_ENC_INT8 => {
                    let int = self.read_i8()?;
                    return Ok(int.to_string().into_bytes());
                }
                RDB_ENC_INT16 => {
                    let int = self.read_integer(2, false)?;
                    return Ok(int.to_string().into_bytes());
                }
                RDB_ENC_INT32 => {
                    let int = self.read_integer(4, false)?;
                    return Ok(int.to_string().into_bytes());
                }
                RDB_ENC_LZF => {
                    let (compressed_len, _) = self.read_length()?;
                    let (origin_len, _) = self.read_length()?;
                    let mut compressed = vec![0; compressed_len as usize];
                    self.read_exact(&mut compressed)?;
                    let mut origin = vec![0; origin_len as usize];
                    lzf::decompress(&mut compressed, compressed_len, &mut origin, origin_len);
                    return Ok(origin);
                }
                _ => panic!("Invalid string length: {}", length)
            };
        };
        let mut buff = vec![0; length as usize];
        self.read_exact(&mut buff)?;
        Ok(buff)
    }
    
    // 从流中读取一个double
    pub(crate) fn read_double(&mut self) -> Result<f64> {
        let len = self.read_u8()?;
        return match len {
            255 => {
                Ok(NEG_INFINITY)
            }
            254 => {
                Ok(INFINITY)
            }
            253 => {
                Ok(NAN)
            }
            _ => {
                let mut buff = vec![0; len as usize];
                self.read_exact(&mut buff)?;
                let score_str = to_string(buff);
                let score = score_str.parse::<f64>().unwrap();
                Ok(score)
            }
        };
    }
    
    // 根据传入的数据类型，从流中读取对应类型的数据
    pub(crate) fn read_object(&mut self, value_type: u8,
                              event_handler: &mut RefMut<dyn EventHandler>,
                              meta: &Meta) -> Result<()> {
        match value_type {
            RDB_TYPE_STRING => {
                let key = self.read_string()?;
                let value = self.read_string()?;
                event_handler.handle(Event::RDB(Object::String(KeyValue { key: &key, value: &value, meta })));
            }
            RDB_TYPE_LIST | RDB_TYPE_SET => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count, input: self };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        if value_type == RDB_TYPE_LIST {
                            event_handler.handle(Event::RDB(Object::List(List { key: &key, values: &val, meta })));
                        } else {
                            event_handler.handle(Event::RDB(Object::Set(Set { key: &key, members: &val, meta })));
                        }
                    }
                }
            }
            RDB_TYPE_ZSET => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 1, input: self };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::SortedSet(SortedSet { key: &key, items: &val, meta })));
                    }
                }
            }
            RDB_TYPE_ZSET_2 => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = SortedSetIter { count, v: 2, input: self };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::SortedSet(SortedSet { key: &key, items: &val, meta })));
                    }
                }
            }
            RDB_TYPE_HASH => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = StrValIter { count: count * 2, input: self };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let name;
                        let value;
                        if let Ok(next_val) = iter.next() {
                            name = next_val;
                            value = iter.next().expect("missing hash field value");
                            val.push(Field { name, value });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::Hash(Hash { key: &key, fields: &val, meta })));
                    }
                }
            }
            RDB_TYPE_HASH_ZIPMAP => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(&bytes);
                cursor.set_position(1);
                let mut iter = ZipMapIter { has_more: true, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut fields = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(field) = iter.next() {
                            fields.push(field);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !fields.is_empty() {
                        event_handler.handle(Event::RDB(Object::Hash(Hash { key: &key, fields: &fields, meta })));
                    }
                }
            }
            RDB_TYPE_LIST_ZIPLIST => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let count = cursor.read_u16::<LittleEndian>()? as isize;
                let mut iter = ZipListIter { count, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::List(List { key: &key, values: &val, meta })));
                    }
                }
            }
            RDB_TYPE_HASH_ZIPLIST => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let count = cursor.read_u16::<LittleEndian>()? as isize;
                let mut iter = ZipListIter { count, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let name;
                        let value;
                        if let Ok(next_val) = iter.next() {
                            name = next_val;
                            value = iter.next().expect("missing hash field value");
                            val.push(Field { name, value });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::Hash(Hash { key: &key, fields: &val, meta })));
                    }
                }
            }
            RDB_TYPE_ZSET_ZIPLIST => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let cursor = &mut Cursor::new(bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let count = cursor.read_u16::<LittleEndian>()? as isize;
                let mut iter = ZipListIter { count, cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let member;
                        let score: f64;
                        if let Ok(next_val) = iter.next() {
                            member = next_val;
                            let score_str = to_string(iter.next()
                                .expect("missing sorted set element's score"));
                            score = score_str.parse::<f64>().unwrap();
                            val.push(Item { member, score });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::SortedSet(SortedSet { key: &key, items: &val, meta })));
                    }
                }
            }
            RDB_TYPE_SET_INTSET => {
                let key = self.read_string()?;
                let bytes = self.read_string()?;
                let mut cursor = Cursor::new(&bytes);
                let encoding = cursor.read_i32::<LittleEndian>()?;
                let length = cursor.read_u32::<LittleEndian>()?;
                let mut iter = IntSetIter { encoding, count: length as isize, cursor: &mut cursor };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::Set(Set { key: &key, members: &val, meta })));
                    }
                }
            }
            RDB_TYPE_LIST_QUICKLIST => {
                let key = self.read_string()?;
                let (count, _) = self.read_length()?;
                let mut iter = QuickListIter { len: -1, count, input: self, cursor: Option::None };
                
                let mut has_more = true;
                while has_more {
                    let mut val = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        if let Ok(next_val) = iter.next() {
                            val.push(next_val);
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::List(List { key: &key, values: &val, meta })));
                    }
                }
            }
            RDB_TYPE_MODULE | RDB_TYPE_MODULE_2 => {
                let key = self.read_string()?;
                let (module_id, _) = self.read_length()?;
                let module_id = module_id as usize;
                let mut array: [char; 9] = [' '; 9];
                for i in 0..array.len() {
                    let i1 = 10 + (array.len() - 1 - i) * 6;
                    let i2 = (module_id >> i1 as usize) as usize;
                    let i3 = i2 & 63;
                    let chr = MODULE_SET.get(i3).unwrap();
                    array[i] = *chr;
                }
                let module_name: String = String::from_iter(array.iter());
                let module_version: usize = module_id & 1023;
                if self.module_parser.is_none() && value_type == RDB_TYPE_MODULE {
                    panic!("MODULE {}, version {} 无法解析", module_name, module_version);
                }
                if let Some(parser) = &mut self.module_parser {
                    let module: Box<dyn Module>;
                    if value_type == RDB_TYPE_MODULE_2 {
                        module = parser.borrow_mut().parse(&mut self.input, &module_name, 2);
                        let (len, _) = self.read_length()?;
                        if len != 0 {
                            panic!("module '{}' that is not terminated by EOF marker, but {}",
                                   &module_name, len);
                        }
                    } else {
                        module = parser.borrow_mut().parse(&mut self.input, &module_name, module_version);
                    }
                    event_handler.handle(Event::RDB(Object::Module(key, module)));
                } else {
                    // 没有parser，并且是Module 2类型的值，那就可以直接跳过了
                    self.rdb_load_check_module_value()?;
                }
            }
            RDB_TYPE_STREAM_LISTPACKS => {
                let key = self.read_string()?;
                let stream = self.read_stream_list_packs()?;
                event_handler.handle(Event::RDB(Object::Stream(key, stream)));
            }
            _ => panic!("unknown data type: {}", value_type)
        }
        Ok(())
    }
    
    pub(crate) fn rdb_load_check_module_value(&mut self) -> Result<()> {
        loop {
            let (op_code, _) = self.read_length()?;
            if op_code == RDB_MODULE_OPCODE_EOF {
                break;
            }
            if op_code == RDB_MODULE_OPCODE_SINT || op_code == RDB_MODULE_OPCODE_UINT {
                self.read_length()?;
            } else if op_code == RDB_MODULE_OPCODE_STRING {
                self.read_string()?;
            } else if op_code == RDB_MODULE_OPCODE_FLOAT {
                self.read_exact(&mut [0; 4])?;
            } else if op_code == RDB_MODULE_OPCODE_DOUBLE {
                self.read_exact(&mut [0; 8])?;
            }
        }
        Ok(())
    }
    
    pub(crate) fn read_stream_list_packs(&mut self) -> Result<Stream> {
        let mut entries: BTreeMap<ID, Entry> = BTreeMap::new();
        let (length, _) = self.read_length()?;
        for _ in 0..length {
            let raw_id = self.read_string()?;
            let mut cursor = Cursor::new(&raw_id);
            let ms = read_long(&mut cursor, 8, false)?;
            let seq = read_long(&mut cursor, 8, false)?;
            let base_id = ID { ms, seq };
            let raw_list_packs = self.read_string()?;
            let mut list_pack = Cursor::new(&raw_list_packs);
            list_pack.set_position(6);
            let count = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?))
                .unwrap();
            let deleted = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?))
                .unwrap();
            let num_fields = i32::from_str(&to_string(read_list_pack_entry(&mut list_pack)?))
                .unwrap();
            let mut tmp_fields = Vec::with_capacity(num_fields as usize);
            for _ in 0..num_fields {
                tmp_fields.push(read_list_pack_entry(&mut list_pack)?);
            }
            read_list_pack_entry(&mut list_pack)?;
            
            let total = count + deleted;
            for _ in 0..total {
                let mut fields = BTreeMap::new();
                let flag = i32::from_str(&to_string(read_list_pack_entry(&mut list_pack)?))
                    .unwrap();
                let ms = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?))
                    .unwrap();
                let seq = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?))
                    .unwrap();
                let id = ID { ms: ms + base_id.ms, seq: seq + base_id.seq };
                let deleted = (flag & 1) != 0;
                if (flag & 2) != 0 {
                    for i in 0..num_fields {
                        let value = read_list_pack_entry(&mut list_pack)?;
                        let field = tmp_fields.get(i as usize).unwrap().to_vec();
                        fields.insert(field, value);
                    }
                    entries.insert(id, Entry { id, deleted, fields });
                } else {
                    let num_fields = i32::from_str(&to_string(read_list_pack_entry(&mut list_pack)?))
                        .unwrap();
                    for _ in 0..num_fields {
                        let field = read_list_pack_entry(&mut list_pack)?;
                        let value = read_list_pack_entry(&mut list_pack)?;
                        fields.insert(field, value);
                    }
                    entries.insert(id, Entry { id, deleted, fields });
                }
                read_list_pack_entry(&mut list_pack)?;
            }
            let end = list_pack.read_u8()?;
            if end != 255 {
                panic!("listpack expect 255 but {}", end);
            }
        }
        self.read_length()?;
        self.read_length()?;
        self.read_length()?;
        
        let mut groups: Vec<Group> = Vec::new();
        let (count, _) = self.read_length()?;
        for _ in 0..count {
            let name = self.read_string()?;
            let (ms, _) = self.read_length()?;
            let (seq, _) = self.read_length()?;
            let group_last_id = ID { ms: ms as i64, seq: seq as i64 };
            groups.push(Group { name, last_id: group_last_id });
            
            let (global_pel, _) = self.read_length()?;
            for _ in 0..global_pel {
                read_long(&mut self.input, 8, false)?;
                read_long(&mut self.input, 8, false)?;
                self.read_integer(8, false)?;
                self.read_length()?;
            }
            
            let (consumer_count, _) = self.read_length()?;
            for _ in 0..consumer_count {
                self.read_string()?;
                self.read_integer(8, false)?;
                
                let (pel, _) = self.read_length()?;
                for _ in 0..pel {
                    read_long(&mut self.input, 8, false)?;
                    read_long(&mut self.input, 8, false)?;
                }
            }
        }
        Ok(Stream { entries, groups })
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
pub(crate) fn read_bytes(input: &mut Conn, length: isize,
                         _: &mut RefMut<dyn EventHandler>, ) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
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
pub(crate) fn skip(input: &mut Conn,
                   length: isize,
                   _: &mut RefMut<dyn EventHandler>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>> {
    std::io::copy(&mut input.input.as_mut().take(length as u64), &mut std::io::sink())?;
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
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'];