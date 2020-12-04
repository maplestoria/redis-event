/*!
RDB中各项Redis数据相关的结构体定义，以及RDB解析相关的代码在此模块下
*/
use core::result;
use std::any::Any;
use std::cmp;
use std::collections::BTreeMap;
use std::fmt::{Debug, Error, Formatter};
use std::io::{Cursor, Read, Result};
use std::sync::atomic::{AtomicBool, Ordering};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use log::info;

use crate::cmd::connection::SELECT;
use crate::cmd::Command;
use crate::iter::{IntSetIter, Iter, QuickListIter, SortedSetIter, StrValIter, ZipListIter, ZipMapIter};
use crate::{lzf, to_string, Event, EventHandler, ModuleParser, RDBParser};
use std::cell::RefCell;
use std::f64::{INFINITY, NAN, NEG_INFINITY};
use std::iter::FromIterator;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;

/// 一些解析RDB数据的方法
pub trait RDBDecode: Read {
    /// 读取redis响应中下一条数据的长度
    fn read_length(&mut self) -> Result<(isize, bool)> {
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

    /// 从流中读取一个Integer
    fn read_integer(&mut self, size: isize, is_big_endian: bool) -> Result<isize> {
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

    /// 从流中读取一个string
    fn read_string(&mut self) -> Result<Vec<u8>> {
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
                    lzf::decompress(&compressed, &mut origin);
                    return Ok(origin);
                }
                _ => panic!("Invalid string length: {}", length),
            };
        };
        let mut buff = vec![0; length as usize];
        self.read_exact(&mut buff)?;
        Ok(buff)
    }

    /// 从流中读取一个double
    fn read_double(&mut self) -> Result<f64> {
        let len = self.read_u8()?;
        return match len {
            255 => Ok(NEG_INFINITY),
            254 => Ok(INFINITY),
            253 => Ok(NAN),
            _ => {
                let mut buff = vec![0; len as usize];
                self.read_exact(&mut buff)?;
                let score_str = to_string(buff);
                let score = score_str.parse::<f64>().unwrap();
                Ok(score)
            }
        };
    }
}

impl<R: Read + ?Sized> RDBDecode for R {}

pub(crate) struct DefaultRDBParser {
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) module_parser: Option<Rc<RefCell<dyn ModuleParser>>>,
}

impl RDBParser for DefaultRDBParser {
    fn parse(&mut self, input: &mut dyn Read, _: i64, event_handler: &mut dyn EventHandler) -> Result<()> {
        event_handler.handle(Event::RDB(Object::BOR));
        let mut bytes = vec![0; 5];
        // 开头5个字节: REDIS
        input.read_exact(&mut bytes)?;
        // 4个字节: rdb版本
        input.read_exact(&mut bytes[..=3])?;
        let rdb_version = String::from_utf8_lossy(&bytes[..=3]);
        let rdb_version = rdb_version.parse::<isize>().unwrap();
        let mut db = 0;

        while self.running.load(Ordering::Relaxed) {
            let mut meta = Meta {
                db,
                expire: None,
                evict: None,
            };

            let data_type = input.read_u8()?;
            match data_type {
                RDB_OPCODE_AUX => {
                    let field_name = input.read_string()?;
                    let field_val = input.read_string()?;
                    let field_name = to_string(field_name);
                    let field_val = to_string(field_val);
                    info!("{}:{}", field_name, field_val);
                }
                RDB_OPCODE_SELECTDB => {
                    let (_db, _) = input.read_length()?;
                    meta.db = _db;
                    db = _db;
                    let cmd = SELECT { db: _db as i32 };
                    event_handler.handle(Event::AOF(Command::SELECT(&cmd)));
                }
                RDB_OPCODE_RESIZEDB => {
                    let (total, _) = input.read_length()?;
                    info!("db[{}] total keys: {}", db, total);
                    let (expired, _) = input.read_length()?;
                    info!("db[{}] expired keys: {}", db, expired);
                }
                RDB_OPCODE_EXPIRETIME | RDB_OPCODE_EXPIRETIME_MS => {
                    if data_type == RDB_OPCODE_EXPIRETIME_MS {
                        let expired_time = input.read_integer(8, false)?;
                        meta.expire = Option::Some((ExpireType::Millisecond, expired_time as i64));
                    } else {
                        let expired_time = input.read_integer(4, false)?;
                        meta.expire = Option::Some((ExpireType::Second, expired_time as i64));
                    }
                    let value_type = input.read_u8()?;
                    match value_type {
                        RDB_OPCODE_FREQ => {
                            let val = input.read_u8()?;
                            let value_type = input.read_u8()?;
                            meta.evict = Option::Some((EvictType::LFU, val as i64));
                            self.read_object(input, value_type, event_handler, &meta)?;
                        }
                        RDB_OPCODE_IDLE => {
                            let (val, _) = input.read_length()?;
                            let value_type = input.read_u8()?;
                            meta.evict = Option::Some((EvictType::LRU, val as i64));
                            self.read_object(input, value_type, event_handler, &meta)?;
                        }
                        _ => {
                            self.read_object(input, value_type, event_handler, &meta)?;
                        }
                    }
                }
                RDB_OPCODE_FREQ => {
                    let val = input.read_u8()?;
                    let value_type = input.read_u8()?;
                    meta.evict = Option::Some((EvictType::LFU, val as i64));
                    self.read_object(input, value_type, event_handler, &meta)?;
                }
                RDB_OPCODE_IDLE => {
                    let (val, _) = input.read_length()?;
                    meta.evict = Option::Some((EvictType::LRU, val as i64));
                    let value_type = input.read_u8()?;
                    self.read_object(input, value_type, event_handler, &meta)?;
                }
                RDB_OPCODE_MODULE_AUX => {
                    input.read_length()?;
                    self.rdb_load_check_module_value(input)?;
                }
                RDB_OPCODE_EOF => {
                    if rdb_version >= 5 {
                        input.read_integer(8, true)?;
                    }
                    break;
                }
                _ => {
                    self.read_object(input, data_type, event_handler, &meta)?;
                }
            };
        }
        event_handler.handle(Event::RDB(Object::EOR));
        Ok(())
    }
}

impl DefaultRDBParser {
    // 根据传入的数据类型，从流中读取对应类型的数据
    fn read_object(
        &mut self, input: &mut dyn Read, value_type: u8, event_handler: &mut dyn EventHandler, meta: &Meta,
    ) -> Result<()> {
        match value_type {
            RDB_TYPE_STRING => {
                let key = input.read_string()?;
                let value = input.read_string()?;
                event_handler.handle(Event::RDB(Object::String(KeyValue {
                    key: &key,
                    value: &value,
                    meta,
                })));
            }
            RDB_TYPE_LIST | RDB_TYPE_SET => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = StrValIter { count, input };

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
                            event_handler.handle(Event::RDB(Object::List(List {
                                key: &key,
                                values: &val,
                                meta,
                            })));
                        } else {
                            event_handler.handle(Event::RDB(Object::Set(Set {
                                key: &key,
                                members: &val,
                                meta,
                            })));
                        }
                    }
                }
            }
            RDB_TYPE_ZSET => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = SortedSetIter { count, v: 1, input };

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
                        event_handler.handle(Event::RDB(Object::SortedSet(SortedSet {
                            key: &key,
                            items: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_ZSET_2 => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = SortedSetIter { count, v: 2, input };

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
                        event_handler.handle(Event::RDB(Object::SortedSet(SortedSet {
                            key: &key,
                            items: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_HASH => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = StrValIter {
                    count: count * 2,
                    input,
                };

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
                        event_handler.handle(Event::RDB(Object::Hash(Hash {
                            key: &key,
                            fields: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_HASH_ZIPMAP => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
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
                        event_handler.handle(Event::RDB(Object::Hash(Hash {
                            key: &key,
                            fields: &fields,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_LIST_ZIPLIST => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
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
                        event_handler.handle(Event::RDB(Object::List(List {
                            key: &key,
                            values: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_HASH_ZIPLIST => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
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
                        event_handler.handle(Event::RDB(Object::Hash(Hash {
                            key: &key,
                            fields: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_ZSET_ZIPLIST => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
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
                            let score_str = to_string(iter.next().expect("missing sorted set element's score"));
                            score = score_str.parse::<f64>().unwrap();
                            val.push(Item { member, score });
                        } else {
                            has_more = false;
                            break;
                        }
                    }
                    if !val.is_empty() {
                        event_handler.handle(Event::RDB(Object::SortedSet(SortedSet {
                            key: &key,
                            items: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_SET_INTSET => {
                let key = input.read_string()?;
                let bytes = input.read_string()?;
                let mut cursor = Cursor::new(&bytes);
                let encoding = cursor.read_i32::<LittleEndian>()?;
                let length = cursor.read_u32::<LittleEndian>()?;
                let mut iter = IntSetIter {
                    encoding,
                    count: length as isize,
                    cursor: &mut cursor,
                };

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
                        event_handler.handle(Event::RDB(Object::Set(Set {
                            key: &key,
                            members: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_LIST_QUICKLIST => {
                let key = input.read_string()?;
                let (count, _) = input.read_length()?;
                let mut iter = QuickListIter {
                    len: -1,
                    count,
                    input,
                    cursor: Option::None,
                };

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
                        event_handler.handle(Event::RDB(Object::List(List {
                            key: &key,
                            values: &val,
                            meta,
                        })));
                    }
                }
            }
            RDB_TYPE_MODULE | RDB_TYPE_MODULE_2 => {
                let key = input.read_string()?;
                let (module_id, _) = input.read_length()?;
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
                        module = parser.borrow_mut().parse(input, &module_name, 2);
                        let (len, _) = input.read_length()?;
                        if len != 0 {
                            panic!(
                                "module '{}' that is not terminated by EOF marker, but {}",
                                &module_name, len
                            );
                        }
                    } else {
                        module = parser.borrow_mut().parse(input, &module_name, module_version);
                    }
                    event_handler.handle(Event::RDB(Object::Module(key, module, meta)));
                } else {
                    // 没有parser，并且是Module 2类型的值，那就可以直接跳过了
                    self.rdb_load_check_module_value(input)?;
                }
            }
            RDB_TYPE_STREAM_LISTPACKS => {
                let key = input.read_string()?;
                let stream = self.read_stream_list_packs(meta, input)?;
                event_handler.handle(Event::RDB(Object::Stream(key, stream)));
            }
            _ => panic!("unknown data type: {}", value_type),
        }
        Ok(())
    }

    fn rdb_load_check_module_value(&mut self, input: &mut dyn Read) -> Result<()> {
        loop {
            let (op_code, _) = input.read_length()?;
            if op_code == RDB_MODULE_OPCODE_EOF {
                break;
            }
            if op_code == RDB_MODULE_OPCODE_SINT || op_code == RDB_MODULE_OPCODE_UINT {
                input.read_length()?;
            } else if op_code == RDB_MODULE_OPCODE_STRING {
                input.read_string()?;
            } else if op_code == RDB_MODULE_OPCODE_FLOAT {
                input.read_exact(&mut [0; 4])?;
            } else if op_code == RDB_MODULE_OPCODE_DOUBLE {
                input.read_exact(&mut [0; 8])?;
            }
        }
        Ok(())
    }

    fn read_stream_list_packs<'a>(&mut self, meta: &'a Meta, input: &mut dyn Read) -> Result<Stream<'a>> {
        let mut entries: BTreeMap<ID, Entry> = BTreeMap::new();
        let (length, _) = input.read_length()?;
        for _ in 0..length {
            let raw_id = input.read_string()?;
            let mut cursor = Cursor::new(&raw_id);
            let ms = read_long(&mut cursor, 8, false)?;
            let seq = read_long(&mut cursor, 8, false)?;
            let base_id = ID { ms, seq };
            let raw_list_packs = input.read_string()?;
            let mut list_pack = Cursor::new(&raw_list_packs);
            list_pack.set_position(6);
            let count = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?)).unwrap();
            let deleted = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?)).unwrap();
            let num_fields = i32::from_str(&to_string(read_list_pack_entry(&mut list_pack)?)).unwrap();
            let mut tmp_fields = Vec::with_capacity(num_fields as usize);
            for _ in 0..num_fields {
                tmp_fields.push(read_list_pack_entry(&mut list_pack)?);
            }
            read_list_pack_entry(&mut list_pack)?;

            let total = count + deleted;
            for _ in 0..total {
                let mut fields = BTreeMap::new();
                let flag = i32::from_str(&to_string(read_list_pack_entry(&mut list_pack)?)).unwrap();
                let ms = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?)).unwrap();
                let seq = i64::from_str(&to_string(read_list_pack_entry(&mut list_pack)?)).unwrap();
                let id = ID {
                    ms: ms + base_id.ms,
                    seq: seq + base_id.seq,
                };
                let deleted = (flag & 1) != 0;
                if (flag & 2) != 0 {
                    for i in 0..num_fields {
                        let value = read_list_pack_entry(&mut list_pack)?;
                        let field = tmp_fields.get(i as usize).unwrap().to_vec();
                        fields.insert(field, value);
                    }
                    entries.insert(id, Entry { id, deleted, fields });
                } else {
                    let num_fields = i32::from_str(&to_string(read_list_pack_entry(&mut list_pack)?)).unwrap();
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
        input.read_length()?;
        input.read_length()?;
        input.read_length()?;

        let mut groups: Vec<Group> = Vec::new();
        let (count, _) = input.read_length()?;
        for _ in 0..count {
            let name = input.read_string()?;
            let (ms, _) = input.read_length()?;
            let (seq, _) = input.read_length()?;
            let group_last_id = ID {
                ms: ms as i64,
                seq: seq as i64,
            };
            groups.push(Group {
                name,
                last_id: group_last_id,
            });

            let (global_pel, _) = input.read_length()?;
            for _ in 0..global_pel {
                read_long(input, 8, false)?;
                read_long(input, 8, false)?;
                input.read_integer(8, false)?;
                input.read_length()?;
            }

            let (consumer_count, _) = input.read_length()?;
            for _ in 0..consumer_count {
                input.read_string()?;
                input.read_integer(8, false)?;

                let (pel, _) = input.read_length()?;
                for _ in 0..pel {
                    read_long(input, 8, false)?;
                    read_long(input, 8, false)?;
                }
            }
        }
        Ok(Stream { entries, groups, meta })
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

pub(crate) fn read_zm_len(cursor: &mut Cursor<&Vec<u8>>) -> Result<usize> {
    let len = cursor.read_u8()?;
    if len <= 253 {
        return Ok(len as usize);
    } else if len == 254 {
        let value = cursor.read_u32::<BigEndian>()?;
        return Ok(value as usize);
    }
    Ok(len as usize)
}

pub(crate) fn read_zip_list_entry(cursor: &mut Cursor<Vec<u8>>) -> Result<Vec<u8>> {
    if cursor.read_u8()? >= 254 {
        cursor.read_u32::<LittleEndian>()?;
    }
    let flag = cursor.read_u8()?;
    match flag >> 6 {
        0 => {
            let length = flag & 0x3F;
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        1 => {
            let next_byte = cursor.read_u8()?;
            let length = (((flag as u16) & 0x3F) << 8) | (next_byte as u16);
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        2 => {
            let length = cursor.read_u32::<BigEndian>()?;
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        _ => {}
    }
    return match flag {
        ZIP_INT_8BIT => {
            let int = cursor.read_i8()?;
            Ok(int.to_string().into_bytes())
        }
        ZIP_INT_16BIT => {
            let int = cursor.read_i16::<LittleEndian>()?;
            Ok(int.to_string().into_bytes())
        }
        ZIP_INT_24BIT => {
            let int = cursor.read_i24::<LittleEndian>()?;
            Ok(int.to_string().into_bytes())
        }
        ZIP_INT_32BIT => {
            let int = cursor.read_i32::<LittleEndian>()?;
            Ok(int.to_string().into_bytes())
        }
        ZIP_INT_64BIT => {
            let int = cursor.read_i64::<LittleEndian>()?;
            Ok(int.to_string().into_bytes())
        }
        _ => {
            let result = (flag - 0xF1) as isize;
            Ok(result.to_string().into_bytes())
        }
    };
}

/// 封装Redis中的各种数据类型，由`RdbHandler`统一处理
#[derive(Debug)]
pub enum Object<'a> {
    /// 代表Redis中的String类型数据
    String(KeyValue<'a>),
    /// 代表Redis中的List类型数据
    List(List<'a>),
    /// 代表Redis中的Set类型数据
    Set(Set<'a>),
    /// 代表Redis中的SortedSet类型数据
    SortedSet(SortedSet<'a>),
    /// 代表Redis中的Hash类型数据
    Hash(Hash<'a>),
    /// 代表Redis中的module, 需要额外实现Module解析器
    Module(Vec<u8>, Box<dyn Module>, &'a Meta),
    /// 代表Redis中的Stream类型数据
    Stream(Vec<u8>, Stream<'a>),
    /// 代表rdb数据解析开始
    BOR,
    /// 代表rdb数据解析完毕
    EOR,
}

pub trait Module {
    fn as_any(&self) -> &dyn Any;
}

impl Debug for dyn Module {
    fn fmt(&self, _: &mut Formatter) -> result::Result<(), Error> {
        unimplemented!()
    }
}

/// 数据的元信息, 包括数据过期类型, 内存驱逐类型, 数据所属的db
#[derive(Debug)]
pub struct Meta {
    /// 数据所属的db
    pub db: isize,
    /// 左为过期时间类型，右为过期时间
    pub expire: Option<(ExpireType, i64)>,
    /// 左为内存驱逐类型，右为被驱逐掉的值
    pub evict: Option<(EvictType, i64)>,
}

/// 过期类型
#[derive(Debug)]
pub enum ExpireType {
    /// 以秒计算过期时间
    Second,
    /// 以毫秒计算过期时间
    Millisecond,
}

/// 内存驱逐类型
#[derive(Debug)]
pub enum EvictType {
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
}

/// 代表Redis中的String类型数据
#[derive(Debug)]
pub struct KeyValue<'a> {
    /// 数据的key
    pub key: &'a [u8],
    /// 数据的值
    pub value: &'a [u8],
    /// 数据的元信息
    pub meta: &'a Meta,
}

/// 代表Redis中的List类型数据
#[derive(Debug)]
pub struct List<'a> {
    /// 数据的key
    pub key: &'a [u8],
    /// Set中所有的元素
    pub values: &'a [Vec<u8>],
    /// 数据的元信息
    pub meta: &'a Meta,
}

/// 代表Redis中的Set类型数据
#[derive(Debug)]
pub struct Set<'a> {
    /// 数据的key
    pub key: &'a [u8],
    /// Set中所有的元素
    pub members: &'a [Vec<u8>],
    /// 数据的元信息
    pub meta: &'a Meta,
}

/// 代表Redis中的SortedSet类型数据
#[derive(Debug)]
pub struct SortedSet<'a> {
    /// 数据的key
    pub key: &'a [u8],
    /// SortedSet中所有的元素
    pub items: &'a [Item],
    /// 数据的元信息
    pub meta: &'a Meta,
}

/// SortedSet中的一条元素
#[derive(Debug)]
pub struct Item {
    /// 元素值
    pub member: Vec<u8>,
    /// 元素的排序分数
    pub score: f64,
}

/// 代表Redis中的Hash类型数据
#[derive(Debug)]
pub struct Hash<'a> {
    /// 数据的key
    pub key: &'a [u8],
    /// 数据所有的字段
    pub fields: &'a [Field],
    /// 数据的元信息
    pub meta: &'a Meta,
}

/// Hash类型数据中的一个字段
#[derive(Debug)]
pub struct Field {
    /// 字段名
    pub name: Vec<u8>,
    /// 字段值
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct Stream<'a> {
    pub entries: BTreeMap<ID, Entry>,
    pub groups: Vec<Group>,
    /// 数据的元信息
    pub meta: &'a Meta,
}

#[derive(Debug, Eq, Copy, Clone)]
pub struct ID {
    pub ms: i64,
    pub seq: i64,
}

impl ID {
    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

impl PartialEq for ID {
    fn eq(&self, other: &Self) -> bool {
        self.ms == other.ms && self.seq == other.seq
    }

    fn ne(&self, other: &Self) -> bool {
        self.ms != other.ms || self.seq != other.seq
    }
}

impl PartialOrd for ID {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }

    fn lt(&self, other: &Self) -> bool {
        match self.cmp(other) {
            cmp::Ordering::Less => true,
            cmp::Ordering::Equal => false,
            cmp::Ordering::Greater => false,
        }
    }

    fn le(&self, other: &Self) -> bool {
        match self.cmp(other) {
            cmp::Ordering::Less => true,
            cmp::Ordering::Equal => true,
            cmp::Ordering::Greater => false,
        }
    }

    fn gt(&self, other: &Self) -> bool {
        match self.cmp(other) {
            cmp::Ordering::Less => false,
            cmp::Ordering::Equal => false,
            cmp::Ordering::Greater => true,
        }
    }

    fn ge(&self, other: &Self) -> bool {
        match self.cmp(other) {
            cmp::Ordering::Less => false,
            cmp::Ordering::Equal => true,
            cmp::Ordering::Greater => true,
        }
    }
}

impl Ord for ID {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let order = self.ms.cmp(&other.ms);
        if order == cmp::Ordering::Equal {
            self.seq.cmp(&other.seq)
        } else {
            order
        }
    }
}

#[derive(Debug)]
pub struct Entry {
    pub id: ID,
    pub deleted: bool,
    pub fields: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug)]
pub struct Group {
    pub name: Vec<u8>,
    pub last_id: ID,
}

/// Map object types to RDB object types.
///
pub(crate) const RDB_TYPE_STRING: u8 = 0;
pub(crate) const RDB_TYPE_LIST: u8 = 1;
pub(crate) const RDB_TYPE_SET: u8 = 2;
pub(crate) const RDB_TYPE_ZSET: u8 = 3;
pub(crate) const RDB_TYPE_HASH: u8 = 4;
/// ZSET version 2 with doubles stored in binary.
pub(crate) const RDB_TYPE_ZSET_2: u8 = 5;
pub(crate) const RDB_TYPE_MODULE: u8 = 6;
/// Module value with annotations for parsing without
/// the generating module being loaded.
pub(crate) const RDB_TYPE_MODULE_2: u8 = 7;

/// Object types for encoded objects.
///
pub(crate) const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub(crate) const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub(crate) const RDB_TYPE_SET_INTSET: u8 = 11;
pub(crate) const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub(crate) const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
pub(crate) const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub(crate) const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;

/// Special RDB opcodes
///
// Module auxiliary data.
pub(crate) const RDB_OPCODE_MODULE_AUX: u8 = 247;
// LRU idle time.
pub(crate) const RDB_OPCODE_IDLE: u8 = 248;
// LFU frequency.
pub(crate) const RDB_OPCODE_FREQ: u8 = 249;
// RDB aux field.
pub(crate) const RDB_OPCODE_AUX: u8 = 250;
// Hash table resize hint.
pub(crate) const RDB_OPCODE_RESIZEDB: u8 = 251;
// Expire time in milliseconds.
pub(crate) const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
// Old expire time in seconds.
pub(crate) const RDB_OPCODE_EXPIRETIME: u8 = 253;
// DB number of the following keys.
pub(crate) const RDB_OPCODE_SELECTDB: u8 = 254;
// End of the RDB file.
pub(crate) const RDB_OPCODE_EOF: u8 = 255;

pub(crate) const RDB_MODULE_OPCODE_EOF: isize = 0;

pub(crate) const RDB_MODULE_OPCODE_SINT: isize = 1;
pub(crate) const RDB_MODULE_OPCODE_UINT: isize = 2;
pub(crate) const RDB_MODULE_OPCODE_STRING: isize = 5;
pub(crate) const RDB_MODULE_OPCODE_FLOAT: isize = 3;
pub(crate) const RDB_MODULE_OPCODE_DOUBLE: isize = 4;

pub(crate) const ZIP_INT_8BIT: u8 = 254;
pub(crate) const ZIP_INT_16BIT: u8 = 192;
pub(crate) const ZIP_INT_24BIT: u8 = 240;
pub(crate) const ZIP_INT_32BIT: u8 = 208;
pub(crate) const ZIP_INT_64BIT: u8 = 224;

/// Defines related to the dump file format. To store 32 bits lengths for short
/// keys requires a lot of space, so we check the most significant 2 bits of
/// the first byte to interpreter the length:
///
/// 00|XXXXXX => if the two MSB are 00 the len is the 6 bits of this byte
/// 01|XXXXXX XXXXXXXX =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
/// 10|000000 [32 bit integer] => A full 32 bit len in net byte order will follow
/// 10|000001 [64 bit integer] => A full 64 bit len in net byte order will follow
/// 11|OBKIND this means: specially encoded object will follow. The six bits
///           number specify the kind of object that follows.
///           See the RDB_ENC_* defines.
///
/// Lengths up to 63 are stored using a single byte, most DB keys, and may
/// values, will fit inside.
pub(crate) const RDB_ENCVAL: u8 = 3;
pub(crate) const RDB_6BITLEN: u8 = 0;
pub(crate) const RDB_14BITLEN: u8 = 1;
pub(crate) const RDB_32BITLEN: u8 = 0x80;
pub(crate) const RDB_64BITLEN: u8 = 0x81;

/// When a length of a string object stored on disk has the first two bits
/// set, the remaining six bits specify a special encoding for the object
/// accordingly to the following defines:
///
/// 8 bit signed integer
pub(crate) const RDB_ENC_INT8: isize = 0;
/// 16 bit signed integer
pub(crate) const RDB_ENC_INT16: isize = 1;
/// 32 bit signed integer
pub(crate) const RDB_ENC_INT32: isize = 2;
/// string compressed with FASTLZ
pub(crate) const RDB_ENC_LZF: isize = 3;
pub(crate) const BATCH_SIZE: usize = 64;

pub(crate) const MODULE_SET: [char; 64] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
    'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
    'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_',
];
