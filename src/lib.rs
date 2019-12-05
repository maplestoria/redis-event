use std::io::{Error, ErrorKind};

use crate::iter::Iter;
use crate::rdb::Object;

mod config;
pub mod listener;
pub mod iter;
mod lzf;
mod rdb;
mod reader;
mod tests;

// 定义redis监听者接口
//
// 具有以下三种监听模式:
// - 单节点(standalone)
// - 集群(cluster)
// - 哨兵(sentinel)
pub trait RedisListener {
    // 开启监听
    fn open(&mut self) -> Result<(), Error>;
}

// 定义redis rdb事件的处理接口
pub trait RdbHandler {
    fn handle(&self, data: &Object);
}

// 定义redis命令的处理接口
pub trait CommandHandler {
    fn handle(&self, c: &Command);
}

pub struct EchoRdbHandler {}

impl RdbHandler for EchoRdbHandler {
    fn handle(&self, data: &Object) {
        match data {
            Object::String(key, val) => {
                println!("{}={}", key, val);
            }
            Object::List(key, val) => {
                print!("{}=[ ", key);
                for x in val.iter() {
                    print!("{} ", String::from_utf8(x.clone()).unwrap());
                }
                print!("]\r\n");
            }
            Object::Set(key, val) => {
                print!("{}=[ ", key);
                for x in val.iter() {
                    print!("{} ", String::from_utf8(x.clone()).unwrap());
                }
                print!("]\r\n");
            }
            Object::SortedSet(key, val) => {
                print!("{}=[ ", key);
                let mut iter = val.iter();
    
                loop {
                    let ele;
                    let sc;
                    if let Some(element) = iter.next() {
                        ele = String::from_utf8(element.clone()).unwrap();
                    } else {
                        break;
                    }
                    if let Some(score) = iter.next() {
                        sc = String::from_utf8(score.clone()).unwrap();
                    } else {
                        panic!("lack score of element")
                    }
                    print!(" {}-{} ", ele, sc);
                }
                
                print!("]\r\n");
            }
            Object::Hash(key, val) => {
                print!("{}=[ ", key);
                let mut iter = val.iter();
    
                loop {
                    let field;
                    let val;
                    if let Some(element) = iter.next() {
                        field = String::from_utf8(element.clone()).unwrap();
                    } else {
                        break;
                    }
                    if let Some(element) = iter.next() {
                        val = String::from_utf8(element.clone()).unwrap();
                    } else {
                        panic!("lack val of field")
                    }
                    print!(" {}={} ", field, val);
                }
                print!("]\r\n");
            }
        }
    }
}

pub struct Command {}

/// Data types
pub const OBJ_STRING: u8 = 0;    /* String object. */
pub const OBJ_LIST: u8 = 1;      /* List object. */
pub const OBJ_SET: u8 = 2;       /* Set object. */
pub const OBJ_ZSET: u8 = 3;      /* Sorted set object. */
pub const OBJ_HASH: u8 = 4;      /* Hash object. */
pub const OBJ_MODULE: u8 = 5;    /* Module object. */
pub const OBJ_STREAM: u8 = 6;    /* Stream object. */