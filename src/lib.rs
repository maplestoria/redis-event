use std::io::{Error, ErrorKind};

use crate::iter::Iter;

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
    fn handle(&self, key: &Vec<u8>, values: &mut dyn Iter, obj_type: u8);
}

// 定义redis命令的处理接口
pub trait CommandHandler {
    fn handle(&self, c: &Command);
}

pub struct EchoRdbHandler {}

impl RdbHandler for EchoRdbHandler {
    fn handle(&self, key: &Vec<u8>, values: &mut dyn Iter, obj_type: u8) {
        print!("[{:?}] {}: ", obj_type, String::from_utf8(key.to_vec()).unwrap());
        loop {
            match values.next() {
                Ok(val) => {
                    print!("{} ", String::from_utf8(val).unwrap());
                }
                Err(ref err) if err.kind() == ErrorKind::NotFound => break,
                Err(err) => {
                    println!("error: {}", err);
                    break;
                }
            }
        }
        println!();
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