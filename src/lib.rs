use std::io::Error;

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
        // 打印的格式不咋样, 将就看吧
        match data {
            Object::String(key, val) => {
                println!("{}={}", key, val);
            }
            Object::List(key, val) => {
                let values = val.join(", ");
                println!("{}=[ {} ]\r", key, values);
            }
            Object::Set(key, val) => {
                let values = val.join(", ");
                println!("{}=[ {} ]\r", key, values);
            }
            Object::SortedSet(key, val) => {
                print!("{}=[", key);
                for (element, score) in val.iter() {
                    print!("{}:{} ", element, score);
                }
                println!("]");
            }
            Object::Hash(key, val) => {
                print!("{}=[ ", key);
                for (field, value) in val.iter() {
                    print!("{}:{} ", field, value);
                }
                println!("]");
            }
        }
    }
}

pub struct Command {}