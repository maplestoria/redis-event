/*!
This crate provides redis event listener
*/

use std::io::Result;

use crate::cmd::Command;
use crate::rdb::Object;

pub mod cmd;
pub mod config;
pub mod listener;
mod iter;
mod lzf;
pub mod rdb;
mod io;
mod tests;

/// RedisListener
pub trait RedisListener {
    /// 开启监听
    fn open(&mut self) -> Result<()>;
}

pub enum Event<'a> {
    RDB(Object<'a>),
    AOF(Command<'a>),
}

pub trait EventHandler {
    fn handle(&mut self, event: Event);
}

pub struct NoOpEventHandler {}

impl EventHandler for NoOpEventHandler {
    fn handle(&mut self, _: Event) {}
}

/// 转换为utf-8字符串，不验证正确性
fn to_string(bytes: Vec<u8>) -> String {
    return unsafe {
        String::from_utf8_unchecked(bytes)
    };
}