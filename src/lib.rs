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

/// 定义redis监听者接口
///
/// 具有以下三种监听模式:
///- 单节点(standalone)
/// - 集群(cluster)
/// - 哨兵(sentinel)
pub trait RedisListener {
    /// 开启监听
    fn open(&mut self) -> Result<()>;
}

/// 定义redis rdb事件的处理接口
pub trait RdbHandler {
    fn handle(&mut self, data: Object);
}

pub struct NoOpRdbHandler {}

impl RdbHandler for NoOpRdbHandler {
    fn handle(&mut self, _: Object) {}
}

/// 定义redis命令的处理接口
pub trait CommandHandler {
    fn handle(&mut self, cmd: Command);
}

pub struct NoOpCommandHandler {}

impl CommandHandler for NoOpCommandHandler {
    fn handle(&mut self, _: Command) {}
}

/// 转换为utf-8字符串，不验证正确性
fn to_string(bytes: Vec<u8>) -> String {
    return unsafe {
        String::from_utf8_unchecked(bytes)
    };
}