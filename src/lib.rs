use std::io::Error;

mod config;
mod listener;
mod lzf;
mod rdb;
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
    // 关闭监听
    fn close(&self);
}

// 代表一个redis事件
pub trait Event {}

// 定义redis事件的处理接口
pub trait EventHandler {
    fn handle(&mut self, e: &dyn Event);
}