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

// 定义redis rdb事件的处理接口
pub trait RdbEventHandler {
    fn handle(&self, e: &KeyValues);
}

// 定义redis命令的处理接口
pub trait CommandHandler {
    fn handle(&self, c: &Command);
}

pub struct EchoRdbHandler {}

impl RdbEventHandler for EchoRdbHandler {
    fn handle(&self, e: &KeyValues) {
        let key = e.key;
        print!("[{:?}] {}: ", e.data_type, String::from_utf8(key.to_vec()).unwrap());
        let val = e.values;
        for x in val {
            print!("{} ", String::from_utf8((x).to_vec()).unwrap());
        }
        println!();
    }
}

pub struct Command {}

pub struct KeyValues<'a> {
    key: &'a Vec<u8>,
    values: &'a Vec<Vec<u8>>,
    data_type: &'a DataType,
}

#[derive(Debug)]
pub enum DataType {
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Stream,
}
