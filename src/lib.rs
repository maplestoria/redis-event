/*!
用于监听Redis的写入操作，据此可以实现数据复制，监控等相关的应用。

# 参考
- [Redis Replication协议]

[Redis Replication协议]: https://redis.io/topics/replication
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

/// Redis事件监听器的定义，所有类型的监听器都实现此接口
pub trait RedisListener {
    /// 开启事件监听
    fn start(&mut self) -> Result<()>;
}

/// Redis事件
pub enum Event<'a> {
    /// RDB事件
    ///
    /// 当开启`RedisListener`之后，Redis会将此刻内存中的数据dump出来(以rdb的格式进行dump)，
    /// dump完毕之后的rdb数据便会发送给`RedisListener`，此rdb中的数据即对应此事件
    RDB(Object<'a>),
    /// AOF事件
    ///
    /// 在上面rdb数据处理完毕之后，客户端对Redis的数据写入操作将会发送给`RedisListener`，
    /// 此写入操作即对应此事件
    AOF(Command<'a>),
}

/// Redis事件处理器的定义，所有类型的处理器都必须实现此接口
pub trait EventHandler {
    fn handle(&mut self, event: Event);
}

/// No Operation处理器，对于接收到的事件，不做任何处理
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