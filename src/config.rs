/*!
定义[`RedisListener`]所需的各项配置信息

[`RedisListener`]: trait.RedisListener.html
*/
use std::net::SocketAddr;
use std::time::Duration;

/// 配置信息结构体定义
#[derive(Debug)]
pub struct Config {
    /// 是否跳过整个RDB不进行处理，直接进入AOF处理
    pub is_discard_rdb: bool,
    /// 是否需要处理AOF, 如为false, 处理完RDB后`RedisListener`将中止
    pub is_aof: bool,
    /// Redis的地址信息
    pub addr: SocketAddr,
    /// Redis的密码
    pub password: String,
    /// Replication ID
    pub repl_id: String,
    /// Replication Offset
    pub repl_offset: i64,
    /// Read Timeout
    pub read_timeout: Option<Duration>,
    /// Write Timeout
    pub write_timeout: Option<Duration>,
}
