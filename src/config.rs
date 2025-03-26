/*!
定义[`RedisListener`]所需的各项配置信息

[`RedisListener`]: trait.RedisListener.html
*/
use std::time::Duration;

/// 配置信息结构体定义
#[derive(Debug)]
pub struct Config {
    /// 是否跳过整个RDB不进行处理，直接进入AOF处理
    pub is_discard_rdb: bool,
    /// 是否需要处理AOF, 如为false, 处理完RDB后`RedisListener`将中止
    pub is_aof: bool,
    /// Redis的地址
    pub host: String,
    /// Redis的端口
    pub port: u16,
    /// Redis的用户名
    pub username: String,
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
    #[cfg(feature = "tls")]
    /// 是否启用TLS
    pub is_tls_enabled: bool,
    #[cfg(feature = "tls")]
    /// 是否信任无效的证书和域名
    pub is_tls_insecure: bool,
    #[cfg(feature = "tls")]
    /// 客户端认证所使用的Key
    pub identity: Option<String>,
    #[cfg(feature = "tls")]
    /// 解密Key所需的密码
    pub identity_passwd: Option<String>,
}

impl Clone for Config {
    fn clone(&self) -> Self {
        Config {
            is_discard_rdb: self.is_discard_rdb,
            is_aof: self.is_aof,
            host: self.host.clone(),
            port: self.port.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            repl_id: self.repl_id.clone(),
            repl_offset: self.repl_offset,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
            #[cfg(feature = "tls")]
            is_tls_enabled: self.is_tls_enabled,
            #[cfg(feature = "tls")]
            is_tls_insecure: self.is_tls_insecure,
            #[cfg(feature = "tls")]
            identity: self.identity.clone(),
            #[cfg(feature = "tls")]
            identity_passwd: self.identity_passwd.clone(),
        }
    }
}
