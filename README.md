# redis-event

[![Build Status](https://travis-ci.com/maplestoria/redis-event.svg?token=LAWtGewQmwi6dpqV9Qcy&branch=master)](https://travis-ci.com/maplestoria/redis-event)
[![codecov](https://codecov.io/gh/maplestoria/redis-event/branch/master/graph/badge.svg?token=u9ZqCQjuPi)](https://codecov.io/gh/maplestoria/redis-event)
[![Crates.io](https://img.shields.io/crates/v/redis-event)](https://crates.io/crates/redis-event)
[![Crates.io](https://img.shields.io/crates/l/redis-event)](LICENSE)

用于监听Redis的写入操作，据此可以实现数据复制，监控等相关的应用。

```
[dependencies]
redis-event = "1.1.0"
```

## 原理

此crate实现了[Redis Replication协议](https://redis.io/topics/replication)，在运行时，程序将以replica的身份连接到Redis，相当于Redis的一个副本。

所以，在程序连接上某个Redis之后，Redis会将它当前的所有数据以RDB的格式dump一份，dump完毕之后便发送过来，这个RDB中的每一条数据就对应一个`Event::RDB`事件。

在这之后，Redis接收到来自客户端的写入操作(即Redis命令)后，也会将这个写入操作传播给它的replica，每一个写入操作就对应一个`Event::AOF`事件。

## 示例

```rust
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::str::FromStr;
use std::rc::Rc;
use std::cell::RefCell;
use std::io;
use redis_event::listener;
use redis_event::config::Config;
use redis_event::{NoOpEventHandler, RedisListener};

fn main() -> io::Result<()> {
    let ip = IpAddr::from_str("127.0.0.1").unwrap();
    let port = 6379;
    
    let conf = Config {
        is_discard_rdb: false,            // 不跳过RDB
        is_aof: false,                    // 不处理AOF
        addr: SocketAddr::new(ip, port),
        password: String::new(),          // 密码为空
        repl_id: String::from("?"),       // replication id，若无此id，设置为?即可
        repl_offset: -1,                  // replication offset，若无此offset，设置为-1即可
        read_timeout: None,               // None，即读取永不超时
        write_timeout: None,              // None，即写入永不超时
    };
    let running = Arc::new(AtomicBool::new(true));

    let mut builder = listener::Builder::new();
    builder.with_config(conf);
    // 设置控制变量, 通过此变量在外界中断`redis_event`内部的逻辑
    builder.with_control_flag(running);
    // 设置事件处理器
    builder.with_event_handler(Rc::new(RefCell::new(NoOpEventHandler{})));

    let mut redis_listener = builder.build();
    // 启动程序
    redis_listener.start()?;
    Ok(())
}
```