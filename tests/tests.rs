use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use redis_event::config::Config;
use redis_event::listener::standalone;
use redis_event::RedisListener;

#[test]
fn open() {
    let ip = IpAddr::from_str("127.0.0.1").unwrap();
    let conf = Config {
        is_discard_rdb: false,
        is_aof: false,
        addr: SocketAddr::new(ip, 6379),
        password: String::from("123456"),
        repl_id: String::from("?"),
        repl_offset: -1,
    };
    
    let mut redis_listener = standalone::new(conf);
    if let Err(error) = redis_listener.open() {
        panic!("couldn't connect to server: {}", error)
    }
}
