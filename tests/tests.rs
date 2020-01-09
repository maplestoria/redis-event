use std::net::{IpAddr, SocketAddr};
use std::process::Command;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

use redis_event::{RdbHandler, RedisListener};
use redis_event::config::Config;
use redis_event::listener::standalone;
use redis_event::rdb::Object;

#[test]
fn test_hash_parser() {
    struct TestRdbHandler {}
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::Hash(hash) => {
                    let key = String::from_utf8_lossy(hash.key);
                    assert_eq!("force_dictionary", key);
                    for field in hash.fields {
                        assert_eq!(50, field.name.len());
                        assert_eq!(50, field.value.len());
                    }
                }
                _ => {}
            }
        }
    }
    
    let port: u16 = 16379;
    let pid = start_redis_server("dictionary.rdb", port);
    // wait redis to start
    sleep(Duration::from_secs(2));
    
    let ip = IpAddr::from_str("127.0.0.1").unwrap();
    let conf = Config {
        is_discard_rdb: false,
        is_aof: false,
        addr: SocketAddr::new(ip, port),
        password: String::new(),
        repl_id: String::from("?"),
        repl_offset: -1,
    };
    let rdb_handler = TestRdbHandler {};
    let mut redis_listener = standalone::new(conf);
    redis_listener.set_rdb_listener(Box::new(rdb_handler));
    if let Err(error) = redis_listener.open() {
        panic!(error)
    }
    shutdown_redis(pid);
}

fn start_redis_server(rdb: &str, port: u16) -> u32 {
    // redis-server --port 6379 --daemonize no --dbfilename rdb --dir ./tests/rdb
    let child = Command::new("redis-server")
        .arg("--port")
        .arg(port.to_string())
        .arg("--daemonize")
        .arg("no")
        .arg("--dbfilename")
        .arg(rdb)
        .arg("--dir")
        .arg("./tests/rdb")
        .spawn()
        .expect("failed to start redis-server");
    return child.id();
}

fn shutdown_redis(pid: u32) {
    let pid_str = format!("{}", pid);
    let output = Command::new("kill")
        .arg("-9")
        .arg(pid_str)
        .output()
        .expect("kill redis failed");
    println!("{:?}", output);
}