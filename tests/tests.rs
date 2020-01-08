use std::net::{IpAddr, SocketAddr};
use std::process::Command;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

use redis_event::RedisListener;
use redis_event::config::Config;
use redis_event::listener::standalone;

#[test]
fn test_parser() {
    let rdbs = ["dictionary.rdb"];
    for rdb in &rdbs {
        let port:u16 = 16379;
        let pid = start_redis_server(rdb, port);
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
        let mut redis_listener = standalone::new(conf);
        if let Err(error) = redis_listener.open() {
            panic!(error)
        }
        shutdown_redis(pid);
    }
}

fn start_redis_server(rdb: &str, port: u16) -> u32 {
    // redis-server --port 6379 --daemonize no --dbfilename rdb --dir ./tests/rdb
    let child = Command::new("/Users/zhongkx/opt/redis-5.0.6/src/redis-server")
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