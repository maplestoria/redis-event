use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::process::Command;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

use serial_test::serial;

use redis_event::{CommandHandler, NoOpCommandHandler, RdbHandler, RedisListener};
use redis_event::config::Config;
use redis_event::listener::standalone;
use redis_event::rdb::Object;

#[test]
#[serial]
fn test_hash() {
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
    
    start_redis_test("dictionary.rdb", Box::new(TestRdbHandler {}), Box::new(NoOpCommandHandler {}));
}

#[test]
#[serial]
fn test_hash_1() {
    struct TestRdbHandler {}
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::Hash(hash) => {
                    let key = String::from_utf8_lossy(hash.key);
                    assert_eq!("zipmap_compresses_easily", key);
                    let mut map = HashMap::new();
                    for field in hash.fields {
                        let name = String::from_utf8_lossy(&field.name);
                        let value = String::from_utf8_lossy(&field.value);
                        map.insert(name, value);
                    }
                    assert_eq!("aa", map.get("a").unwrap());
                    assert_eq!("aaaa", map.get("aa").unwrap());
                    assert_eq!("aaaaaaaaaaaaaa", map.get("aaaaa").unwrap());
                }
                _ => {}
            }
        }
    }
    
    start_redis_test("hash_as_ziplist.rdb", Box::new(TestRdbHandler {}), Box::new(NoOpCommandHandler {}));
}

#[test]
#[serial]
fn test_string() {
    struct TestRdbHandler {}
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::String(kv) => {
                    let key = String::from_utf8_lossy(kv.key);
                    assert_eq!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", key);
                    assert_eq!("Key that redis should compress easily", String::from_utf8_lossy(kv.value))
                }
                _ => {}
            }
        }
    }
    
    start_redis_test("easily_compressible_string_key.rdb", Box::new(TestRdbHandler {}), Box::new(NoOpCommandHandler {}));
}

#[test]
#[serial]
fn test_integer() {
    struct TestRdbHandler {
        map: HashMap<String, String>
    }
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::String(kv) => {
                    self.map.insert(String::from_utf8_lossy(kv.key).to_string(),
                                    String::from_utf8_lossy(kv.value).to_string());
                }
                Object::EOR => {
                    assert_eq!(self.map.get("125").unwrap(), "Positive 8 bit integer");
                    assert_eq!(self.map.get("43947").unwrap(), "Positive 16 bit integer");
                    assert_eq!(self.map.get("183358245").unwrap(), "Positive 32 bit integer");
                    assert_eq!(self.map.get("-123").unwrap(), "Negative 8 bit integer");
                    assert_eq!(self.map.get("-29477").unwrap(), "Negative 16 bit integer");
                    assert_eq!(self.map.get("-183358245").unwrap(), "Negative 32 bit integer");
                }
                _ => {}
            }
        }
    }
    
    start_redis_test("integer_keys.rdb", Box::new(TestRdbHandler { map: HashMap::new() }), Box::new(NoOpCommandHandler {}));
}

#[test]
#[serial]
fn test_intset16() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<String>>
    }
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::Set(set) => {
                    let key = String::from_utf8_lossy(set.key).to_string();
                    let mut val = Vec::new();
                    for mem in set.members {
                        val.push(String::from_utf8_lossy(mem).to_string());
                    }
                    self.map.insert(key, val);
                }
                Object::EOR => {
                    let values = self.map.get("intset_16").unwrap();
                    let arr = ["32766", "32765", "32764"];
                    for val in values {
                        assert!(arr.contains(&val.as_str()));
                    }
                }
                _ => {}
            }
        }
    }
    start_redis_test("intset_16.rdb", Box::new(TestRdbHandler { map: HashMap::new() }), Box::new(NoOpCommandHandler {}));
}

#[test]
#[serial]
fn test_intset32() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<String>>
    }
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::Set(set) => {
                    let key = String::from_utf8_lossy(set.key).to_string();
                    let mut val = Vec::new();
                    for mem in set.members {
                        val.push(String::from_utf8_lossy(mem).to_string());
                    }
                    self.map.insert(key, val);
                }
                Object::EOR => {
                    let values = self.map.get("intset_32").unwrap();
                    let arr = ["2147418110", "2147418109", "2147418108"];
                    for val in values {
                        assert!(arr.contains(&val.as_str()));
                    }
                }
                _ => {}
            }
        }
    }
    start_redis_test("intset_32.rdb", Box::new(TestRdbHandler { map: HashMap::new() }), Box::new(NoOpCommandHandler {}));
}

#[test]
#[serial]
fn test_intset64() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<String>>
    }
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::Set(set) => {
                    let key = String::from_utf8_lossy(set.key).to_string();
                    let mut val = Vec::new();
                    for mem in set.members {
                        val.push(String::from_utf8_lossy(mem).to_string());
                    }
                    self.map.insert(key, val);
                }
                Object::EOR => {
                    let values = self.map.get("intset_64").unwrap();
                    let arr = ["9223090557583032318", "9223090557583032317", "9223090557583032316"];
                    for val in values {
                        assert!(arr.contains(&val.as_str()));
                    }
                }
                _ => {}
            }
        }
    }
    start_redis_test("intset_64.rdb", Box::new(TestRdbHandler { map: HashMap::new() }), Box::new(NoOpCommandHandler {}));
}

#[test]
#[serial]
fn test_keys_with_expiry() {
    struct TestRdbHandler {}
    
    impl RdbHandler for TestRdbHandler {
        fn handle(&mut self, data: Object) {
            match data {
                Object::String(kv) => {
                    let key = String::from_utf8_lossy(kv.key).to_string();
                    let val = String::from_utf8_lossy(kv.value).to_string();
                    assert_eq!("expires_ms_precision", key);
                    assert_eq!("2022-12-25 10:11:12.573 UTC", val);
                }
                _ => {}
            }
        }
    }
    start_redis_test("keys_with_expiry.rdb", Box::new(TestRdbHandler {}), Box::new(NoOpCommandHandler {}));
}

fn start_redis_test(rdb: &str, rdb_handler: Box<dyn RdbHandler>, cmd_handler: Box<dyn CommandHandler>) {
    let port: u16 = 16379;
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
    redis_listener.set_rdb_listener(rdb_handler);
    redis_listener.set_command_listener(cmd_handler);
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