use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::ops::{Deref, DerefMut};
use std::process::Command;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use redis;
use redis::Commands;
use redis::ConnectionAddr;
use serial_test::serial;

use redis_event::config::Config;
use redis_event::{listener, NoOpEventHandler};
use redis_event::rdb::{ExpireType, Object};
use redis_event::{cmd, Event, EventHandler, RedisListener};
use crate::support::*;

mod support;

#[test]
#[serial]
fn test_hash() {
    struct TestRdbHandler {}

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::Hash(hash) => {
                        let key = String::from_utf8_lossy(hash.key);
                        assert_eq!("force_dictionary", key);
                        for field in hash.fields {
                            assert_eq!(50, field.name.len());
                            assert_eq!(50, field.value.len());
                        }
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }

    start_redis_test(
        "dictionary.rdb",
        10000,
        Rc::new(RefCell::new(TestRdbHandler {})),
    );
}

#[test]
#[serial]
fn test_hash_1() {
    struct TestRdbHandler {}

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
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
                },
                Event::AOF(_) => {}
            }
        }
    }

    start_redis_test(
        "hash_as_ziplist.rdb",
        10001,
        Rc::new(RefCell::new(TestRdbHandler {})),
    );
}

#[test]
#[serial]
fn test_string() {
    struct TestRdbHandler {}

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::String(kv) => {
                        let key = String::from_utf8_lossy(kv.key);
                        assert_eq!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", key);
                        assert_eq!(
                            "Key that redis should compress easily",
                            String::from_utf8_lossy(kv.value)
                        )
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }

    start_redis_test(
        "easily_compressible_string_key.rdb",
        10002,
        Rc::new(RefCell::new(TestRdbHandler {})),
    );
}

#[test]
#[serial]
fn test_integer() {
    struct TestRdbHandler {
        map: HashMap<String, String>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::String(kv) => {
                        self.map.insert(
                            String::from_utf8_lossy(kv.key).to_string(),
                            String::from_utf8_lossy(kv.value).to_string(),
                        );
                    }
                    Object::EOR => {
                        assert_eq!(self.map.get("125").unwrap(), "Positive 8 bit integer");
                        assert_eq!(self.map.get("43947").unwrap(), "Positive 16 bit integer");
                        assert_eq!(
                            self.map.get("183358245").unwrap(),
                            "Positive 32 bit integer"
                        );
                        assert_eq!(self.map.get("-123").unwrap(), "Negative 8 bit integer");
                        assert_eq!(self.map.get("-29477").unwrap(), "Negative 16 bit integer");
                        assert_eq!(
                            self.map.get("-183358245").unwrap(),
                            "Negative 32 bit integer"
                        );
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }

    start_redis_test(
        "integer_keys.rdb",
        10003,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_intset16() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<String>>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
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
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "intset_16.rdb",
        10004,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_intset32() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<String>>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
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
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "intset_32.rdb",
        10005,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_intset64() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<String>>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
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
                        let arr = [
                            "9223090557583032318",
                            "9223090557583032317",
                            "9223090557583032316",
                        ];
                        for val in values {
                            assert!(arr.contains(&val.as_str()));
                        }
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "intset_64.rdb",
        10006,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_keys_with_expiry() {
    struct TestRdbHandler {}

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::String(kv) => {
                        let key = String::from_utf8_lossy(kv.key).to_string();
                        let val = String::from_utf8_lossy(kv.value).to_string();
                        assert_eq!("expires_ms_precision", key);
                        assert_eq!("2022-12-25 10:11:12.573 UTC", val);
                        if let Some((ExpireType::Millisecond, val)) = kv.meta.expire {
                            assert_eq!(1671963072573, val);
                        } else {
                            panic!("错误的过期类型")
                        }
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "keys_with_expiry.rdb",
        10007,
        Rc::new(RefCell::new(TestRdbHandler {})),
    );
}

#[test]
#[serial]
fn test_linked_list() {
    struct TestRdbHandler {
        list: Vec<String>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::List(list) => {
                        assert_eq!("force_linkedlist", String::from_utf8_lossy(list.key));
                        for val in list.values {
                            let value = String::from_utf8_lossy(val).to_string();
                            self.list.push(value);
                        }
                    }
                    Object::EOR => {
                        assert_eq!(1000, self.list.len());
                        assert_eq!(
                            "41PJSO2KRV6SK1WJ6936L06YQDPV68R5J2TAZO3YAR5IL5GUI8",
                            self.list.get(0).unwrap()
                        );
                        assert_eq!(
                            "E41JRQX2DB4P1AQZI86BAT7NHPBHPRIIHQKA4UXG94ELZZ7P3Y",
                            self.list.get(1).unwrap()
                        );
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "linkedlist.rdb",
        10008,
        Rc::new(RefCell::new(TestRdbHandler { list: vec![] })),
    );
}

#[test]
#[serial]
fn test_multiple_database() {
    struct TestRdbHandler {}

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::String(kv) => {
                        let key = String::from_utf8_lossy(kv.key);
                        if "key_in_zeroth_database".eq(&key) {
                            assert_eq!(0, kv.meta.db);
                            assert_eq!("zero", String::from_utf8_lossy(kv.value))
                        } else if "key_in_second_database".eq(&key) {
                            assert_eq!(2, kv.meta.db);
                            assert_eq!("second", String::from_utf8_lossy(kv.value))
                        } else {
                            panic!("key名错误")
                        }
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "multiple_databases.rdb",
        10009,
        Rc::new(RefCell::new(TestRdbHandler {})),
    );
}

#[test]
#[serial]
fn test_regular_set() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<String>>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::Set(set) => {
                        let key = String::from_utf8_lossy(set.key).to_string();
                        let mut val = Vec::new();
                        for mem in set.members {
                            val.push(String::from_utf8_lossy(mem).to_string());
                        }
                        self.map.insert(key, val);
                    }
                    Object::EOR => {
                        let values = self.map.get("regular_set").unwrap();
                        let arr = ["alpha", "beta", "gamma", "delta", "phi", "kappa"];
                        for val in values {
                            assert!(arr.contains(&val.as_str()));
                        }
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "regular_set.rdb",
        10010,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_regular_sorted_set() {
    struct TestRdbHandler {
        map: HashMap<String, f64>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::SortedSet(set) => {
                        let key = String::from_utf8_lossy(set.key).to_string();
                        assert_eq!("force_sorted_set", key);
                        for item in set.items {
                            self.map.insert(
                                String::from_utf8_lossy(&item.member).to_string(),
                                item.score,
                            );
                        }
                    }
                    Object::EOR => {
                        assert_eq!(500, self.map.len());
                        assert_eq!(
                            3.19,
                            self.map
                                .get("G72TWVWH0DY782VG0H8VVAR8RNO7BS9QGOHTZFJU67X7L0Z3PR")
                                .unwrap()
                                .clone()
                        );
                        assert_eq!(
                            0.76,
                            self.map
                                .get("N8HKPIK4RC4I2CXVV90LQCWODW1DZYD0DA26R8V5QP7UR511M8")
                                .unwrap()
                                .clone()
                        );
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "regular_sorted_set.rdb",
        10011,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_zipmap_big_values() {
    struct TestRdbHandler {
        map: HashMap<String, Vec<u8>>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::Hash(hash) => {
                        assert_eq!("zipmap_with_big_values", String::from_utf8_lossy(hash.key));
                        for field in hash.fields {
                            let name = String::from_utf8_lossy(&field.name).to_string();
                            self.map.insert(name, field.value.to_vec());
                        }
                    }
                    Object::EOR => {
                        assert_eq!(253, self.map.get("253bytes").unwrap().len());
                        assert_eq!(254, self.map.get("254bytes").unwrap().len());
                        assert_eq!(255, self.map.get("255bytes").unwrap().len());
                        assert_eq!(300, self.map.get("300bytes").unwrap().len());
                        assert_eq!(20000, self.map.get("20kbytes").unwrap().len());
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "zipmap_with_big_values.rdb",
        10012,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_zipmap_compress() {
    struct TestRdbHandler {
        map: HashMap<String, String>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::Hash(hash) => {
                        assert_eq!(
                            "zipmap_compresses_easily",
                            String::from_utf8_lossy(hash.key)
                        );
                        for field in hash.fields {
                            let name = String::from_utf8_lossy(&field.name).to_string();
                            let val = String::from_utf8_lossy(&field.value).to_string();
                            self.map.insert(name, val);
                        }
                    }
                    Object::EOR => {
                        assert_eq!("aa", self.map.get("a").unwrap());
                        assert_eq!("aaaa", self.map.get("aa").unwrap());
                        assert_eq!("aaaaaaaaaaaaaa", self.map.get("aaaaa").unwrap());
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "zipmap_that_compresses_easily.rdb",
        10013,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_zipmap_not_compress() {
    struct TestRdbHandler {
        map: HashMap<String, String>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::Hash(hash) => {
                        assert_eq!("zimap_doesnt_compress", String::from_utf8_lossy(hash.key));
                        for field in hash.fields {
                            let name = String::from_utf8_lossy(&field.name).to_string();
                            let val = String::from_utf8_lossy(&field.value).to_string();
                            self.map.insert(name, val);
                        }
                    }
                    Object::EOR => {
                        assert_eq!("2", self.map.get("MKD1G6").unwrap());
                        assert_eq!("F7TI", self.map.get("YNNXK").unwrap());
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "zipmap_that_doesnt_compress.rdb",
        10014,
        Rc::new(RefCell::new(TestRdbHandler {
            map: HashMap::new(),
        })),
    );
}

#[test]
#[serial]
fn test_ziplist() {
    struct TestRdbHandler {
        list: Vec<String>,
    }

    impl EventHandler for TestRdbHandler {
        fn handle(&mut self, data: Event) {
            match data {
                Event::RDB(rdb) => match rdb {
                    Object::List(list) => {
                        assert_eq!(
                            "ziplist_compresses_easily",
                            String::from_utf8_lossy(list.key)
                        );
                        for val in list.values {
                            let value = String::from_utf8_lossy(val).to_string();
                            self.list.push(value);
                        }
                    }
                    Object::EOR => {
                        assert_eq!(6, self.list.len());
                        assert_eq!("aaaaaa", self.list.get(0).unwrap());
                        assert_eq!("aaaaaaaaaaaa", self.list.get(1).unwrap());
                        assert_eq!("aaaaaaaaaaaaaaaaaa", self.list.get(2).unwrap());
                        assert_eq!("aaaaaaaaaaaaaaaaaaaaaaaa", self.list.get(3).unwrap());
                        assert_eq!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", self.list.get(4).unwrap());
                        assert_eq!(
                            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            self.list.get(5).unwrap()
                        );
                    }
                    _ => {}
                },
                Event::AOF(_) => {}
            }
        }
    }
    start_redis_test(
        "ziplist_that_compresses_easily.rdb",
        10015,
        Rc::new(RefCell::new(TestRdbHandler { list: vec![] })),
    );
}

#[test]
#[serial]
fn test_aof() {
    let port = 10016;

    let pid = Command::new("redis-server")
        .arg("--port")
        .arg(port.to_string())
        .arg("--requirepass")
        .arg("123456")
        .arg("--daemonize")
        .arg("no")
        .arg("--loglevel")
        .arg("warning")
        .arg("--logfile")
        .arg(port.to_string())
        .spawn()
        .expect("failed to start redis-server")
        .id();

    // wait redis to start
    sleep(Duration::from_secs(2));

    struct TestCmdHandler {
        pid: u32,
        count: Arc<Mutex<i32>>,
    }

    impl EventHandler for TestCmdHandler {
        fn handle(&mut self, cmd: Event) {
            match cmd {
                Event::RDB(_) => {}
                Event::AOF(cmd) => {
                    println!("{:?}", cmd);
                    if let Ok(mut count) = self.count.lock() {
                        let c = count.borrow_mut();
                        let c = c.deref_mut();
                        *c += 1;
                    }
                    match cmd {
                        cmd::Command::FLUSHDB(flushdb) => {
                            assert_eq!(true, flushdb._async.expect("no async field"));
                        }
                        cmd::Command::FLUSHALL(_) => {
                            shutdown_redis(self.pid);
                        }
                        cmd::Command::EXPIRE(expire) => {
                            assert_eq!(b"aa", expire.key);
                            assert_eq!(b"1", expire.seconds);
                        }
                        cmd::Command::LINSERT(linsert) => {
                            assert_eq!(b"list", linsert.key);
                            if let cmd::lists::POSITION::AFTER = linsert.position {
                                panic!("wrong position");
                            }
                        }
                        cmd::Command::RPOPLPUSH(rpoplpush) => {
                            assert_eq!(b"list", rpoplpush.source);
                            assert_eq!(b"destlist", rpoplpush.destination);
                        }
                        cmd::Command::RPUSH(rpush) => {
                            assert_eq!(b"list", rpush.key);
                            assert_eq!(1, rpush.elements.len());
                            assert_eq!(b"hello", rpush.elements.get(0).unwrap());
                        }
                        cmd::Command::SELECT(select) => {
                            assert_eq!(0, select.db);
                        }
                        cmd::Command::SET(set) => {
                            assert_eq!(b"aa", set.key);
                            assert_eq!(b"bb", set.value);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let cmd_count = Arc::new(Mutex::new(0));

    let rc = cmd_count.clone();

    let t = thread::spawn(move || {
        let cmd_handler = TestCmdHandler { pid, count: rc };

        let ip = String::from("127.0.0.1");
        let conf = Config {
            is_discard_rdb: false,
            is_aof: true,
            host: ip,
            port,
            password: String::from("123456"),
            repl_id: String::from("?"),
            repl_offset: -1,
            read_timeout: None,
            write_timeout: None,
            is_tls_enabled: false,
            is_tls_insecure: false,
            identity: None,
            username: "".to_string(),
            identity_passwd: None,
        };
        let running = Arc::new(AtomicBool::new(true));

        let mut builder = listener::Builder::new();
        builder.with_config(conf);
        builder.with_control_flag(running);
        builder.with_event_handler(Rc::new(RefCell::new(cmd_handler)));

        let mut redis_listener = builder.build();

        if let Err(_) = redis_listener.start() {
            println!("redis-server closed");
        }
    });
    // wait thread start
    thread::sleep(Duration::from_secs(2));

    let uri = format!("redis://:123456@127.0.0.1:{}", port);

    if let Ok(client) = redis::Client::open(uri.as_str()) {
        if let Ok(mut conn) = client.get_connection() {
            let _: () = conn.set("aa", "bb").unwrap();
            let _: () = conn.expire("aa", 1).unwrap();
            let _: () = redis::cmd("SET")
                .arg("aa")
                .arg("bb")
                .arg("EX")
                .arg("100")
                .arg("XX")
                .query(&mut conn)
                .unwrap();
            let _: () = conn.rpush("list", "hello").unwrap();
            let _: () = redis::cmd("LINSERT")
                .arg("list")
                .arg("BEFORE")
                .arg("hello")
                .arg("world")
                .query(&mut conn)
                .unwrap();
            let _: () = redis::cmd("SORT")
                .arg(&[
                    "list",
                    "ALPHA",
                    "ASC",
                    "LIMIT",
                    "0",
                    "10",
                    "BY",
                    "weight_*",
                    "GET",
                    "nosort",
                    "STORE",
                    "storelidst",
                ])
                .query(&mut conn)
                .unwrap();
            let _: () = conn.rpoplpush("list", "destlist").unwrap();
            let _: () = conn.rename_nx("destlist", "destlist2").unwrap();
            let _: () = conn.pexpire("destlist2", 1000).unwrap();
            let _: () = conn.expire_at("list", 1000).unwrap();
            // flush all, end the test
            let _: () = redis::cmd("FLUSHDB").arg("ASYNC").query(&mut conn).unwrap();
            let _: () = redis::cmd("FLUSHALL")
                .arg("ASYNC")
                .query(&mut conn)
                .unwrap();
            t.join().expect("thread error");
        } else {
            shutdown_redis(pid);
        }
    }

    assert_eq!(13, *cmd_count.lock().unwrap().deref());
}

#[test]
#[serial]
fn test_tls() {
    env::set_var("REDISRS_SERVER_TYPE", "tcp+tls");
    let mut context = TestContext::new();
    let addr = context.server.get_client_addr();
    let (host, port) = match addr {
        ConnectionAddr::TcpTls { ref host, port, .. } => {
            (host, port)
        },
        _ => {panic!("wrong mode")}
    };
    println!("redis-server: {}:{}", host, port);
    let conf = Config {
        is_discard_rdb: true,
        is_aof: false,
        host: host.to_string(),
        port: *port,
        password: String::new(),
        repl_id: String::from("?"),
        repl_offset: -1,
        read_timeout: None,
        write_timeout: None,
        is_tls_enabled: true,
        is_tls_insecure: true,
        identity: None,
        username: "".to_string(),
        identity_passwd: None,
    };
    let running = Arc::new(AtomicBool::new(true));
    
    let mut builder = listener::Builder::new();
    builder.with_config(conf);
    builder.with_control_flag(running);
    builder.with_event_handler(Rc::new(RefCell::new(NoOpEventHandler {})));
    
    let mut redis_listener = builder.build();
    println!("connect to redis-server");
    if let Err(err) = redis_listener.start() {
        println!("error: {}", err);
        panic!(err);
    }
    println!("done");
    context.stop_server();
}

fn start_redis_test(rdb: &str, port: u16, rdb_handler: Rc<RefCell<dyn EventHandler>>) {
    let pid = start_redis_server(rdb, port);
    // wait redis to start
    sleep(Duration::from_secs(2));

    let ip = String::from("127.0.0.1");
    let conf = Config {
        is_discard_rdb: false,
        is_aof: false,
        host: ip,
        port: port,
        username: "".to_string(),
        password: String::new(),
        repl_id: String::from("?"),
        repl_offset: -1,
        read_timeout: None,
        write_timeout: None,
        is_tls_enabled: false,
        is_tls_insecure: false,
        identity: None,
        identity_passwd: None,
    };
    let running = Arc::new(AtomicBool::new(true));

    let mut builder = listener::Builder::new();
    builder.with_config(conf);
    builder.with_control_flag(running);
    builder.with_event_handler(rdb_handler);

    let mut redis_listener = builder.build();

    if let Err(error) = redis_listener.start() {
        eprintln!("error: {}", error);
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
        .arg("--loglevel")
        .arg("warning")
        .arg("--logfile")
        .arg(port.to_string())
        .spawn()
        .expect("failed to start redis-server");
    return child.id();
}

fn shutdown_redis(pid: u32) {
    Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .status()
        .expect("kill redis failed");
}
