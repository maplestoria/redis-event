#[cfg(test)]
mod rdb_tests {
    use std::any::Any;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Read;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use num_bigint::Sign;
    use num_traits::ToPrimitive;

    use crate::rdb::{DefaultRDBParser, EvictType, ExpireType, ID, Module, Object, RDBDecode};
    use crate::{Event, EventHandler, ModuleParser, RDBParser};

    #[test]
    fn test_zipmap_not_compress() {
        let mut file = File::open("tests/rdb/zipmap_that_doesnt_compress_1.rdb").expect("file not found");

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
                            assert_eq!("2", self.map.get("MKD1G6").expect("field not found"));
                            assert_eq!("F7TI", self.map.get("YNNXK").expect("field not found"));
                        }
                        _ => {}
                    },
                    Event::AOF(_) => {}
                }
            }
        }
        let mut handler = TestRdbHandler { map: HashMap::new() };

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_zipmap_compress() {
        let mut file = File::open("tests/rdb/zipmap_that_compresses_easily_1.rdb").expect("file not found");

        struct TestRdbHandler {
            map: HashMap<String, String>,
        }

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, data: Event) {
                match data {
                    Event::RDB(rdb) => match rdb {
                        Object::Hash(hash) => {
                            assert_eq!("zipmap_compresses_easily", String::from_utf8_lossy(hash.key));
                            for field in hash.fields {
                                let name = String::from_utf8_lossy(&field.name).to_string();
                                let val = String::from_utf8_lossy(&field.value).to_string();
                                self.map.insert(name, val);
                            }
                        }
                        Object::EOR => {
                            assert_eq!("aa", self.map.get("a").expect("field not found"));
                            assert_eq!("aaaa", self.map.get("aa").expect("field not found"));
                            assert_eq!("aaaaaaaaaaaaaa", self.map.get("aaaaa").expect("field not found"));
                        }
                        _ => {}
                    },
                    Event::AOF(_) => {}
                }
            }
        }

        let mut handler = TestRdbHandler { map: HashMap::new() };

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_regular_sorted_set() {
        let mut file = File::open("tests/rdb/regular_sorted_set_1.rdb").expect("file not found");

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
                                self.map
                                    .insert(String::from_utf8_lossy(&item.member).to_string(), item.score);
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

        let mut handler = TestRdbHandler { map: HashMap::new() };

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_ziplist_that_compresses_easily() {
        let mut file = File::open("tests/rdb/ziplist_that_compresses_easily_1.rdb").expect("file not found");

        struct TestRdbHandler {
            list: Vec<String>,
        }

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, data: Event) {
                match data {
                    Event::RDB(rdb) => match rdb {
                        Object::List(list) => {
                            assert_eq!("ziplist_compresses_easily", String::from_utf8_lossy(list.key));
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
                            assert_eq!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", self.list.get(5).unwrap());
                        }
                        _ => {}
                    },
                    Event::AOF(_) => {}
                }
            }
        }

        let mut handler = TestRdbHandler { list: Vec::new() };

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_zset_ziplist() {
        let mut file = File::open("tests/rdb/parser_filters.rdb").expect("file not found");

        struct TestRdbHandler {
            map: HashMap<String, f64>,
        }

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, data: Event) {
                match data {
                    Event::RDB(rdb) => match rdb {
                        Object::SortedSet(set) => {
                            let key = String::from_utf8_lossy(set.key);
                            match key.as_ref() {
                                "z1" => assert_eq!(2, set.items.len()),
                                "z2" => assert_eq!(3, set.items.len()),
                                "z3" => assert_eq!(2, set.items.len()),
                                "z4" => assert_eq!(3, set.items.len()),
                                _ => panic!("unknown key name: {}", key.as_ref()),
                            }
                            for item in set.items {
                                self.map
                                    .insert(String::from_utf8_lossy(&item.member).to_string(), item.score);
                            }
                        }
                        Object::EOR => {
                            assert_eq!(1.0, self.map.get("1").unwrap().clone());
                            assert_eq!(2.0, self.map.get("2").unwrap().clone());
                            assert_eq!(3.0, self.map.get("3").unwrap().clone());
                            assert_eq!(10001.0, self.map.get("10002").unwrap().clone());
                            assert_eq!(10003.0, self.map.get("10003").unwrap().clone());
                            assert_eq!(1.0000000001E10, self.map.get("10000000001").unwrap().clone());
                            assert_eq!(10000000002.0, self.map.get("10000000002").unwrap().clone());
                            assert_eq!(1.0000000003E10, self.map.get("10000000003").unwrap().clone());
                        }
                        _ => {}
                    },
                    Event::AOF(_) => {}
                }
            }
        }

        let mut handler = TestRdbHandler { map: HashMap::new() };

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_ziplist_with_integers() {
        let mut file = File::open("tests/rdb/ziplist_with_integers.rdb").expect("file not found");

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, data: Event) {
                match data {
                    Event::RDB(rdb) => match rdb {
                        Object::List(list) => {
                            let key = String::from_utf8_lossy(list.key);
                            assert_eq!("ziplist_with_integers", key);

                            let vec = [
                                "0",
                                "1",
                                "2",
                                "3",
                                "4",
                                "5",
                                "6",
                                "7",
                                "8",
                                "9",
                                "10",
                                "11",
                                "12",
                                "13",
                                "-2",
                                "25",
                                "-61",
                                "63",
                                "16380",
                                "-16000",
                                "65535",
                                "-65523",
                                "4194304",
                                "9223372036854775807",
                            ];

                            for val in list.values {
                                let val = String::from_utf8_lossy(val);
                                assert!(vec.contains(&val.as_ref()));
                            }
                        }
                        _ => {}
                    },
                    Event::AOF(_) => {}
                }
            }
        }

        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_dump_lru() {
        let mut file = File::open("tests/rdb/dump-lru.rdb").expect("file not found");

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, data: Event) {
                match data {
                    Event::RDB(rdb) => match rdb {
                        Object::String(kv) => {
                            let key = String::from_utf8_lossy(kv.key);
                            if "key".eq(&key) {
                                if let Some((ExpireType::Millisecond, val)) = kv.meta.expire {
                                    assert_eq!(1528592665231, val);
                                } else {
                                    panic!("no expire");
                                }
                                if let Some((EvictType::LRU, val)) = kv.meta.evict {
                                    assert_eq!(4, val);
                                } else {
                                    panic!("no evict");
                                }
                            } else if "key1".eq(&key) {
                                if let Some((EvictType::LRU, val)) = kv.meta.evict {
                                    assert_eq!(1914611, val);
                                } else {
                                    panic!("no evict");
                                }
                                if let Some(_) = kv.meta.expire {
                                    panic!("have expire");
                                }
                            } else {
                                panic!("unknown key");
                            }
                        }
                        _ => {}
                    },
                    Event::AOF(_) => {}
                }
            }
        }

        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_dump_lfu() {
        let mut file = File::open("tests/rdb/dump-lfu.rdb").expect("file not found");

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, data: Event) {
                match data {
                    Event::RDB(rdb) => match rdb {
                        Object::String(kv) => {
                            let key = String::from_utf8_lossy(kv.key);
                            if "key".eq(&key) {
                                if let Some((ExpireType::Millisecond, val)) = kv.meta.expire {
                                    assert_eq!(1528592896226, val);
                                } else {
                                    panic!("no expire");
                                }
                                if let Some((EvictType::LFU, val)) = kv.meta.evict {
                                    assert_eq!(4, val);
                                } else {
                                    panic!("no evict");
                                }
                            } else if "key1".eq(&key) {
                                if let Some((EvictType::LFU, val)) = kv.meta.evict {
                                    assert_eq!(1, val);
                                } else {
                                    panic!("no evict");
                                }
                                if let Some(_) = kv.meta.expire {
                                    panic!("have expire");
                                }
                            } else {
                                panic!("unknown key");
                            }
                        }
                        _ => {}
                    },
                    Event::AOF(_) => {}
                }
            }
        }
        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    struct HelloModuleParser {}

    #[derive(Debug)]
    struct HelloModule {
        pub values: Vec<i64>,
    }

    impl Module for HelloModule {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl HelloModuleParser {
        fn load_unsigned(&self, input: &mut dyn Read, version: usize) -> num_bigint::BigInt {
            let mut digits = [0; 8];
            let value = self.load_signed(input, version);
            for i in 0..8 {
                digits[7 - i] = ((value as usize >> (i << 3)) & 0xFF) as u8;
            }
            num_bigint::BigInt::from_bytes_be(Sign::Plus, &digits)
        }

        fn load_signed(&self, input: &mut dyn Read, version: usize) -> i64 {
            if version == 2 {
                let (opcode, _) = input.read_length().unwrap();
                if opcode != 2 {
                    panic!("opcode != 2");
                }
            }
            let (len, _) = input.read_length().unwrap();
            len as i64
        }
    }

    impl ModuleParser for HelloModuleParser {
        fn parse(&mut self, input: &mut dyn Read, _module_name: &str, module_version: usize) -> Box<dyn Module> {
            let elements = self.load_unsigned(input, module_version);
            let elements = elements.to_u32().unwrap();

            let mut array = Vec::new();
            for _ in 0..elements {
                let val = self.load_signed(input, module_version);
                array.push(val);
            }

            Box::new(HelloModule { values: array })
        }
    }

    #[test]
    fn test_module() {
        let mut file = File::open("tests/rdb/module.rdb").expect("file not found");

        let parser = Rc::new(RefCell::new(HelloModuleParser {}));

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, event: Event) {
                match event {
                    Event::RDB(rdb) => match rdb {
                        Object::Module(_, module, _) => {
                            let hello_module: &HelloModule = match module.as_any().downcast_ref::<HelloModule>() {
                                Some(hello_module) => hello_module,
                                None => panic!("not HelloModule"),
                            };
                            let values = &hello_module.values;
                            assert_eq!(1, values.len());
                            let val = values.get(0).unwrap();
                            assert_eq!(&12123123112, val);
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }

        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: Some(parser),
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_module2() {
        let mut file = File::open("tests/rdb/dump-module-2.rdb").expect("file not found");

        let parser = Rc::new(RefCell::new(HelloModuleParser {}));

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, event: Event) {
                match event {
                    Event::RDB(rdb) => match rdb {
                        Object::Module(_, module, _) => {
                            let hello_module: &HelloModule = match module.as_any().downcast_ref::<HelloModule>() {
                                Some(hello_module) => hello_module,
                                None => panic!("not HelloModule"),
                            };
                            let values = &hello_module.values;
                            assert_eq!(2, values.len());
                            let val1 = values.get(0).unwrap();
                            assert_eq!(-1025, *val1);
                            let val2 = values.get(1).unwrap();
                            assert_eq!(-1024, *val2);
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }

        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: Some(parser),
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_module2_skip() {
        let mut file = File::open("tests/rdb/dump-json-module.rdb").expect("file not found");

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, _: Event) {}
        }

        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_stream() {
        let mut file = File::open("tests/rdb/dump-stream.rdb").expect("file not found");

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, event: Event) {
                match event {
                    Event::RDB(rdb) => match rdb {
                        Object::Stream(key, stream) => {
                            let key = String::from_utf8(key).unwrap();
                            if &key == "listpack" {
                                let mut i = 0;
                                for (_, entry) in &stream.entries {
                                    let __key = format!("field{}", i);
                                    assert!(entry.fields.contains_key(__key.as_bytes()));
                                    i += 1;
                                }
                                assert!(stream.groups.len() == 4);
                            } else if &key == "trim" {
                                let mut i = 0;
                                for (_, entry) in &stream.entries {
                                    if i < 20 {
                                        assert!(entry.deleted)
                                    } else {
                                        if entry.id.eq(&ID {
                                            ms: 1528512149341,
                                            seq: 0,
                                        }) {
                                            assert!(entry.deleted)
                                        } else if entry.id.eq(&ID {
                                            ms: 1528512149742,
                                            seq: 0,
                                        }) {
                                            assert!(entry.deleted)
                                        } else {
                                            assert!(entry.deleted == false)
                                        }
                                    }
                                    i += 1;
                                }
                            } else if &key == "nums" {
                                let mut i = 0;
                                for (_, entry) in stream.entries {
                                    match i {
                                        0 => {
                                            let __key = format!("{}", -2);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        1 => {
                                            let __key = format!("{}", -2000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        2 => {
                                            let __key = format!("{}", -20000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        3 => {
                                            let __key = format!("{}", -200000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        4 => {
                                            let __key = format!("{}", -20000000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        5 => {
                                            let __key = format!("{}", -2000000000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        6 => {
                                            let __key = format!("{}", -200000000000 as i64);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        7 => {
                                            let __key = format!("{}", -20000000000000 as i64);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        8 => {
                                            let __key = format!("{}", -2);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        9 => {
                                            let __key = format!("{}", -2000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        10 => {
                                            let __key = format!("{}", -20000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        11 => {
                                            let __key = format!("{}", -200000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        12 => {
                                            let __key = format!("{}", -20000000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        13 => {
                                            let __key = format!("{}", -2000000000);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        14 => {
                                            let __key = format!("{}", -200000000000 as i64);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        15 => {
                                            let __key = format!("{}", -20000000000000 as i64);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        16 => {
                                            let __key = format!("{}", -20);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        17 => {
                                            let __key = format!("{}", -200);
                                            assert!(entry.fields.contains_key(__key.as_bytes()));
                                        }
                                        _ => break,
                                    }
                                    i += 1;
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }

        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }

    #[test]
    fn test_stream1() {
        let mut file = File::open("tests/rdb/dump-stream1.rdb").expect("file not found");

        struct TestRdbHandler {}

        impl EventHandler for TestRdbHandler {
            fn handle(&mut self, _: Event) {}
        }

        let mut handler = TestRdbHandler {};

        let mut rdb_parser = DefaultRDBParser {
            running: Arc::new(AtomicBool::new(true)),
            module_parser: None,
        };
        rdb_parser.parse(&mut file, 0, &mut handler).unwrap();
    }
}

#[cfg(test)]
mod aof_tests {
    use std::fs::File;

    use crate::cmd::Command;
    use crate::resp::{Resp, RespDecode};
    use crate::{Event, EventHandler, cmd};
    use std::io::ErrorKind;

    #[test]
    fn test_aof1() {
        let mut file = File::open("tests/aof/appendonly1.aof").expect("file not found");

        struct TestCmdHandler {}

        impl EventHandler for TestCmdHandler {
            fn handle(&mut self, cmd: Event) {
                match cmd {
                    Event::RDB(_) => {}
                    Event::AOF(cmd) => match cmd {
                        Command::HMSET(hmset) => {
                            let key = String::from_utf8_lossy(hmset.key);
                            if "key".eq(&key) {
                                for field in &hmset.fields {
                                    let name = String::from_utf8_lossy(field.name);
                                    if "field".eq(&name) {
                                        let val = String::from_utf8_lossy(field.value);
                                        assert_eq!("a", val);
                                    } else if "field1".eq(&name) {
                                        let val = String::from_utf8_lossy(field.value);
                                        assert_eq!("b", val);
                                    } else {
                                        panic!("wrong name");
                                    }
                                }
                            } else {
                                panic!("wrong key");
                            }
                        }
                        Command::SELECT(select) => {
                            assert_eq!(0, select.db);
                        }
                        Command::SET(set) => {
                            let key = String::from_utf8_lossy(set.key);
                            let val = String::from_utf8_lossy(set.value);
                            assert_eq!("a", key);
                            assert_eq!("b", val);
                        }
                        _ => {}
                    },
                }
            }
        }

        let mut cmd_handler = TestCmdHandler {};

        loop {
            match file.decode_resp() {
                Ok(resp) => match resp {
                    Resp::Array(arr) => {
                        let mut data = Vec::new();
                        for x in arr {
                            if let Resp::BulkBytes(bytes) = x {
                                data.push(bytes);
                            } else {
                                panic!("wrong data type");
                            }
                        }
                        cmd::parse(data, &mut cmd_handler);
                    }
                    _ => panic!("wrong resp type "),
                },
                Err(ref e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        panic!("err")
                    }
                }
            }
        }
    }

    #[test]
    fn test_aof2() {
        let mut file = File::open("tests/aof/appendonly2.aof").expect("file not found");

        struct TestCmdHandler {
            count: isize,
        }

        impl EventHandler for TestCmdHandler {
            fn handle(&mut self, cmd: Event) {
                match cmd {
                    Event::RDB(_) => {}
                    Event::AOF(cmd) => {
                        if let Command::SET(set) = cmd {
                            let key = String::from_utf8_lossy(set.key);
                            if key.starts_with("test_") {
                                self.count += 1;
                            }
                        }
                    }
                }
            }
        }

        let mut cmd_handler = TestCmdHandler { count: 0 };

        loop {
            match file.decode_resp() {
                Ok(resp) => match resp {
                    Resp::Array(arr) => {
                        let mut data = Vec::new();
                        for x in arr {
                            if let Resp::BulkBytes(bytes) = x {
                                data.push(bytes);
                            } else {
                                panic!("wrong data type");
                            }
                        }
                        cmd::parse(data, &mut cmd_handler);
                    }
                    _ => panic!("wrong resp type "),
                },
                Err(ref e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        panic!("err")
                    }
                }
            }
        }

        assert_eq!(48000, cmd_handler.count);
    }

    #[test]
    fn test_aof3() {
        let mut file = File::open("tests/aof/appendonly3.aof").expect("file not found");

        struct TestCmdHandler {
            count: isize,
        }

        impl EventHandler for TestCmdHandler {
            fn handle(&mut self, _cmd: Event) {
                self.count += 1;
            }
        }

        let mut cmd_handler = TestCmdHandler { count: 0 };
        loop {
            match file.decode_resp() {
                Ok(resp) => match resp {
                    Resp::Array(arr) => {
                        let mut data = Vec::new();
                        for x in arr {
                            if let Resp::BulkBytes(bytes) = x {
                                data.push(bytes);
                            } else {
                                panic!("wrong data type");
                            }
                        }
                        cmd::parse(data, &mut cmd_handler);
                    }
                    _ => panic!("wrong resp type "),
                },
                Err(ref e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        panic!("err")
                    }
                }
            }
        }
        assert_eq!(92539, cmd_handler.count);
    }

    #[test]
    fn test_aof5() {
        let mut file = File::open("tests/aof/appendonly5.aof").expect("file not found");

        struct TestCmdHandler {
            count: isize,
        }

        impl EventHandler for TestCmdHandler {
            fn handle(&mut self, _cmd: Event) {
                self.count += 1;
            }
        }

        let mut cmd_handler = TestCmdHandler { count: 0 };

        loop {
            match file.decode_resp() {
                Ok(resp) => match resp {
                    Resp::Array(arr) => {
                        let mut data = Vec::new();
                        for x in arr {
                            if let Resp::BulkBytes(bytes) = x {
                                data.push(bytes);
                            } else {
                                panic!("wrong data type");
                            }
                        }
                        cmd::parse(data, &mut cmd_handler);
                    }
                    _ => panic!("wrong resp type "),
                },
                Err(ref e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        panic!("err")
                    }
                }
            }
        }

        assert_eq!(71, cmd_handler.count);
    }

    #[test]
    fn test_aof6() {
        let mut file = File::open("tests/aof/appendonly.aof").expect("file not found");

        struct TestCmdHandler {}

        impl EventHandler for TestCmdHandler {
            fn handle(&mut self, cmd: Event) {
                match cmd {
                    Event::RDB(_) => {}
                    Event::AOF(cmd) => match cmd {
                        Command::XADD(xadd) => {
                            let key = String::from_utf8_lossy(xadd.key);
                            assert_eq!(key, "stream");
                            for field in &xadd.fields {
                                let name = String::from_utf8_lossy(field.name);
                                let value = String::from_utf8_lossy(field.value);
                                if "name" == name {
                                    assert_eq!("tomcat", value);
                                } else if "age" == name {
                                    assert_eq!("18", value);
                                }
                            }
                        }
                        Command::XGROUP(xgroup) => {
                            if let Some(create) = &xgroup.create {
                                let key = String::from_utf8_lossy(create.key);
                                assert_eq!("stream", key);
                                let group = String::from_utf8_lossy(create.group_name);
                                assert_eq!("group", group);
                            }
                        }
                        Command::XTRIM(xtrim) => {
                            assert_eq!(false, xtrim.approximation);
                            assert_eq!(1, xtrim.count);
                        }
                        Command::XDEL(xdel) => {
                            assert!(xdel.ids.len() == 1);
                            let id = xdel.ids.get(0);
                            let id = id.unwrap();
                            let id = String::from_utf8_lossy(*id);
                            assert_eq!("1588842699754-0", id);
                        }
                        _ => {}
                    },
                }
            }
        }

        let mut cmd_handler = TestCmdHandler {};
        loop {
            match file.decode_resp() {
                Ok(resp) => match resp {
                    Resp::Array(arr) => {
                        let mut data = Vec::new();
                        for x in arr {
                            if let Resp::BulkBytes(bytes) = x {
                                data.push(bytes);
                            } else {
                                panic!("wrong data type");
                            }
                        }
                        cmd::parse(data, &mut cmd_handler);
                    }
                    _ => panic!("wrong resp type "),
                },
                Err(ref e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        panic!("err")
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod other_tests {
    use crate::rdb::ID;

    #[test]
    fn test_id_cmp() {
        let mut id1 = ID { ms: 0, seq: 0 };
        let id2 = ID { ms: 0, seq: 1 };

        assert_eq!(id1 < id2, true);

        id1.seq = 1;
        assert_eq!(id1 == id2, true);
        assert_eq!(id1 >= id2, true);
        assert_eq!(id1 <= id2, true);

        id1.ms = 1;
        id1.seq = 0;
        assert_eq!(id1 > id2, true);
    }
}
