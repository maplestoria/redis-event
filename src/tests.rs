#[cfg(test)]
mod rdb_tests {
    use std::collections::HashMap;
    use std::fs::File;
    
    use crate::{io, NoOpCommandHandler, rdb, RdbHandler};
    use crate::rdb::{EvictType, ExpireType, Object};
    
    #[test]
    fn test_zipmap_not_compress() {
        let file = File::open("tests/rdb/zipmap_that_doesnt_compress_1.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {
            map: HashMap<String, String>
        }
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
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
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler { map: HashMap::new() }, &mut NoOpCommandHandler {})
            .unwrap();
    }
    
    #[test]
    fn test_zipmap_compress() {
        let file = File::open("tests/rdb/zipmap_that_compresses_easily_1.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {
            map: HashMap<String, String>
        }
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
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
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler { map: HashMap::new() }, &mut NoOpCommandHandler {})
            .unwrap();
    }
    
    #[test]
    fn test_regular_sorted_set() {
        let file = File::open("tests/rdb/regular_sorted_set_1.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {
            map: HashMap<String, f64>
        }
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
                    Object::SortedSet(set) => {
                        let key = String::from_utf8_lossy(set.key).to_string();
                        assert_eq!("force_sorted_set", key);
                        for item in set.items {
                            self.map.insert(String::from_utf8_lossy(&item.member).to_string(), item.score);
                        }
                    }
                    Object::EOR => {
                        assert_eq!(500, self.map.len());
                        assert_eq!(3.19, self.map.get("G72TWVWH0DY782VG0H8VVAR8RNO7BS9QGOHTZFJU67X7L0Z3PR").unwrap().clone());
                        assert_eq!(0.76, self.map.get("N8HKPIK4RC4I2CXVV90LQCWODW1DZYD0DA26R8V5QP7UR511M8").unwrap().clone());
                    }
                    _ => {}
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler { map: HashMap::new() }, &mut NoOpCommandHandler {})
            .unwrap();
    }
    
    #[test]
    fn test_ziplist_that_compresses_easily() {
        let file = File::open("tests/rdb/ziplist_that_compresses_easily_1.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {
            list: Vec<String>
        }
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
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
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler { list: Vec::new() }, &mut NoOpCommandHandler {})
            .unwrap();
    }
    
    #[test]
    fn test_zset_ziplist() {
        let file = File::open("tests/rdb/parser_filters.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {
            map: HashMap<String, f64>
        }
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
                    Object::SortedSet(set) => {
                        let key = String::from_utf8_lossy(set.key);
                        match key.as_ref() {
                            "z1" => assert_eq!(2, set.items.len()),
                            "z2" => assert_eq!(3, set.items.len()),
                            "z3" => assert_eq!(2, set.items.len()),
                            "z4" => assert_eq!(3, set.items.len()),
                            _ => panic!("unknown key name: {}", key.as_ref())
                        }
                        for item in set.items {
                            self.map.insert(String::from_utf8_lossy(&item.member).to_string(), item.score);
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
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler { map: HashMap::new() }, &mut NoOpCommandHandler {})
            .unwrap();
    }
    
    #[test]
    fn test_ziplist_with_integers() {
        let file = File::open("tests/rdb/ziplist_with_integers.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {}
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
                    Object::List(list) => {
                        let key = String::from_utf8_lossy(list.key);
                        assert_eq!("ziplist_with_integers", key);
                        
                        let vec = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
                            "11", "12", "13", "-2", "25", "-61", "63", "16380", "-16000", "65535",
                            "-65523", "4194304", "9223372036854775807"];
                        
                        for val in list.values {
                            let val = String::from_utf8_lossy(val);
                            vec.contains(&val.as_ref());
                        }
                    }
                    _ => {}
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler {}, &mut NoOpCommandHandler {})
            .unwrap();
    }
    
    #[test]
    fn test_dump_lru() {
        let file = File::open("tests/rdb/dump-lru.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {}
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
                    Object::String(kv) => {
                        let key = String::from_utf8_lossy(kv.key);
                        if "key".eq(&key) {
                            if let Some((ExpireType::Millisecond, val)) = kv.meta.expire {
                                assert_eq!(1528592665231, val);
                            } else {
                                panic!("no expire");
                            }
                            if let Some((EvictType::Lru, val)) = kv.meta.evict {
                                assert_eq!(4, val);
                            } else {
                                panic!("no evict");
                            }
                        } else if "key1".eq(&key) {
                            if let Some((EvictType::Lru, val)) = kv.meta.evict {
                                assert_eq!(1914611, val);
                            } else {
                                panic!("no evict");
                            }
                        } else {
                            panic!("unknown key");
                        }
                    }
                    _ => {}
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler {}, &mut NoOpCommandHandler {})
            .unwrap();
    }
    
    #[test]
    fn test_dump_lfu() {
        let file = File::open("tests/rdb/dump-lfu.rdb").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestRdbHandler {}
        
        impl RdbHandler for TestRdbHandler {
            fn handle(&mut self, data: Object) {
                match data {
                    Object::String(kv) => {
                        let key = String::from_utf8_lossy(kv.key);
                        if "key".eq(&key) {
                            if let Some((ExpireType::Millisecond, val)) = kv.meta.expire {
                                assert_eq!(1528592896226, val);
                            } else {
                                panic!("no expire");
                            }
                            if let Some((EvictType::Lfu, val)) = kv.meta.evict {
                                assert_eq!(4, val);
                            } else {
                                panic!("no evict");
                            }
                        } else if "key1".eq(&key) {
                            if let Some((EvictType::Lfu, val)) = kv.meta.evict {
                                assert_eq!(1, val);
                            } else {
                                panic!("no evict");
                            }
                        } else {
                            panic!("unknown key");
                        }
                    }
                    _ => {}
                }
            }
        }
        
        rdb::parse(&mut file, 0, &mut TestRdbHandler {}, &mut NoOpCommandHandler {})
            .unwrap();
    }
}

#[cfg(test)]
mod aof_tests {
    use std::error::Error;
    use std::fs::File;
    
    use crate::{cmd, CommandHandler, io, NoOpRdbHandler};
    use crate::cmd::Command;
    use crate::rdb::Data;
    
    #[test]
    fn test_aof1() {
        let file = File::open("tests/aof/appendonly1.aof").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestCmdHandler {}
        
        impl CommandHandler for TestCmdHandler {
            fn handle(&mut self, cmd: Command) {
                match cmd {
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
                }
            }
        }
        
        let mut handler = TestCmdHandler {};
        
        loop {
            match file.reply(io::read_bytes, &mut NoOpRdbHandler {}, &mut handler) {
                Ok(Data::Bytes(_)) => panic!("Expect BytesVec response, but got Bytes"),
                Ok(Data::BytesVec(data)) => cmd::parse(data, &mut handler),
                Err(err) => {
                    // eof reached
                    if "failed to fill whole buffer".eq(err.description()) {
                        break;
                    } else {
                        panic!(err);
                    }
                }
                Ok(Data::Empty) => break
            }
        }
    }
    
    #[test]
    fn test_aof2() {
        let file = File::open("tests/aof/appendonly2.aof").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestCmdHandler {
            count: isize
        }
        
        impl CommandHandler for TestCmdHandler {
            fn handle(&mut self, cmd: Command) {
                if let Command::SET(set) = cmd {
                    let key = String::from_utf8_lossy(set.key);
                    if key.starts_with("test_") {
                        self.count += 1;
                    }
                }
            }
        }
        
        let mut handler = TestCmdHandler { count: 0 };
        
        loop {
            match file.reply(io::read_bytes, &mut NoOpRdbHandler {}, &mut handler) {
                Ok(Data::Bytes(_)) => panic!("Expect BytesVec response, but got Bytes"),
                Ok(Data::BytesVec(data)) => cmd::parse(data, &mut handler),
                Err(err) => {
                    // eof reached
                    if "failed to fill whole buffer".eq(err.description()) {
                        break;
                    } else {
                        panic!(err);
                    }
                }
                Ok(Data::Empty) => break
            }
        }
        
        assert_eq!(48000, handler.count);
    }
    
    #[test]
    fn test_aof3() {
        let file = File::open("tests/aof/appendonly3.aof").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestCmdHandler {
            count: isize
        }
        
        impl CommandHandler for TestCmdHandler {
            fn handle(&mut self, _cmd: Command) {
                self.count += 1;
            }
        }
        
        let mut handler = TestCmdHandler { count: 0 };
        
        loop {
            match file.reply(io::read_bytes, &mut NoOpRdbHandler {}, &mut handler) {
                Ok(Data::Bytes(_)) => panic!("Expect BytesVec response, but got Bytes"),
                Ok(Data::BytesVec(data)) => cmd::parse(data, &mut handler),
                Err(err) => {
                    // eof reached
                    if "failed to fill whole buffer".eq(err.description()) {
                        break;
                    } else {
                        panic!(err);
                    }
                }
                Ok(Data::Empty) => break
            }
        }
        
        assert_eq!(92539, handler.count);
    }
    
    #[test]
    fn test_aof5() {
        let file = File::open("tests/aof/appendonly5.aof").expect("file not found");
        let mut file = io::from_file(file);
        
        struct TestCmdHandler {
            count: isize
        }
        
        impl CommandHandler for TestCmdHandler {
            fn handle(&mut self, _cmd: Command) {
                self.count += 1;
            }
        }
        
        let mut handler = TestCmdHandler { count: 0 };
        
        loop {
            match file.reply(io::read_bytes, &mut NoOpRdbHandler {}, &mut handler) {
                Ok(Data::Bytes(_)) => panic!("Expect BytesVec response, but got Bytes"),
                Ok(Data::BytesVec(data)) => cmd::parse(data, &mut handler),
                Err(err) => {
                    // eof reached
                    if "failed to fill whole buffer".eq(err.description()) {
                        break;
                    } else {
                        panic!(err);
                    }
                }
                Ok(Data::Empty) => break
            }
        }
        
        assert_eq!(71, handler.count);
    }
}