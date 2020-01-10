#[cfg(test)]
mod test_cases {
    use std::collections::HashMap;
    use std::fs::File;
    
    use crate::{io, NoOpCommandHandler, rdb, RdbHandler};
    use crate::rdb::Object;
    
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
}
