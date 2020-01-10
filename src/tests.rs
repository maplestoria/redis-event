#[cfg(test)]
mod test_cases {
    use std::any::Any;
    use std::collections::HashMap;
    use std::fs::File;
    
    use crate::{io, NoOpCommandHandler, rdb, RdbHandler};
    use crate::io::ReadWrite;
    use crate::rdb::Object;
    
    #[test]
    fn test_zipmap_not_compress() {
        let file = File::open("tests/rdb/zipmap_that_doesnt_compress.rdb").expect("file not found");
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
        let file = File::open("tests/rdb/zipmap_that_compresses_easily.rdb").expect("file not found");
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
}
