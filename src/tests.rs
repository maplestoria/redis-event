// 测试用例
#[cfg(test)]
mod test_case {
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;
    
    use crate::{EchoRdbHandler, RedisListener};
    use crate::listener::standalone;
    
    #[test]
    fn open() {
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let mut redis_listener = standalone::new(SocketAddr::new(ip, 6379), "123456");
        redis_listener.add_rdb_listener(Box::new(EchoRdbHandler {}));
        if let Err(error) = redis_listener.open() {
            panic!("couldn't connect to server: {}", error)
        }
        redis_listener.close();
    }
}