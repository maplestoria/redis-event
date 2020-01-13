use std::net::SocketAddr;

#[derive(Debug)]
pub struct Config {
    pub is_discard_rdb: bool,
    pub is_aof: bool,
    pub addr: SocketAddr,
    pub password: String,
    pub repl_id: String,
    pub repl_offset: i64,
}