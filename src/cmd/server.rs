use std::slice::Iter;

/// 这个模块处理server相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#server
///
#[derive(Debug)]
pub struct FLUSHDB {
    pub db: u8
}

pub(crate) fn parse_flushdb(mut iter: Iter<Vec<u8>>) -> FLUSHDB {
    let db = iter.next().unwrap();
    let db = String::from_utf8_lossy(db).parse::<u8>().unwrap();
    FLUSHDB { db }
}