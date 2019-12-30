use std::slice::Iter;

/// 这个模块处理server相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#server
///
#[derive(Debug)]
pub struct FLUSHDB<'a> {
    pub db: &'a [u8]
}

pub(crate) fn parse_flushdb(mut iter: Iter<Vec<u8>>) -> FLUSHDB {
    let db = iter.next().unwrap();
    FLUSHDB { db }
}