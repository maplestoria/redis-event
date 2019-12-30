use std::slice::Iter;

/// 这个模块处理connection相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#connection
///
#[derive(Debug)]
pub struct SELECT {
    pub db: i32
}

pub(crate) fn parse_select(mut iter: Iter<Vec<u8>>) -> SELECT {
    let db = String::from_utf8_lossy(iter.next().unwrap());
    let db = db.parse::<i32>().unwrap();
    SELECT { db }
}

#[derive(Debug)]
pub struct SWAPDB<'a> {
    pub index1: &'a [u8],
    pub index2: &'a [u8],
}

pub(crate) fn parse_swapdb(mut iter: Iter<Vec<u8>>) -> SWAPDB {
    let index1 = iter.next().unwrap();
    let index2 = iter.next().unwrap();
    SWAPDB { index1, index2 }
}