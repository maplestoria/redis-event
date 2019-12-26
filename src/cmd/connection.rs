use std::slice::Iter;

/// 这个模块处理connection相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#connection
///
#[derive(Debug)]
pub struct SELECT {
    pub db: u8
}

pub(crate) fn parse_select(mut iter: Iter<Vec<u8>>) -> SELECT {
    let db = String::from_utf8_lossy(iter.next().unwrap());
    let db = db.parse::<u8>().unwrap();
    SELECT { db }
}

#[derive(Debug)]
pub struct SWAPDB {
    pub index1: u8,
    pub index2: u8,
}

pub(crate) fn parse_swapdb(mut iter: Iter<Vec<u8>>) -> SWAPDB {
    let index1 = String::from_utf8_lossy(iter.next().unwrap());
    let index1 = index1.parse::<u8>().unwrap();
    
    let index2 = String::from_utf8_lossy(iter.next().unwrap());
    let index2 = index2.parse::<u8>().unwrap();
    
    SWAPDB { index1, index2 }
}