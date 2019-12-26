use std::slice::Iter;

/// 这个模块处理HyperLogLog相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#hyperloglog
///
#[derive(Debug)]
pub struct PFADD<'a> {
    pub key: &'a [u8],
    pub elements: Vec<&'a [u8]>,
}

pub(crate) fn parse_pfadd(mut iter: Iter<Vec<u8>>) -> PFADD {
    let key = iter.next().unwrap();
    let mut elements = Vec::new();
    while let Some(element) = iter.next() {
        elements.push(element.as_slice());
    }
    PFADD { key, elements }
}

#[derive(Debug)]
pub struct PFCOUNT<'a> {
    pub keys: Vec<&'a [u8]>,
}

pub(crate) fn parse_pfcount(mut iter: Iter<Vec<u8>>) -> PFCOUNT {
    let mut keys = Vec::new();
    while let Some(key) = iter.next() {
        keys.push(key.as_slice());
    }
    PFCOUNT { keys }
}

#[derive(Debug)]
pub struct PFMERGE<'a> {
    pub dest_key: &'a [u8],
    pub source_keys: Vec<&'a [u8]>,
}

pub(crate) fn parse_pfmerge(mut iter: Iter<Vec<u8>>) -> PFMERGE {
    let dest_key = iter.next().unwrap();
    let mut source_keys = Vec::new();
    while let Some(source) = iter.next() {
        source_keys.push(source.as_slice());
    }
    PFMERGE { dest_key, source_keys }
}