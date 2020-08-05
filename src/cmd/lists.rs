/*!
Lists相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#list
*/

use std::slice::Iter;

use crate::cmd::lists::POSITION::{AFTER, BEFORE};

#[derive(Debug)]
pub struct BRPOPLPUSH<'a> {
    pub source: &'a [u8],
    pub destination: &'a [u8],
    pub timeout: &'a [u8],
}

pub(crate) fn parse_brpoplpush(mut iter: Iter<Vec<u8>>) -> BRPOPLPUSH {
    let source = iter.next().unwrap();
    let destination = iter.next().unwrap();
    let timeout = iter.next().unwrap();
    BRPOPLPUSH {
        source,
        destination,
        timeout,
    }
}

#[derive(Debug)]
pub struct LINSERT<'a> {
    pub key: &'a [u8],
    pub position: POSITION,
    pub pivot: &'a [u8],
    pub element: &'a [u8],
}

#[derive(Debug)]
pub enum POSITION {
    BEFORE,
    AFTER,
}

pub(crate) fn parse_linsert(mut iter: Iter<Vec<u8>>) -> LINSERT {
    let key = iter.next().unwrap();
    let next_arg = iter.next().unwrap();
    let position;
    let arg_upper = String::from_utf8_lossy(next_arg).to_uppercase();
    if &arg_upper == "BEFORE" {
        position = BEFORE;
    } else {
        position = AFTER;
    }
    let pivot = iter.next().unwrap();
    let element = iter.next().unwrap();
    LINSERT {
        key,
        position,
        pivot,
        element,
    }
}

#[derive(Debug)]
pub struct LPOP<'a> {
    pub key: &'a [u8],
}

pub(crate) fn parse_lpop(mut iter: Iter<Vec<u8>>) -> LPOP {
    let key = iter.next().unwrap();
    LPOP { key }
}

#[derive(Debug)]
pub struct LPUSH<'a> {
    pub key: &'a [u8],
    pub elements: Vec<&'a [u8]>,
}

pub(crate) fn parse_lpush(mut iter: Iter<Vec<u8>>) -> LPUSH {
    let key = iter.next().unwrap();
    let mut elements = Vec::new();
    while let Some(ele) = iter.next() {
        elements.push(ele.as_slice());
    }
    LPUSH { key, elements }
}

#[derive(Debug)]
pub struct LPUSHX<'a> {
    pub key: &'a [u8],
    pub elements: Vec<&'a [u8]>,
}

pub(crate) fn parse_lpushx(mut iter: Iter<Vec<u8>>) -> LPUSHX {
    let key = iter.next().unwrap();
    let mut elements = Vec::new();
    while let Some(ele) = iter.next() {
        elements.push(ele.as_slice());
    }
    LPUSHX { key, elements }
}

#[derive(Debug)]
pub struct LREM<'a> {
    pub key: &'a [u8],
    pub count: &'a [u8],
    pub element: &'a [u8],
}

pub(crate) fn parse_lrem(mut iter: Iter<Vec<u8>>) -> LREM {
    let key = iter.next().unwrap();
    let count = iter.next().unwrap();
    let element = iter.next().unwrap();
    LREM { key, count, element }
}

#[derive(Debug)]
pub struct LSET<'a> {
    pub key: &'a [u8],
    pub index: &'a [u8],
    pub element: &'a [u8],
}

pub(crate) fn parse_lset(mut iter: Iter<Vec<u8>>) -> LSET {
    let key = iter.next().unwrap();
    let index = iter.next().unwrap();
    let element = iter.next().unwrap();
    LSET { key, index, element }
}

#[derive(Debug)]
pub struct LTRIM<'a> {
    pub key: &'a [u8],
    pub start: &'a [u8],
    pub stop: &'a [u8],
}

pub(crate) fn parse_ltrim(mut iter: Iter<Vec<u8>>) -> LTRIM {
    let key = iter.next().unwrap();
    let start = iter.next().unwrap();
    let stop = iter.next().unwrap();
    LTRIM { key, start, stop }
}

#[derive(Debug)]
pub struct RPOP<'a> {
    pub key: &'a [u8],
}

pub(crate) fn parse_rpop(mut iter: Iter<Vec<u8>>) -> RPOP {
    let key = iter.next().unwrap();
    RPOP { key }
}

#[derive(Debug)]
pub struct RPOPLPUSH<'a> {
    pub source: &'a [u8],
    pub destination: &'a [u8],
}

pub(crate) fn parse_rpoplpush(mut iter: Iter<Vec<u8>>) -> RPOPLPUSH {
    let source = iter.next().unwrap();
    let destination = iter.next().unwrap();
    RPOPLPUSH { source, destination }
}

#[derive(Debug)]
pub struct RPUSH<'a> {
    pub key: &'a [u8],
    pub elements: Vec<&'a [u8]>,
}

pub(crate) fn parse_rpush(mut iter: Iter<Vec<u8>>) -> RPUSH {
    let key = iter.next().unwrap();
    let mut elements = Vec::new();
    while let Some(ele) = iter.next() {
        elements.push(ele.as_slice());
    }
    RPUSH { key, elements }
}

#[derive(Debug)]
pub struct RPUSHX<'a> {
    pub key: &'a [u8],
    pub elements: Vec<&'a [u8]>,
}

pub(crate) fn parse_rpushx(mut iter: Iter<Vec<u8>>) -> RPUSHX {
    let key = iter.next().unwrap();
    let mut elements = Vec::new();
    while let Some(ele) = iter.next() {
        elements.push(ele.as_slice());
    }
    RPUSHX { key, elements }
}
