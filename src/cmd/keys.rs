/*!
Keys相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#generic
*/

use std::slice::Iter;

use crate::cmd::keys::ORDER::{ASC, DESC};

#[derive(Debug)]
pub struct DEL<'a> {
    pub keys: Vec<&'a Vec<u8>>,
}

pub(crate) fn parse_del(iter: Iter<Vec<u8>>) -> DEL {
    let mut keys = Vec::new();
    for next_key in iter {
        keys.push(next_key);
    }
    DEL { keys }
}

#[derive(Debug)]
pub struct PERSIST<'a> {
    pub key: &'a [u8],
}

pub(crate) fn parse_persist(mut iter: Iter<Vec<u8>>) -> PERSIST {
    let key = iter.next().unwrap();
    PERSIST { key }
}

#[derive(Debug)]
pub struct EXPIRE<'a> {
    pub key: &'a [u8],
    pub seconds: &'a [u8],
}

pub(crate) fn parse_expire(mut iter: Iter<Vec<u8>>) -> EXPIRE {
    let key = iter.next().unwrap();
    let seconds = iter.next().unwrap();
    EXPIRE { key, seconds }
}

#[derive(Debug)]
pub struct PEXPIRE<'a> {
    pub key: &'a [u8],
    pub milliseconds: &'a [u8],
}

pub(crate) fn parse_pexpire(mut iter: Iter<Vec<u8>>) -> PEXPIRE {
    let key = iter.next().unwrap();
    let milliseconds = iter.next().unwrap();
    PEXPIRE { key, milliseconds }
}

#[derive(Debug)]
pub struct EXPIREAT<'a> {
    pub key: &'a [u8],
    pub timestamp: &'a [u8],
}

pub(crate) fn parse_expireat(mut iter: Iter<Vec<u8>>) -> EXPIREAT {
    let key = iter.next().unwrap();
    let timestamp = iter.next().unwrap();
    EXPIREAT { key, timestamp }
}

#[derive(Debug)]
pub struct PEXPIREAT<'a> {
    pub key: &'a [u8],
    pub mill_timestamp: &'a [u8],
}

pub(crate) fn parse_pexpireat(mut iter: Iter<Vec<u8>>) -> PEXPIREAT {
    let key = iter.next().unwrap();
    let mill_timestamp = iter.next().unwrap();
    PEXPIREAT {
        key,
        mill_timestamp,
    }
}

#[derive(Debug)]
pub struct MOVE<'a> {
    pub key: &'a [u8],
    pub db: &'a [u8],
}

pub(crate) fn parse_move(mut iter: Iter<Vec<u8>>) -> MOVE {
    let key = iter.next().unwrap();
    let db = iter.next().unwrap();
    MOVE { key, db }
}

#[derive(Debug)]
pub struct RENAME<'a> {
    pub key: &'a [u8],
    pub new_key: &'a [u8],
}

pub(crate) fn parse_rename(mut iter: Iter<Vec<u8>>) -> RENAME {
    let key = iter.next().unwrap();
    let new_key = iter.next().unwrap();
    RENAME { key, new_key }
}

#[derive(Debug)]
pub struct RENAMENX<'a> {
    pub key: &'a [u8],
    pub new_key: &'a [u8],
}

pub(crate) fn parse_renamenx(mut iter: Iter<Vec<u8>>) -> RENAMENX {
    let key = iter.next().unwrap();
    let new_key = iter.next().unwrap();
    RENAMENX { key, new_key }
}

#[derive(Debug)]
pub struct RESTORE<'a> {
    pub key: &'a [u8],
    pub ttl: &'a [u8],
    pub value: &'a [u8],
    pub replace: Option<bool>,
    pub abs_ttl: Option<bool>,
    pub idle_time: Option<&'a [u8]>,
    pub freq: Option<&'a [u8]>,
}

pub(crate) fn parse_restore(mut iter: Iter<Vec<u8>>) -> RESTORE {
    let key = iter.next().unwrap();
    let ttl = iter.next().unwrap();
    let value = iter.next().unwrap();
    let mut replace = None;
    let mut abs_ttl = None;
    let mut idle_time = None;
    let mut freq = None;
    while let Some(next_arg) = iter.next() {
        let arg = String::from_utf8_lossy(next_arg).to_uppercase();
        if &arg == "REPLACE" {
            replace = Some(true);
        } else if &arg == "ABSTTL" {
            abs_ttl = Some(true);
        } else if &arg == "IDLETIME" {
            idle_time = Some(iter.next().unwrap().as_slice());
        } else if &arg == "FREQ" {
            freq = Some(iter.next().unwrap().as_slice());
        }
    }
    RESTORE {
        key,
        ttl,
        value,
        replace,
        abs_ttl,
        idle_time,
        freq,
    }
}

#[derive(Debug)]
pub struct SORT<'a> {
    pub key: &'a [u8],
    pub by_pattern: Option<&'a [u8]>,
    pub limit: Option<LIMIT<'a>>,
    pub get_patterns: Option<Vec<&'a [u8]>>,
    pub order: Option<ORDER>,
    pub alpha: Option<bool>,
    pub destination: Option<&'a [u8]>,
}

#[derive(Debug)]
pub struct LIMIT<'a> {
    pub offset: &'a [u8],
    pub count: &'a [u8],
}

#[derive(Debug)]
pub enum ORDER {
    ASC,
    DESC,
}

pub(crate) fn parse_sort(mut iter: Iter<Vec<u8>>) -> SORT {
    let key = iter.next().unwrap();
    let mut order = None;
    let mut alpha = None;
    let mut limit = None;
    let mut destination = None;
    let mut by_pattern = None;
    let mut patterns = Vec::new();
    let mut get_patterns = None;

    while let Some(next_arg) = iter.next() {
        let arg_upper = String::from_utf8_lossy(next_arg).to_uppercase();
        if &arg_upper == "ASC" {
            order = Some(ASC);
        } else if &arg_upper == "DESC" {
            order = Some(DESC);
        } else if &arg_upper == "ALPHA" {
            alpha = Some(true);
        } else if &arg_upper == "LIMIT" {
            let offset = iter.next().unwrap();
            let count = iter.next().unwrap();
            limit = Some(LIMIT { offset, count });
        } else if &arg_upper == "STORE" {
            let store = iter.next().unwrap();
            destination = Some(store.as_slice());
        } else if &arg_upper == "BY" {
            let pattern = iter.next().unwrap();
            by_pattern = Some(pattern.as_slice());
        } else if &arg_upper == "GET" {
            let next_pattern = iter.next().unwrap();
            patterns.push(next_pattern.as_slice());
        }
    }
    if !patterns.is_empty() {
        get_patterns = Some(patterns);
    }
    SORT {
        key,
        by_pattern,
        limit,
        get_patterns,
        order,
        alpha,
        destination,
    }
}

#[derive(Debug)]
pub struct UNLINK<'a> {
    pub keys: Vec<&'a [u8]>,
}

pub(crate) fn parse_unlink(mut iter: Iter<Vec<u8>>) -> UNLINK {
    let mut keys = Vec::new();
    while let Some(next_key) = iter.next() {
        keys.push(next_key.as_slice());
    }
    UNLINK { keys }
}
