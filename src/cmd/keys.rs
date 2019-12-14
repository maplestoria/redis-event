use std::slice::Iter;

/// 这个模块处理keys相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#generic
///
#[derive(Debug)]
pub struct DEL<'a> {
    pub keys: Vec<&'a Vec<u8>>
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
    pub key: &'a [u8]
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
    PEXPIREAT { key, mill_timestamp }
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