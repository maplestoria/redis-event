/*!
Strings相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#string
*/

use core::slice::Iter;

use crate::cmd::strings::Op::{AND, NOT, OR, XOR};

#[derive(Debug)]
pub struct APPEND<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_append(mut iter: Iter<Vec<u8>>) -> APPEND {
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();
    APPEND { key, value }
}

#[derive(Debug)]
pub struct BITFIELD<'a> {
    pub key: &'a [u8],
    pub statements: Option<Vec<Operation<'a>>>,
    pub overflows: Option<Vec<Overflow>>,
}

#[derive(Debug)]
pub enum Operation<'a> {
    GET(Get<'a>),
    INCRBY(IncrBy<'a>),
    SET(Set<'a>),
}

#[derive(Debug)]
pub struct Get<'a> {
    pub _type: &'a [u8],
    pub offset: &'a [u8],
}

#[derive(Debug)]
pub struct IncrBy<'a> {
    pub _type: &'a [u8],
    pub offset: &'a [u8],
    pub increment: &'a [u8],
}

#[derive(Debug)]
pub struct Set<'a> {
    pub _type: &'a [u8],
    pub offset: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Debug)]
pub enum Overflow {
    WRAP,
    SAT,
    FAIL,
}

pub(crate) fn parse_bitfield(mut iter: Iter<Vec<u8>>) -> BITFIELD {
    let key = iter.next().unwrap();

    let mut statements = Vec::new();
    let mut overflows = Vec::new();
    while let Some(next_arg) = iter.next() {
        let arg_upper = &String::from_utf8_lossy(next_arg).to_uppercase();
        if arg_upper == "GET" {
            let _type = iter.next().expect("bitfield 缺失get type");
            let offset = iter.next().expect("bitfield 缺失get offset");
            statements.push(Operation::GET(Get { _type, offset }));
        } else if arg_upper == "SET" {
            let _type = iter.next().unwrap();
            let offset = iter.next().expect("bitfield 缺失SET offset");
            let value = iter.next().expect("bitfield 缺失SET offset");
            statements.push(Operation::SET(Set {
                _type,
                offset,
                value,
            }));
        } else if arg_upper == "INCRBY" {
            let _type = iter.next().expect("bitfield 缺失INCR type");
            let offset = iter.next().expect("bitfield 缺失INCR offset");
            let increment = iter.next().expect("bitfield 缺失INCR offset");
            statements.push(Operation::INCRBY(IncrBy {
                _type,
                offset,
                increment,
            }));
        } else if arg_upper == "OVERFLOW" {
            let _type = String::from_utf8_lossy(iter.next().expect("bitfield 缺失OVERFLOW type"));
            let type_upper = &_type.to_uppercase();
            if type_upper == "FAIL" {
                overflows.push(Overflow::FAIL);
            } else if type_upper == "SAT" {
                overflows.push(Overflow::SAT);
            } else if type_upper == "WRAP" {
                overflows.push(Overflow::WRAP);
            }
        }
    }

    let _statements;
    if statements.is_empty() {
        _statements = None;
    } else {
        _statements = Some(statements);
    }
    let _overflows;
    if overflows.is_empty() {
        _overflows = None;
    } else {
        _overflows = Some(overflows);
    }
    BITFIELD {
        key,
        statements: _statements,
        overflows: _overflows,
    }
}

#[derive(Debug)]
pub struct BITOP<'a> {
    pub operation: Op,
    pub dest_key: &'a [u8],
    pub keys: Vec<&'a Vec<u8>>,
}

#[derive(Debug)]
pub enum Op {
    AND,
    OR,
    XOR,
    NOT,
}

pub(crate) fn parse_bitop(mut iter: Iter<Vec<u8>>) -> BITOP {
    let operation;
    let op = String::from_utf8_lossy(iter.next().unwrap()).to_uppercase();
    if &op == "AND" {
        operation = AND;
    } else if &op == "OR" {
        operation = OR;
    } else if &op == "XOR" {
        operation = XOR;
    } else if &op == "NOT" {
        operation = NOT;
    } else {
        panic!("bitop命令缺失operation")
    }
    let dest_key = iter.next().unwrap();

    let mut keys = Vec::new();
    while let Some(next_arg) = iter.next() {
        keys.push(next_arg);
    }
    if keys.is_empty() {
        panic!("bitop命令缺失input key")
    }
    BITOP {
        operation,
        dest_key,
        keys,
    }
}

#[derive(Debug)]
pub struct SET<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub expire: Option<(ExpireType, &'a Vec<u8>)>,
    pub exist_type: Option<ExistType>,
    pub keep_ttl: Option<bool>,
}

#[derive(Debug)]
pub enum ExpireType {
    // seconds -- Set the specified expire time, in seconds.
    EX,
    // milliseconds -- Set the specified expire time, in milliseconds.
    PX,
}

#[derive(Debug)]
pub enum ExistType {
    // Only set the key if it does not already exist.
    NX,
    // Only set the key if it already exist.
    XX,
}

pub(crate) fn parse_set(mut iter: Iter<Vec<u8>>) -> SET {
    let key = iter.next().unwrap();

    let value = iter.next().unwrap();

    let mut expire_time = None;
    let mut expire_type = None;
    let mut exist_type = None;
    let mut expire = None;
    let mut keep_ttl = None;
    
    for arg in iter {
        let arg_string = String::from_utf8_lossy(arg);
        let p_arg = &arg_string.to_uppercase();
        if p_arg == "EX" {
            expire_type = Some(ExpireType::EX);
        } else if p_arg == "PX" {
            expire_type = Some(ExpireType::PX);
        } else if p_arg == "NX" {
            exist_type = Some(ExistType::NX);
        } else if p_arg == "XX" {
            exist_type = Some(ExistType::XX);
        } else if p_arg == "KEEPTTL" {
            keep_ttl = Some(true)
        }else {
            // 读取过期时间
            expire_time = Some(arg);
        }
    }
    if expire_type.is_some() && expire_time.is_some() {
        expire = Some((expire_type.unwrap(), expire_time.unwrap()));
    }
    SET {
        key,
        value,
        exist_type,
        expire,
        keep_ttl
    }
}

#[derive(Debug)]
pub struct SETEX<'a> {
    pub key: &'a [u8],
    pub seconds: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_setex(mut iter: Iter<Vec<u8>>) -> SETEX {
    let key = iter.next().unwrap();
    let seconds = iter.next().unwrap();
    let value = iter.next().unwrap();
    SETEX {
        key,
        seconds,
        value,
    }
}

#[derive(Debug)]
pub struct SETNX<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_setnx(mut iter: Iter<Vec<u8>>) -> SETNX {
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();
    SETNX { key, value }
}

#[derive(Debug)]
pub struct PSETEX<'a> {
    pub key: &'a [u8],
    pub milliseconds: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_psetex(mut iter: Iter<Vec<u8>>) -> PSETEX {
    let key = iter.next().unwrap();
    let milliseconds = iter.next().unwrap();
    let value = iter.next().unwrap();
    PSETEX {
        key,
        milliseconds,
        value,
    }
}

#[derive(Debug)]
pub struct SETRANGE<'a> {
    pub key: &'a [u8],
    pub offset: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_setrange(mut iter: Iter<Vec<u8>>) -> SETRANGE {
    let key = iter.next().unwrap();
    let offset = iter.next().unwrap();
    let value = iter.next().unwrap();
    SETRANGE { key, offset, value }
}

#[derive(Debug)]
pub struct DECR<'a> {
    pub key: &'a [u8],
}

pub(crate) fn parse_decr(mut iter: Iter<Vec<u8>>) -> DECR {
    let key = iter.next().unwrap();
    DECR { key }
}

#[derive(Debug)]
pub struct DECRBY<'a> {
    pub key: &'a [u8],
    pub decrement: &'a [u8],
}

pub(crate) fn parse_decrby(mut iter: Iter<Vec<u8>>) -> DECRBY {
    let key = iter.next().unwrap();
    let decrement = iter.next().unwrap();
    DECRBY { key, decrement }
}

#[derive(Debug)]
pub struct INCR<'a> {
    pub key: &'a [u8],
}

pub(crate) fn parse_incr(mut iter: Iter<Vec<u8>>) -> INCR {
    let key = iter.next().unwrap();
    INCR { key }
}

#[derive(Debug)]
pub struct INCRBY<'a> {
    pub key: &'a [u8],
    pub increment: &'a [u8],
}

pub(crate) fn parse_incrby(mut iter: Iter<Vec<u8>>) -> INCRBY {
    let key = iter.next().unwrap();
    let increment = iter.next().unwrap();
    INCRBY { key, increment }
}

#[derive(Debug)]
pub struct KeyValue<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Debug)]
pub struct MSET<'a> {
    pub key_values: Vec<KeyValue<'a>>,
}

pub(crate) fn parse_mset(mut iter: Iter<Vec<u8>>) -> MSET {
    let mut key_values = Vec::new();
    while let Some(key) = iter.next() {
        if let Some(value) = iter.next() {
            key_values.push(KeyValue { key, value });
        }
    }
    if key_values.is_empty() {
        panic!("mset命令缺失key value");
    }
    MSET { key_values }
}

#[derive(Debug)]
pub struct MSETNX<'a> {
    pub key_values: Vec<KeyValue<'a>>,
}

pub(crate) fn parse_msetnx(mut iter: Iter<Vec<u8>>) -> MSETNX {
    let mut key_values = Vec::new();
    while let Some(key) = iter.next() {
        if let Some(value) = iter.next() {
            key_values.push(KeyValue { key, value });
        }
    }
    if key_values.is_empty() {
        panic!("msetnx命令缺失key value");
    }
    MSETNX { key_values }
}

#[derive(Debug)]
pub struct SETBIT<'a> {
    pub key: &'a [u8],
    pub offset: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_setbit(mut iter: Iter<Vec<u8>>) -> SETBIT {
    let key = iter.next().unwrap();
    let offset = iter.next().unwrap();
    let value = iter.next().unwrap();
    SETBIT { key, value, offset }
}

#[derive(Debug)]
pub struct GETSET<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_getset(mut iter: Iter<Vec<u8>>) -> GETSET {
    let key = iter.next().unwrap();
    let value = iter.next().unwrap();
    GETSET { key, value }
}
