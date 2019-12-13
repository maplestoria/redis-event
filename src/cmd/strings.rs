use core::slice::Iter;

use crate::to_string;

/// 这个模块处理Strings相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#string
///
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
    loop {
        if let Some(next_arg) = iter.next() {
            let arg = to_string(next_arg.to_vec());
            let arg_upper = &arg.to_uppercase();
            if arg_upper == "GET" {
                let _type = iter.next()
                    .expect("bitfield 缺失get type");
                let offset = iter.next()
                    .expect("bitfield 缺失get offset");
                statements.push(Operation::GET(Get { _type, offset }));
            } else if arg_upper == "SET" {
                let _type = iter.next().unwrap();
                let offset = iter.next()
                    .expect("bitfield 缺失SET offset");
                let value = iter.next()
                    .expect("bitfield 缺失SET offset");
                statements.push(Operation::SET(Set { _type, offset, value }));
            } else if arg_upper == "INCRBY" {
                let _type = iter.next()
                    .expect("bitfield 缺失INCR type");
                let offset = iter.next()
                    .expect("bitfield 缺失INCR offset");
                let increment = iter.next()
                    .expect("bitfield 缺失INCR offset");
                statements.push(Operation::INCRBY(IncrBy { _type, offset, increment }));
            } else if arg_upper == "OVERFLOW" {
                let _type = to_string(iter.next()
                    .expect("bitfield 缺失OVERFLOW type")
                    .to_vec());
                let type_upper = &_type.to_uppercase();
                if type_upper == "FAIL" {
                    overflows.push(Overflow::FAIL);
                } else if type_upper == "SAT" {
                    overflows.push(Overflow::SAT);
                } else if type_upper == "WRAP" {
                    overflows.push(Overflow::WRAP);
                }
            }
        } else {
            break;
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
    BITFIELD { key, statements: _statements, overflows: _overflows }
}

#[derive(Debug)]
pub struct SET<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub expire_type: Option<ExpireType>,
    pub expire_time: Option<&'a Vec<u8>>,
    pub exist_type: Option<ExistType>,
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
    
    for arg in iter {
        let arg_string = to_string(arg.to_vec());
        let p_arg = arg_string.as_str();
        if p_arg == "EX" {
            expire_type = Some(ExpireType::EX);
        } else if p_arg == "PX" {
            expire_type = Some(ExpireType::PX);
        } else if p_arg == "NX" {
            exist_type = Some(ExistType::NX);
        } else if p_arg == "XX" {
            exist_type = Some(ExistType::XX);
        } else {
            // 读取过期时间
            expire_time = Some(arg);
        }
    }
    
    SET { key, value, expire_type, exist_type, expire_time }
}

#[derive(Debug)]
pub struct SETEX<'a> {
    pub key: &'a [u8],
    pub seconds: i64,
    pub value: &'a [u8],
}

pub(crate) fn parse_setex(iter: Iter<Vec<u8>>) -> SETEX {
    let args = iter.as_slice();
    if args.len() != 3 {
        panic!("invalid setnx args len");
    }
    let key = args.get(0).unwrap();
    let seconds = to_string(args[1].to_vec());
    let seconds = seconds.parse::<i64>()
        .expect("解析setex命令时间参数错误");
    let value = args.get(2).unwrap();
    SETEX { key, seconds, value }
}

#[derive(Debug)]
pub struct SETNX<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_setnx(iter: Iter<Vec<u8>>) -> SETNX {
    let args = iter.as_slice();
    if args.len() != 2 {
        panic!("invalid setnx args len");
    }
    let key = args.get(0).unwrap();
    let value = args.get(1).unwrap();
    SETNX { key, value }
}

#[derive(Debug)]
pub struct PSETEX<'a> {
    pub key: &'a [u8],
    pub milliseconds: i64,
    pub value: &'a [u8],
}

pub(crate) fn parse_psetex(iter: Iter<Vec<u8>>) -> PSETEX {
    let args = iter.as_slice();
    if args.len() != 3 {
        panic!("invalid psetex args len");
    }
    let key = args.get(0).unwrap();
    let milliseconds = to_string(args[1].to_vec());
    let milliseconds = milliseconds.parse::<i64>()
        .expect("解析psetex命令时间参数错误");
    let value = args.get(2).unwrap();
    PSETEX { key, milliseconds, value }
}