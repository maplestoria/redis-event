use core::slice;

use crate::to_string;

/// 这个模块处理Strings相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#string
///
#[derive(Debug)]
pub struct APPEND {
    pub key: String,
    pub value: String,
}

pub(crate) fn parse_append(mut iter: slice::Iter<Vec<u8>>) -> APPEND {
    let key = iter.next();
    let key = to_string(key.unwrap().to_vec());
    let value = iter.next();
    let value = to_string(value.unwrap().to_vec());
    APPEND { key, value }
}

#[derive(Debug)]
pub struct SET {
    pub key: String,
    pub value: String,
    pub expire_type: Option<ExpireType>,
    pub expire_time: Option<i64>,
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

pub(crate) fn parse_set(mut iter: slice::Iter<Vec<u8>>) -> SET {
    let key = iter.next();
    let key = to_string(key.unwrap().to_vec());
    
    let value = iter.next();
    let value = to_string(value.unwrap().to_vec());
    
    let mut expire_time = None;
    let mut expire_type = None;
    let mut exist_type = None;
    
    for arg in iter {
        let arg = to_string(arg.to_vec());
        let p_arg = arg.as_str();
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
            expire_time = Some(arg.parse::<i64>().unwrap());
        }
    }
    
    SET { key, value, expire_type, exist_type, expire_time }
}

#[derive(Debug)]
pub struct SETEX {
    pub key: String,
    pub seconds: i64,
    pub value: String,
}

pub(crate) fn parse_setex(iter: slice::Iter<Vec<u8>>) -> SETEX {
    let args = iter.as_slice();
    if args.len() != 3 {
        panic!("invalid setnx args len");
    }
    let key = to_string(args[0].to_vec());
    let seconds = to_string(args[1].to_vec());
    let seconds = seconds.parse::<i64>()
        .expect("解析setex命令时间参数错误");
    let value = to_string(args[2].to_vec());
    SETEX { key, seconds, value }
}

#[derive(Debug)]
pub struct SETNX {
    pub key: String,
    pub value: String,
}

pub(crate) fn parse_setnx(iter: slice::Iter<Vec<u8>>) -> SETNX {
    let args = iter.as_slice();
    if args.len() != 2 {
        panic!("invalid setnx args len");
    }
    let key = to_string(args[0].to_vec());
    let value = to_string(args[1].to_vec());
    SETNX { key, value }
}

#[derive(Debug)]
pub struct PSETEX {
    pub key: String,
    pub milliseconds: i64,
    pub value: String,
}

pub(crate) fn parse_psetex(iter: slice::Iter<Vec<u8>>) -> PSETEX {
    let args = iter.as_slice();
    if args.len() != 3 {
        panic!("invalid psetex args len");
    }
    let key = to_string(args[0].to_vec());
    let milliseconds = to_string(args[1].to_vec());
    let milliseconds = milliseconds.parse::<i64>()
        .expect("解析psetex命令时间参数错误");
    let value = to_string(args[2].to_vec());
    PSETEX { key, milliseconds, value }
}