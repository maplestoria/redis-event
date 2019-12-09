use core::slice;
use std::io::Result;

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

pub(crate) fn parse_set(mut iter: slice::Iter<Vec<u8>>) -> Result<SET> {
    let key = iter.next();
    let key = String::from_utf8(key.unwrap().to_vec())
        .expect("UTF-8转换错误");
    
    let value = iter.next();
    let value = String::from_utf8(value.unwrap().to_vec())
        .expect("UTF-8转换错误");
    
    let mut expire_time = None;
    let mut expire_type = None;
    let mut exist_type = None;
    
    for arg in iter {
        let arg = String::from_utf8(arg.to_vec()).unwrap();
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
    
    Ok(SET { key, value, expire_type, exist_type, expire_time })
}

#[derive(Debug)]
pub struct SETEX {
    pub key: String,
    pub seconds: i64,
    pub value: String,
}

pub(crate) fn parse_setex(iter: slice::Iter<Vec<u8>>) -> Result<SETEX> {
    let args = iter.as_slice();
    if args.len() != 3 {
        panic!("invalid setnx args len");
    }
    let key = String::from_utf8(args[0].to_vec())
        .expect("UTF-8转换错误");
    let seconds = String::from_utf8(args[1].to_vec())
        .expect("UTF-8转换错误");
    let seconds = seconds.parse::<i64>()
        .expect("解析setex命令时间参数错误");
    let value = String::from_utf8(args[2].to_vec())
        .expect("UTF-8转换错误");
    Ok(SETEX { key, seconds, value })
}

#[derive(Debug)]
pub struct SETNX {
    pub key: String,
    pub value: String,
}

pub(crate) fn parse_setnx(iter: slice::Iter<Vec<u8>>) -> Result<SETNX> {
    let args = iter.as_slice();
    if args.len() != 2 {
        panic!("invalid setnx args len");
    }
    let key = String::from_utf8(args[0].to_vec())
        .expect("UTF-8转换错误");
    let value = String::from_utf8(args[1].to_vec())
        .expect("UTF-8转换错误");
    Ok(SETNX { key, value })
}

#[derive(Debug)]
pub struct PSETEX {
    pub key: String,
    pub milliseconds: i64,
    pub value: String,
}

pub(crate) fn parse_psetex(iter: slice::Iter<Vec<u8>>) -> Result<PSETEX> {
    let args = iter.as_slice();
    if args.len() != 3 {
        panic!("invalid psetex args len");
    }
    let key = String::from_utf8(args[0].to_vec())
        .expect("UTF-8转换错误");
    let milliseconds = String::from_utf8(args[1].to_vec())
        .expect("UTF-8转换错误");
    let milliseconds = milliseconds.parse::<i64>()
        .expect("解析psetex命令时间参数错误");
    let value = String::from_utf8(args[2].to_vec())
        .expect("UTF-8转换错误");
    Ok(PSETEX { key, milliseconds, value })
}