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
        .expect("expect key of SET command");
    
    let value = iter.next();
    let value = String::from_utf8(value.unwrap().to_vec())
        .expect("expect value of SET command");
    
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

pub struct SETEX {}

pub struct SETNX {}

pub struct PSETEX {}