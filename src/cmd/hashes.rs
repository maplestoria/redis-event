/*!
Hashes相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#hash
*/

use std::slice::Iter;

#[derive(Debug)]
pub struct HDEL<'a> {
    pub key: &'a [u8],
    pub fields: Vec<&'a [u8]>,
}

pub(crate) fn parse_hdel(mut iter: Iter<Vec<u8>>) -> HDEL {
    let key = iter.next().unwrap();
    let mut fields = Vec::new();
    while let Some(field) = iter.next() {
        fields.push(field.as_slice());
    }
    HDEL { key, fields }
}

#[derive(Debug)]
pub struct HINCRBY<'a> {
    pub key: &'a [u8],
    pub field: &'a [u8],
    pub increment: &'a [u8],
}

pub(crate) fn parse_hincrby(mut iter: Iter<Vec<u8>>) -> HINCRBY {
    let key = iter.next().unwrap();
    let field = iter.next().unwrap();
    let increment = iter.next().unwrap();
    HINCRBY {
        key,
        field,
        increment,
    }
}

#[derive(Debug)]
pub struct HMSET<'a> {
    pub key: &'a [u8],
    pub fields: Vec<Field<'a>>,
}

#[derive(Debug)]
pub struct HSET<'a> {
    pub key: &'a [u8],
    pub fields: Vec<Field<'a>>,
}

#[derive(Debug)]
pub struct Field<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_hmset(mut iter: Iter<Vec<u8>>) -> HMSET {
    let key = iter.next().unwrap();
    let mut fields = Vec::new();
    loop {
        if let Some(field) = iter.next() {
            if let Some(value) = iter.next() {
                let field = Field { name: field, value };
                fields.push(field);
            } else {
                panic!("HMSET缺失field value");
            }
        } else {
            break;
        }
    }
    HMSET { key, fields }
}

pub(crate) fn parse_hset(mut iter: Iter<Vec<u8>>) -> HSET {
    let key = iter.next().unwrap();
    let mut fields = Vec::new();
    loop {
        if let Some(field) = iter.next() {
            if let Some(value) = iter.next() {
                let field = Field { name: field, value };
                fields.push(field);
            } else {
                panic!("HSET缺失field value");
            }
        } else {
            break;
        }
    }
    HSET { key, fields }
}

#[derive(Debug)]
pub struct HSETNX<'a> {
    pub key: &'a [u8],
    pub field: &'a [u8],
    pub value: &'a [u8],
}

pub(crate) fn parse_hsetnx(mut iter: Iter<Vec<u8>>) -> HSETNX {
    let key = iter.next().unwrap();
    let field = iter.next().unwrap();
    let value = iter.next().unwrap();
    HSETNX { key, field, value }
}
