/*!
Sorted Sets相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#sorted_set
*/

use std::slice::Iter;

use crate::cmd::sorted_sets::AGGREGATE::{MAX, MIN, SUM};
use crate::cmd::strings::ExistType;
use crate::cmd::strings::ExistType::{NX, XX};

#[derive(Debug)]
pub struct ZADD<'a> {
    pub key: &'a [u8],
    /// XX: 只更新现有的元素，不添加新的元素.
    /// NX: 只添加新的元素，不更新现有的元素.
    pub exist_type: Option<ExistType>,
    pub ch: Option<bool>,
    pub incr: Option<bool>,
    pub items: Vec<Item<'a>>,
}

#[derive(Debug)]
pub struct Item<'a> {
    pub score: &'a [u8],
    pub member: &'a [u8],
}

pub(crate) fn parse_zadd(mut iter: Iter<Vec<u8>>) -> ZADD {
    let key = iter.next().unwrap();
    let mut exist_type = None;
    let mut ch = None;
    let mut incr = None;
    let mut items = Vec::new();
    while let Some(next_arg) = iter.next() {
        let arg_upper = String::from_utf8_lossy(next_arg).to_uppercase();
        if &arg_upper == "NX" {
            exist_type = Some(NX);
        } else if &arg_upper == "XX" {
            exist_type = Some(XX);
        } else if &arg_upper == "CH" {
            ch = Some(true);
        } else if &arg_upper == "INCR" {
            incr = Some(true);
        } else {
            // score在前，element在后
            let member = iter.next().unwrap();
            items.push(Item { score: next_arg, member });
        }
    }
    ZADD {
        key,
        exist_type,
        ch,
        incr,
        items,
    }
}

#[derive(Debug)]
pub struct ZINCRBY<'a> {
    pub key: &'a [u8],
    pub increment: &'a [u8],
    pub member: &'a [u8],
}

pub(crate) fn parse_zincrby(mut iter: Iter<Vec<u8>>) -> ZINCRBY {
    let key = iter.next().unwrap();
    let increment = iter.next().unwrap();
    let member = iter.next().unwrap();
    ZINCRBY {
        key,
        increment,
        member,
    }
}

#[derive(Debug)]
pub struct ZINTERSTORE<'a> {
    pub destination: &'a [u8],
    pub num_keys: i32,
    pub keys: Vec<&'a [u8]>,
    pub weights: Option<Vec<&'a [u8]>>,
    pub aggregate: Option<AGGREGATE>,
}

#[derive(Debug)]
pub enum AGGREGATE {
    SUM,
    MIN,
    MAX,
}

pub(crate) fn parse_zinterstore(mut iter: Iter<Vec<u8>>) -> ZINTERSTORE {
    let destination = iter.next().unwrap();
    let num_keys = String::from_utf8_lossy(iter.next().unwrap());
    let num_keys = num_keys.parse::<i32>().unwrap();
    let mut keys = Vec::new();
    for _ in 0..num_keys {
        let next_key = iter.next().unwrap();
        keys.push(next_key.as_slice());
    }
    let mut _weights = Vec::new();
    let mut aggregate = None;
    while let Some(next_arg) = iter.next() {
        let arg_upper = String::from_utf8_lossy(next_arg).to_uppercase();
        if &arg_upper == "WEIGHTS" || &arg_upper == "AGGREGATE" {
            continue;
        } else if &arg_upper == "SUM" {
            aggregate = Some(SUM);
        } else if &arg_upper == "MIN" {
            aggregate = Some(MIN);
        } else if &arg_upper == "MAX" {
            aggregate = Some(MAX);
        } else {
            _weights.push(next_arg.as_slice());
        }
    }
    let weights;
    if _weights.is_empty() {
        weights = None;
    } else {
        weights = Some(_weights);
    }
    ZINTERSTORE {
        destination,
        num_keys,
        keys,
        weights,
        aggregate,
    }
}

#[derive(Debug)]
pub struct ZPOPMAX<'a> {
    pub key: &'a [u8],
    pub count: Option<&'a [u8]>,
}

pub(crate) fn parse_zpopmax(mut iter: Iter<Vec<u8>>) -> ZPOPMAX {
    let key = iter.next().unwrap();
    let mut count = None;
    if let Some(next_arg) = iter.next() {
        count = Some(next_arg.as_slice());
    }
    ZPOPMAX { key, count }
}

#[derive(Debug)]
pub struct ZPOPMIN<'a> {
    pub key: &'a [u8],
    pub count: Option<&'a [u8]>,
}

pub(crate) fn parse_zpopmin(mut iter: Iter<Vec<u8>>) -> ZPOPMIN {
    let key = iter.next().unwrap();
    let mut count = None;
    if let Some(next_arg) = iter.next() {
        count = Some(next_arg.as_slice());
    }
    ZPOPMIN { key, count }
}

#[derive(Debug)]
pub struct ZREM<'a> {
    pub key: &'a [u8],
    pub members: Vec<&'a [u8]>,
}

pub(crate) fn parse_zrem(mut iter: Iter<Vec<u8>>) -> ZREM {
    let key = iter.next().unwrap();
    let mut members = Vec::new();
    while let Some(next_arg) = iter.next() {
        members.push(next_arg.as_slice());
    }
    ZREM { key, members }
}

#[derive(Debug)]
pub struct ZREMRANGEBYLEX<'a> {
    pub key: &'a [u8],
    pub min: &'a [u8],
    pub max: &'a [u8],
}

pub(crate) fn parse_zremrangebylex(mut iter: Iter<Vec<u8>>) -> ZREMRANGEBYLEX {
    let key = iter.next().unwrap();
    let min = iter.next().unwrap();
    let max = iter.next().unwrap();
    ZREMRANGEBYLEX { key, min, max }
}

#[derive(Debug)]
pub struct ZREMRANGEBYRANK<'a> {
    pub key: &'a [u8],
    pub start: &'a [u8],
    pub stop: &'a [u8],
}

pub(crate) fn parse_zremrangebyrank(mut iter: Iter<Vec<u8>>) -> ZREMRANGEBYRANK {
    let key = iter.next().unwrap();
    let start = iter.next().unwrap();
    let stop = iter.next().unwrap();
    ZREMRANGEBYRANK { key, start, stop }
}

#[derive(Debug)]
pub struct ZREMRANGEBYSCORE<'a> {
    pub key: &'a [u8],
    pub min: &'a [u8],
    pub max: &'a [u8],
}

pub(crate) fn parse_zremrangebyscore(mut iter: Iter<Vec<u8>>) -> ZREMRANGEBYSCORE {
    let key = iter.next().unwrap();
    let min = iter.next().unwrap();
    let max = iter.next().unwrap();
    ZREMRANGEBYSCORE { key, min, max }
}

#[derive(Debug)]
pub struct ZUNIONSTORE<'a> {
    pub destination: &'a [u8],
    pub num_keys: i32,
    pub keys: Vec<&'a [u8]>,
    pub weights: Option<Vec<&'a [u8]>>,
    pub aggregate: Option<AGGREGATE>,
}

pub(crate) fn parse_zunionstore(mut iter: Iter<Vec<u8>>) -> ZUNIONSTORE {
    let destination = iter.next().unwrap();
    let num_keys = String::from_utf8_lossy(iter.next().unwrap());
    let num_keys = num_keys.parse::<i32>().unwrap();
    let mut keys = Vec::new();
    for _ in 0..num_keys {
        let next_key = iter.next().unwrap();
        keys.push(next_key.as_slice());
    }
    let mut _weights = Vec::new();
    let mut aggregate = None;
    while let Some(next_arg) = iter.next() {
        let arg_upper = String::from_utf8_lossy(next_arg).to_uppercase();
        if &arg_upper == "WEIGHTS" || &arg_upper == "AGGREGATE" {
            continue;
        } else if &arg_upper == "SUM" {
            aggregate = Some(SUM);
        } else if &arg_upper == "MIN" {
            aggregate = Some(MIN);
        } else if &arg_upper == "MAX" {
            aggregate = Some(MAX);
        } else {
            _weights.push(next_arg.as_slice());
        }
    }
    let weights;
    if _weights.is_empty() {
        weights = None;
    } else {
        weights = Some(_weights);
    }
    ZUNIONSTORE {
        destination,
        num_keys,
        keys,
        weights,
        aggregate,
    }
}