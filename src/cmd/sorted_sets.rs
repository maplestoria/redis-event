use std::slice::Iter;

use crate::cmd::strings::ExistType;
use crate::cmd::strings::ExistType::{NX, XX};

/// 这个模块处理sorted sets相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#sorted_set
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
    pub element: &'a [u8],
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
            let element = iter.next().unwrap();
            items.push(Item { score: next_arg, element });
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