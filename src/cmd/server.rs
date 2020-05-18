/*!
Server相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#server
*/

use std::slice::Iter;

#[derive(Debug)]
pub struct FLUSHDB {
    pub _async: Option<bool>,
}

pub(crate) fn parse_flushdb(mut iter: Iter<Vec<u8>>) -> FLUSHDB {
    let mut _async = None;
    if let Some(next_arg) = iter.next() {
        let arg_upper = String::from_utf8_lossy(next_arg).to_uppercase();
        if &arg_upper == "ASYNC" {
            _async = Some(true);
        } else {
            panic!("Invalid argument")
        }
    }
    FLUSHDB { _async }
}

#[derive(Debug)]
pub struct FLUSHALL {
    pub _async: Option<bool>,
}

pub(crate) fn parse_flushall(mut iter: Iter<Vec<u8>>) -> FLUSHALL {
    let mut _async = None;
    if let Some(next_arg) = iter.next() {
        let arg_upper = String::from_utf8_lossy(next_arg).to_uppercase();
        if &arg_upper == "ASYNC" {
            _async = Some(true);
        } else {
            panic!("Invalid argument")
        }
    }
    FLUSHALL { _async }
}
