/*!
Scripting相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#scripting
*/

use std::slice::Iter;

#[derive(Debug)]
pub struct EVAL<'a> {
    pub script: &'a [u8],
    pub num_keys: i32,
    pub keys: Vec<&'a [u8]>,
    pub args: Vec<&'a [u8]>,
}

pub(crate) fn parse_eval(mut iter: Iter<Vec<u8>>) -> EVAL {
    let script = iter.next().unwrap();
    let num_keys = iter.next().unwrap();
    let num_keys = String::from_utf8_lossy(num_keys).parse::<i32>()
        .unwrap();
    let mut keys = Vec::with_capacity(num_keys as usize);
    for _ in 0..num_keys {
        let key = iter.next().unwrap();
        keys.push(key.as_slice());
    }
    let mut args = Vec::new();
    while let Some(arg) = iter.next() {
        args.push(arg.as_slice());
    }
    EVAL { script, num_keys, keys, args }
}

#[derive(Debug)]
pub struct EVALSHA<'a> {
    pub sha1: &'a [u8],
    pub num_keys: i32,
    pub keys: Vec<&'a [u8]>,
    pub args: Vec<&'a [u8]>,
}

pub(crate) fn parse_evalsha(mut iter: Iter<Vec<u8>>) -> EVALSHA {
    let sha1 = iter.next().unwrap();
    let num_keys = iter.next().unwrap();
    let num_keys = String::from_utf8_lossy(num_keys).parse::<i32>()
        .unwrap();
    let mut keys = Vec::with_capacity(num_keys as usize);
    for _ in 0..num_keys {
        let key = iter.next().unwrap();
        keys.push(key.as_slice());
    }
    let mut args = Vec::new();
    while let Some(arg) = iter.next() {
        args.push(arg.as_slice());
    }
    EVALSHA { sha1, num_keys, keys, args }
}

#[derive(Debug)]
pub struct SCRIPTLOAD<'a> {
    pub script: &'a [u8]
}

pub(crate) fn parse_script_load(mut iter: Iter<Vec<u8>>) -> SCRIPTLOAD {
    let script = iter.next().unwrap();
    SCRIPTLOAD { script }
}
