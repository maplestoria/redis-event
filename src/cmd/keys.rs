use std::slice::Iter;

/// 这个模块处理keys相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#generic
///
#[derive(Debug)]
pub struct DEL<'a> {
    pub keys: Vec<&'a Vec<u8>>
}

pub(crate) fn parse_del(mut iter: Iter<Vec<u8>>) -> DEL {
    let mut keys = Vec::new();
    for next_key in iter {
        keys.push(next_key);
    }
    DEL { keys }
}