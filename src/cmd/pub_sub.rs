use std::slice::Iter;

/// 这个模块处理pub/sub相关的命令
/// 所有涉及到的命令参考https://redis.io/commands#pubsub
///
#[derive(Debug)]
pub struct PUBLISH<'a> {
    pub channel: &'a [u8],
    pub message: &'a [u8],
}

pub(crate) fn parse_publish(mut iter: Iter<Vec<u8>>) -> PUBLISH {
    let channel = iter.next().unwrap();
    let message = iter.next().unwrap();
    PUBLISH { channel, message }
}