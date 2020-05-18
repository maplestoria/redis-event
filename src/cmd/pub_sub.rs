/*!
Pub/Sub相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#pubsub
*/

use std::slice::Iter;

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
