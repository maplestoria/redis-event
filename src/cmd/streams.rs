/*!
Stream相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#stream
*/

use core::slice::Iter;

use crate::cmd::hashes::Field;

#[derive(Debug)]
pub struct XACK<'a> {
    pub key: &'a [u8],
    pub group: &'a [u8],
    pub ids: Vec<&'a Vec<u8>>,
}

pub(crate) fn parse_xack(mut iter: Iter<Vec<u8>>) -> XACK {
    let key = iter.next().unwrap();
    let group = iter.next().unwrap();
    let mut ids = Vec::new();
    for id in iter {
        ids.push(id);
    }
    XACK { key, group, ids }
}

#[derive(Debug)]
pub struct XADD<'a> {
    pub key: &'a [u8],
    pub id: &'a [u8],
    pub fields: Vec<Field<'a>>,
}

pub(crate) fn parse_xadd(mut iter: Iter<Vec<u8>>) -> XADD {
    let key = iter.next().unwrap();
    let id = iter.next().unwrap();
    let mut fields = Vec::new();
    loop {
        if let Some(field) = iter.next() {
            if let Some(value) = iter.next() {
                let field = Field { name: field, value };
                fields.push(field);
            } else {
                panic!("XADD缺失field value");
            }
        } else {
            break;
        }
    }
    XADD { key, id, fields }
}

#[derive(Debug)]
pub struct XCLAIM<'a> {
    pub key: &'a [u8],
    pub group: &'a [u8],
    pub consumer: &'a [u8],
    pub min_idle_time: &'a [u8],
    pub ids: Vec<&'a Vec<u8>>,
    pub idle: Option<&'a Vec<u8>>,
    pub time: Option<&'a Vec<u8>>,
    pub retry_count: Option<&'a Vec<u8>>,
    pub force: Option<bool>,
    pub just_id: Option<bool>,
}

pub(crate) fn parse_xclaim(mut iter: Iter<Vec<u8>>) -> XCLAIM {
    let key = iter.next().unwrap();
    let group = iter.next().unwrap();
    let consumer = iter.next().unwrap();
    let min_idle_time = iter.next().unwrap();
    let mut ids = Vec::new();
    let id = iter.next().unwrap();
    ids.push(id);
    let mut idle = None;
    let mut time = None;
    let mut retry_count = None;
    let mut force = None;
    let mut just_id = None;
    for arg in iter.next() {
        let arg_string = String::from_utf8_lossy(arg);
        let p_arg = &arg_string.to_uppercase();
        if p_arg == "IDLE" {
            let _idle = iter.next().unwrap();
            idle = Some(_idle);
        } else if p_arg == "TIME" {
            let _time = iter.next().unwrap();
            time = Some(_time);
        } else if p_arg == "RETRYCOUNT" {
            let _retry_count = iter.next().unwrap();
            retry_count = Some(_retry_count);
        } else if p_arg == "FORCE" {
            force = Some(true);
        } else if p_arg == "JUSTID" {
            just_id = Some(true);
        } else {
            ids.push(arg);
        }
    }
    XCLAIM {
        key,
        group,
        consumer,
        min_idle_time,
        ids,
        idle,
        time,
        retry_count,
        force,
        just_id,
    }
}

#[derive(Debug)]
pub struct XDEL<'a> {
    pub key: &'a [u8],
    pub ids: Vec<&'a Vec<u8>>,
}

pub(crate) fn parse_xdel(mut iter: Iter<Vec<u8>>) -> XDEL {
    let key = iter.next().unwrap();
    let mut ids = Vec::new();
    for id in iter {
        ids.push(id);
    }
    XDEL { key, ids }
}

#[derive(Debug)]
pub struct XGROUP<'a> {
    pub create: Option<Create<'a>>,
    pub set_id: Option<SetID<'a>>,
    pub destroy: Option<Destroy<'a>>,
    pub del_consumer: Option<DelConsumer<'a>>,
}

#[derive(Debug)]
pub struct Create<'a> {
    pub key: &'a [u8],
    pub group_name: &'a [u8],
    pub id: &'a [u8],
}

#[derive(Debug)]
pub struct SetID<'a> {
    pub key: &'a [u8],
    pub group_name: &'a [u8],
    pub id: &'a [u8],
}

#[derive(Debug)]
pub struct Destroy<'a> {
    pub key: &'a [u8],
    pub group_name: &'a [u8],
}

#[derive(Debug)]
pub struct DelConsumer<'a> {
    pub key: &'a [u8],
    pub group_name: &'a [u8],
    pub consumer_name: &'a [u8],
}

pub(crate) fn parse_xgroup(mut iter: Iter<Vec<u8>>) -> XGROUP {
    let mut create = None;
    let mut set_id = None;
    let mut destroy = None;
    let mut del_consumer = None;
    for arg in iter.next() {
        let arg_string = String::from_utf8_lossy(arg);
        let p_arg = &arg_string.to_uppercase();
        if p_arg == "CREATE" {
            let key = iter.next().unwrap();
            let group_name = iter.next().unwrap();
            let id = iter.next().unwrap();
            create = Some(Create {
                key,
                group_name,
                id,
            })
        } else if p_arg == "SETID" {
            let key = iter.next().unwrap();
            let group_name = iter.next().unwrap();
            let id = iter.next().unwrap();
            set_id = Some(SetID {
                key,
                group_name,
                id,
            })
        } else if p_arg == "DESTROY" {
            let key = iter.next().unwrap();
            let group_name = iter.next().unwrap();
            destroy = Some(Destroy { key, group_name })
        } else if p_arg == "DELCONSUMER" {
            let key = iter.next().unwrap();
            let group_name = iter.next().unwrap();
            let consumer_name = iter.next().unwrap();
            del_consumer = Some(DelConsumer {
                key,
                group_name,
                consumer_name,
            })
        }
    }
    XGROUP {
        create,
        set_id,
        destroy,
        del_consumer,
    }
}

#[derive(Debug)]
pub struct XTRIM<'a> {
    pub key: &'a [u8],
    pub approximation: bool,
    pub count: u64,
}

pub(crate) fn parse_xtrim(mut iter: Iter<Vec<u8>>) -> XTRIM {
    let key = iter.next().unwrap();
    iter.next().unwrap();
    let third = iter.next().unwrap();
    let third = String::from_utf8_lossy(third);
    let approximation;
    let count;
    if "~" == third {
        approximation = true;
        let arg = String::from_utf8_lossy(iter.next().unwrap());
        count = arg.parse::<u64>().unwrap();
    } else {
        approximation = false;
        count = third.parse::<u64>().unwrap();
    }
    XTRIM {
        key,
        approximation,
        count,
    }
}
