use std::slice::Iter;

#[derive(Debug)]
pub struct SINTERSTORE<'a> {
    pub dest: &'a [u8],
    pub keys: Vec<&'a Vec<u8>>,
}

pub(crate) fn parse_sinterstore(mut iter: Iter<Vec<u8>>) -> SINTERSTORE {
    let dest = iter.next().unwrap();
    let mut keys = Vec::new();
    for next_arg in iter {
        keys.push(next_arg);
    }
    SINTERSTORE { dest, keys }
}

#[derive(Debug)]
pub struct SADD<'a> {
    pub key: &'a [u8],
    pub members: Vec<&'a [u8]>,
}

pub(crate) fn parse_sadd(mut iter: Iter<Vec<u8>>) -> SADD {
    let key = iter.next().unwrap();
    let mut members = Vec::new();
    while let Some(member) = iter.next() {
        members.push(member.as_slice());
    }
    SADD { key, members }
}

#[derive(Debug)]
pub struct SDIFFSTORE<'a> {
    pub destination: &'a [u8],
    pub keys: Vec<&'a [u8]>,
}

pub(crate) fn parse_sdiffstore(mut iter: Iter<Vec<u8>>) -> SDIFFSTORE {
    let destination = iter.next().unwrap();
    let mut keys = Vec::new();
    while let Some(key) = iter.next() {
        keys.push(key.as_slice());
    }
    SDIFFSTORE { destination, keys }
}

#[derive(Debug)]
pub struct SMOVE<'a> {
    pub source: &'a [u8],
    pub destination: &'a [u8],
    pub member: &'a [u8],
}

pub(crate) fn parse_smove(mut iter: Iter<Vec<u8>>) -> SMOVE {
    let source = iter.next().unwrap();
    let destination = iter.next().unwrap();
    let member = iter.next().unwrap();
    SMOVE { source, destination, member }
}

#[derive(Debug)]
pub struct SREM<'a> {
    pub key: &'a [u8],
    pub members: Vec<&'a [u8]>,
}

pub(crate) fn parse_srem(mut iter: Iter<Vec<u8>>) -> SREM {
    let key = iter.next().unwrap();
    let mut members = Vec::new();
    while let Some(member) = iter.next() {
        members.push(member.as_slice());
    }
    SREM { key, members }
}