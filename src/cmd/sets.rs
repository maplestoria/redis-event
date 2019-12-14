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
