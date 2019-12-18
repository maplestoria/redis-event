use crate::{CommandHandler, to_string};
use crate::cmd::keys::*;
use crate::cmd::sets::*;
use crate::cmd::strings::*;

pub mod keys;
pub mod sets;
pub mod strings;

#[derive(Debug)]
pub enum Command<'a> {
    APPEND(&'a APPEND<'a>),
    BITFIELD(&'a BITFIELD<'a>),
    BITOP(&'a BITOP<'a>),
    DECR(&'a DECR<'a>),
    DECRBY(&'a DECRBY<'a>),
    DEL(&'a DEL<'a>),
    EXPIRE(&'a EXPIRE<'a>),
    EXPIREAT(&'a EXPIREAT<'a>),
    INCR(&'a INCR<'a>),
    INCRBY(&'a INCRBY<'a>),
    MOVE(&'a MOVE<'a>),
    MSET(&'a MSET<'a>),
    MSETNX(&'a MSETNX<'a>),
    RENAME(&'a RENAME<'a>),
    RENAMENX(&'a RENAMENX<'a>),
    RESTORE(&'a RESTORE<'a>),
    SADD(&'a SADD<'a>),
    SDIFFSTORE(&'a SDIFFSTORE<'a>),
    SET(&'a SET<'a>),
    SETBIT(&'a SETBIT<'a>),
    SETEX(&'a SETEX<'a>),
    SETNX(&'a SETNX<'a>),
    SELECT(u8),
    SETRANGE(&'a SETRANGE<'a>),
    SINTERSTORE(&'a SINTERSTORE<'a>),
    SMOVE(&'a SMOVE<'a>),
    SORT(&'a SORT<'a>),
    SREM(&'a SREM<'a>),
    SUNIONSTORE(&'a SUNIONSTORE<'a>),
    PING,
    PERSIST(&'a PERSIST<'a>),
    PEXPIRE(&'a PEXPIRE<'a>),
    PEXPIREAT(&'a PEXPIREAT<'a>),
    PSETEX(&'a PSETEX<'a>),
    UNLINK(&'a UNLINK<'a>),
}

pub(crate) fn parse(data: Vec<Vec<u8>>, cmd_handler: &Vec<Box<dyn CommandHandler>>) {
    let mut iter = data.iter();
    if let Some(cmd_name) = iter.next() {
        let cmd_name = String::from_utf8_lossy(cmd_name).to_uppercase();
        match cmd_name.as_str() {
            "APPEND" => {
                let cmd = strings::parse_append(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::APPEND(&cmd))
                );
            }
            "BITFIELD" => {
                let cmd = strings::parse_bitfield(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::BITFIELD(&cmd))
                );
            }
            "BITOP" => {
                let cmd = strings::parse_bitop(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::BITOP(&cmd))
                );
            }
            "DEL" => {
                let cmd = keys::parse_del(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::DEL(&cmd))
                );
            }
            "DECR" => {
                let cmd = strings::parse_decr(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::DECR(&cmd))
                );
            }
            "DECRBY" => {
                let cmd = strings::parse_decrby(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::DECRBY(&cmd))
                );
            }
            "EXPIRE" => {
                let cmd = keys::parse_expire(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::EXPIRE(&cmd))
                );
            }
            "EXPIREAT" => {
                let cmd = keys::parse_expireat(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::EXPIREAT(&cmd))
                );
            }
            "INCR" => {
                let cmd = strings::parse_incr(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::INCR(&cmd))
                );
            }
            "INCRBY" => {
                let cmd = strings::parse_incrby(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::INCRBY(&cmd))
                );
            }
            "RENAME" => {
                let cmd = keys::parse_rename(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::RENAME(&cmd))
                );
            }
            "RENAMENX" => {
                let cmd = keys::parse_renamenx(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::RENAMENX(&cmd))
                );
            }
            "RESTORE" => {
                let cmd = keys::parse_restore(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::RESTORE(&cmd))
                );
            }
            "SADD" => {
                let cmd = sets::parse_sadd(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SADD(&cmd))
                );
            }
            "SDIFFSTORE" => {
                let cmd = sets::parse_sdiffstore(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SDIFFSTORE(&cmd))
                );
            }
            "SMOVE" => {
                let cmd = sets::parse_smove(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SMOVE(&cmd))
                );
            }
            "SET" => {
                let cmd = strings::parse_set(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SET(&cmd))
                );
            }
            "SELECT" => {
                let db = to_string(iter.next().unwrap().to_vec());
                let db = db.parse::<u8>().unwrap();
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SELECT(db))
                );
            }
            "SORT" => {
                let cmd = keys::parse_sort(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SORT(&cmd))
                );
            }
            "SREM" => {
                let cmd = sets::parse_srem(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SREM(&cmd))
                );
            }
            "SUNIONSTORE" => {
                let cmd = sets::parse_sunionstore(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SUNIONSTORE(&cmd))
                );
            }
            "UNLINK" => {
                let cmd = keys::parse_unlink(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::UNLINK(&cmd))
                );
            }
            "MOVE" => {
                let cmd = keys::parse_move(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::MOVE(&cmd))
                );
            }
            "MSET" => {
                let cmd = strings::parse_mset(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::MSET(&cmd))
                );
            }
            "MSETNX" => {
                let cmd = strings::parse_msetnx(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::MSETNX(&cmd))
                );
            }
            "SETEX" => {
                let cmd = strings::parse_setex(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SETEX(&cmd))
                );
            }
            "SETNX" => {
                let cmd = strings::parse_setnx(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SETNX(&cmd))
                );
            }
            "PSETEX" => {
                let cmd = strings::parse_psetex(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::PSETEX(&cmd))
                );
            }
            "PEXPIRE" => {
                let cmd = keys::parse_pexpire(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::PEXPIRE(&cmd))
                );
            }
            "PEXPIREAT" => {
                let cmd = keys::parse_pexpireat(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::PEXPIREAT(&cmd))
                );
            }
            "PERSIST" => {
                let cmd = keys::parse_persist(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::PERSIST(&cmd))
                );
            }
            "SETRANGE" => {
                let cmd = strings::parse_setrange(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SETRANGE(&cmd))
                );
            }
            "SETBIT" => {
                let cmd = strings::parse_setbit(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SETBIT(&cmd))
                );
            }
            "SINTERSTORE" => {
                let cmd = sets::parse_sinterstore(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SINTERSTORE(&cmd))
                );
            }
            "PING" => cmd_handler.iter().for_each(|handler|
                handler.handle(Command::PING)),
            _ => {}
        };
    } else {
        // command not found
    }
}