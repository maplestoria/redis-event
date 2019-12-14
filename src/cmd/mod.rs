use crate::cmd::keys::*;
use crate::cmd::sets::*;
use crate::cmd::strings::*;
use crate::CommandHandler;

pub mod keys;
pub mod sets;
pub mod strings;

#[derive(Debug)]
pub enum Command<'a> {
    APPEND(&'a APPEND<'a>),
    BITFIELD(&'a BITFIELD<'a>),
    BITOP(&'a BITOP<'a>),
    DEL(&'a DEL<'a>),
    DECR(&'a DECR<'a>),
    DECRBY(&'a DECRBY<'a>),
    INCR(&'a INCR<'a>),
    INCRBY(&'a INCRBY<'a>),
    SET(&'a SET<'a>),
    MSET(&'a MSET<'a>),
    MSETNX(&'a MSETNX<'a>),
    SETEX(&'a SETEX<'a>),
    SETBIT(&'a SETBIT<'a>),
    SETNX(&'a SETNX<'a>),
    PSETEX(&'a PSETEX<'a>),
    SETRANGE(&'a SETRANGE<'a>),
    SINTERSTORE(&'a SINTERSTORE<'a>),
    PING,
    SELECT(u8),
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
            "SET" => {
                let cmd = strings::parse_set(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SET(&cmd))
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