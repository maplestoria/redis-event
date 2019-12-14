use crate::cmd::keys::*;
use crate::cmd::strings::*;
use crate::CommandHandler;

pub mod strings;
pub mod keys;

#[derive(Debug)]
pub enum Command<'a> {
    APPEND(&'a APPEND<'a>),
    BITFIELD(&'a BITFIELD<'a>),
    BITOP(&'a BITOP<'a>),
    DEL(&'a DEL<'a>),
    SET(&'a SET<'a>),
    SETEX(&'a SETEX<'a>),
    SETNX(&'a SETNX<'a>),
    PSETEX(&'a PSETEX<'a>),
    SETRANGE(&'a SETRANGE<'a>),
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
            "SET" => {
                let cmd = strings::parse_set(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SET(&cmd))
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
            "PING" => cmd_handler.iter().for_each(|handler|
                handler.handle(Command::PING)),
            _ => {}
        };
    } else {
        // command not found
    }
}