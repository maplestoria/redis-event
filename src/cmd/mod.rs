use crate::cmd::strings::*;
use crate::CommandHandler;

pub mod strings;

#[derive(Debug)]
pub enum Command<'a> {
    APPEND(&'a APPEND),
    SET(&'a SET),
    SETEX(&'a SETEX),
    SETNX(&'a SETNX),
    PSETEX(&'a PSETEX),
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
            "PING" => cmd_handler.iter().for_each(|handler|
                handler.handle(Command::PING)),
            _ => {}
        };
    } else {
        // command not found
    }
}