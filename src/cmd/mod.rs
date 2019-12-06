use std::io::Result;

use crate::cmd::set::SET;
use crate::CommandHandler;

pub mod set;

pub enum Command<'a> {
    SET(&'a SET),
    PING,
    SELECT(u8),
}

pub(crate) fn parse(data: Vec<Vec<u8>>, cmd_handler: &Vec<Box<dyn CommandHandler>>) -> Result<()> {
    let mut iter = data.iter();
    if let Some(cmd_name) = iter.next() {
        let cmd_name = String::from_utf8_lossy(cmd_name).to_uppercase();
        match cmd_name.as_str() {
            "SET" => {
                let cmd = set::parse_set(iter)?;
                cmd_handler.iter().for_each(|handler|
                    handler.handle(&Command::SET(&cmd))
                );
            }
            "PING" => {
                cmd_handler.iter().for_each(|handler|
                    handler.handle(&Command::PING)
                );
            }
            _ => {}
        };
    } else {
        // return error
    }
    Ok(())
}