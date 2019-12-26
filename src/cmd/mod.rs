use crate::cmd::connection::{SELECT, SWAPDB};
use crate::cmd::hashes::*;
use crate::cmd::hyperloglog::{PFADD, PFCOUNT, PFMERGE};
use crate::cmd::keys::*;
use crate::cmd::lists::*;
use crate::cmd::sets::*;
use crate::cmd::sorted_sets::*;
use crate::cmd::strings::*;
use crate::CommandHandler;

pub mod connection;
pub mod hashes;
pub mod hyperloglog;
pub mod keys;
pub mod lists;
pub mod sets;
pub mod sorted_sets;
pub mod strings;

#[derive(Debug)]
pub enum Command<'a> {
    APPEND(&'a APPEND<'a>),
    BITFIELD(&'a BITFIELD<'a>),
    BITOP(&'a BITOP<'a>),
    BRPOPLPUSH(&'a BRPOPLPUSH<'a>),
    DECR(&'a DECR<'a>),
    DECRBY(&'a DECRBY<'a>),
    DEL(&'a DEL<'a>),
    EXPIRE(&'a EXPIRE<'a>),
    EXPIREAT(&'a EXPIREAT<'a>),
    HDEL(&'a HDEL<'a>),
    HINCRBY(&'a HINCRBY<'a>),
    HMSET(&'a HMSET<'a>),
    HSET(&'a HSET<'a>),
    HSETNX(&'a HSETNX<'a>),
    INCR(&'a INCR<'a>),
    INCRBY(&'a INCRBY<'a>),
    LINSERT(&'a LINSERT<'a>),
    LPOP(&'a LPOP<'a>),
    LPUSH(&'a LPUSH<'a>),
    LPUSHX(&'a LPUSHX<'a>),
    LREM(&'a LREM<'a>),
    LSET(&'a LSET<'a>),
    LTRIM(&'a LTRIM<'a>),
    MOVE(&'a MOVE<'a>),
    MSET(&'a MSET<'a>),
    MSETNX(&'a MSETNX<'a>),
    PING,
    PERSIST(&'a PERSIST<'a>),
    PEXPIRE(&'a PEXPIRE<'a>),
    PEXPIREAT(&'a PEXPIREAT<'a>),
    PFADD(&'a PFADD<'a>),
    PFCOUNT(&'a PFCOUNT<'a>),
    PFMERGE(&'a PFMERGE<'a>),
    PSETEX(&'a PSETEX<'a>),
    RENAME(&'a RENAME<'a>),
    RENAMENX(&'a RENAMENX<'a>),
    RESTORE(&'a RESTORE<'a>),
    RPOP(&'a RPOP<'a>),
    RPOPLPUSH(&'a RPOPLPUSH<'a>),
    RPUSH(&'a RPUSH<'a>),
    RPUSHX(&'a RPUSHX<'a>),
    SADD(&'a SADD<'a>),
    SDIFFSTORE(&'a SDIFFSTORE<'a>),
    SET(&'a SET<'a>),
    SETBIT(&'a SETBIT<'a>),
    SETEX(&'a SETEX<'a>),
    SETNX(&'a SETNX<'a>),
    SELECT(&'a SELECT),
    SETRANGE(&'a SETRANGE<'a>),
    SINTERSTORE(&'a SINTERSTORE<'a>),
    SMOVE(&'a SMOVE<'a>),
    SORT(&'a SORT<'a>),
    SREM(&'a SREM<'a>),
    SUNIONSTORE(&'a SUNIONSTORE<'a>),
    SWAPDB(&'a SWAPDB),
    UNLINK(&'a UNLINK<'a>),
    ZADD(&'a ZADD<'a>),
    ZINCRBY(&'a ZINCRBY<'a>),
    ZINTERSTORE(&'a ZINTERSTORE<'a>),
    ZPOPMAX(&'a ZPOPMAX<'a>),
    ZPOPMIN(&'a ZPOPMIN<'a>),
    ZREM(&'a ZREM<'a>),
    ZREMRANGEBYLEX(&'a ZREMRANGEBYLEX<'a>),
    ZREMRANGEBYRANK(&'a ZREMRANGEBYRANK<'a>),
    ZREMRANGEBYSCORE(&'a ZREMRANGEBYSCORE<'a>),
    ZUNIONSTORE(&'a ZUNIONSTORE<'a>),
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
            "BRPOPLPUSH" => {
                let cmd = lists::parse_brpoplpush(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::BRPOPLPUSH(&cmd))
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
            "HDEL" => {
                let cmd = hashes::parse_hdel(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::HDEL(&cmd))
                );
            }
            "HINCRBY" => {
                let cmd = hashes::parse_hincrby(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::HINCRBY(&cmd))
                );
            }
            "HMSET" => {
                let cmd = hashes::parse_hmset(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::HMSET(&cmd))
                );
            }
            "HSET" => {
                let cmd = hashes::parse_hset(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::HSET(&cmd))
                );
            }
            "HSETNX" => {
                let cmd = hashes::parse_hsetnx(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::HSETNX(&cmd))
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
            "LINSERT" => {
                let cmd = lists::parse_linsert(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::LINSERT(&cmd))
                );
            }
            "LPOP" => {
                let cmd = lists::parse_lpop(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::LPOP(&cmd))
                );
            }
            "LPUSH" => {
                let cmd = lists::parse_lpush(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::LPUSH(&cmd))
                );
            }
            "LPUSHX" => {
                let cmd = lists::parse_lpushx(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::LPUSHX(&cmd))
                );
            }
            "LREM" => {
                let cmd = lists::parse_lrem(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::LREM(&cmd))
                );
            }
            "LSET" => {
                let cmd = lists::parse_lset(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::LSET(&cmd))
                );
            }
            "LTRIM" => {
                let cmd = lists::parse_ltrim(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::LTRIM(&cmd))
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
            "RPOP" => {
                let cmd = lists::parse_rpop(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::RPOP(&cmd))
                );
            }
            "RPOPLPUSH" => {
                let cmd = lists::parse_rpoplpush(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::RPOPLPUSH(&cmd))
                );
            }
            "RPUSH" => {
                let cmd = lists::parse_rpush(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::RPUSH(&cmd))
                );
            }
            "RPUSHX" => {
                let cmd = lists::parse_rpushx(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::RPUSHX(&cmd))
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
                let cmd = connection::parse_select(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SELECT(&cmd))
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
            "SWAPDB" => {
                let cmd = connection::parse_swapdb(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::SWAPDB(&cmd))
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
            "PFADD" => {
                let cmd = hyperloglog::parse_pfadd(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::PFADD(&cmd))
                );
            }
            "PFCOUNT" => {
                let cmd = hyperloglog::parse_pfcount(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::PFCOUNT(&cmd))
                );
            }
            "PFMERGE" => {
                let cmd = hyperloglog::parse_pfmerge(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::PFMERGE(&cmd))
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
            "ZADD" => {
                let cmd = sorted_sets::parse_zadd(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZADD(&cmd))
                );
            }
            "ZINCRBY" => {
                let cmd = sorted_sets::parse_zincrby(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZINCRBY(&cmd))
                );
            }
            "ZINTERSTORE" => {
                let cmd = sorted_sets::parse_zinterstore(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZINTERSTORE(&cmd))
                );
            }
            "ZPOPMAX" => {
                let cmd = sorted_sets::parse_zpopmax(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZPOPMAX(&cmd))
                );
            }
            "ZPOPMIN" => {
                let cmd = sorted_sets::parse_zpopmin(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZPOPMIN(&cmd))
                );
            }
            "ZREM" => {
                let cmd = sorted_sets::parse_zrem(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZREM(&cmd))
                );
            }
            "ZREMRANGEBYLEX" => {
                let cmd = sorted_sets::parse_zremrangebylex(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZREMRANGEBYLEX(&cmd))
                );
            }
            "ZREMRANGEBYRANK" => {
                let cmd = sorted_sets::parse_zremrangebyrank(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZREMRANGEBYRANK(&cmd))
                );
            }
            "ZREMRANGEBYSCORE" => {
                let cmd = sorted_sets::parse_zremrangebyscore(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZREMRANGEBYSCORE(&cmd))
                );
            }
            "ZUNIONSTORE" => {
                let cmd = sorted_sets::parse_zunionstore(iter);
                cmd_handler.iter().for_each(|handler|
                    handler.handle(Command::ZUNIONSTORE(&cmd))
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