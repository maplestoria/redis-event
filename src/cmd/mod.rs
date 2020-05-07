/*!
所有支持的Redis命令的定义，以及命令相关的解析代码俱在此模块下

此模块包括:
- 所有支持的Redis命令定义，见于枚举[Command]及各个子模块
- 相关Redis命令的解析代码，见于各个子模块

子模块的命名与分组，按照[Redis Command Reference]中的`filter by group`进行命名与分组，各个命令所对应的结构体中的字段命名已尽可能和文档中的保持一致。

所有涉及到的命令参考[Redis Command Reference]所描述。

[Command]: enum.Command.html
[Redis Command Reference]: https://redis.io/commands
*/
use std::cell::RefMut;

use crate::{Event, EventHandler};
use crate::cmd::connection::{SELECT, SWAPDB};
use crate::cmd::hashes::*;
use crate::cmd::hyperloglog::{PFADD, PFCOUNT, PFMERGE};
use crate::cmd::keys::*;
use crate::cmd::lists::*;
use crate::cmd::pub_sub::PUBLISH;
use crate::cmd::scripting::{EVAL, EVALSHA, SCRIPTLOAD};
use crate::cmd::server::{FLUSHALL, FLUSHDB};
use crate::cmd::sets::*;
use crate::cmd::sorted_sets::*;
use crate::cmd::streams::{XACK, XADD, XCLAIM, XDEL, XGROUP, XTRIM};
use crate::cmd::strings::*;

pub mod connection;
pub mod hashes;
pub mod hyperloglog;
pub mod keys;
pub mod lists;
pub mod pub_sub;
pub mod scripting;
pub mod server;
pub mod sets;
pub mod sorted_sets;
pub mod strings;
pub mod streams;

/// 所有支持的Redis命令
///
/// 不在此枚举中的Redis命令均不支持
#[derive(Debug)]
pub enum Command<'a> {
    APPEND(&'a APPEND<'a>),
    BITFIELD(&'a BITFIELD<'a>),
    BITOP(&'a BITOP<'a>),
    BRPOPLPUSH(&'a BRPOPLPUSH<'a>),
    DECR(&'a DECR<'a>),
    DECRBY(&'a DECRBY<'a>),
    DEL(&'a DEL<'a>),
    EVAL(&'a EVAL<'a>),
    EVALSHA(&'a EVALSHA<'a>),
    EXPIRE(&'a EXPIRE<'a>),
    EXPIREAT(&'a EXPIREAT<'a>),
    EXEC,
    FLUSHALL(&'a FLUSHALL),
    FLUSHDB(&'a FLUSHDB),
    GETSET(&'a GETSET<'a>),
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
    MULTI,
    PERSIST(&'a PERSIST<'a>),
    PEXPIRE(&'a PEXPIRE<'a>),
    PEXPIREAT(&'a PEXPIREAT<'a>),
    PFADD(&'a PFADD<'a>),
    PFCOUNT(&'a PFCOUNT<'a>),
    PFMERGE(&'a PFMERGE<'a>),
    PSETEX(&'a PSETEX<'a>),
    PUBLISH(&'a PUBLISH<'a>),
    RENAME(&'a RENAME<'a>),
    RENAMENX(&'a RENAMENX<'a>),
    RESTORE(&'a RESTORE<'a>),
    RPOP(&'a RPOP<'a>),
    RPOPLPUSH(&'a RPOPLPUSH<'a>),
    RPUSH(&'a RPUSH<'a>),
    RPUSHX(&'a RPUSHX<'a>),
    SADD(&'a SADD<'a>),
    SCRIPTFLUSH,
    SCRIPTLOAD(&'a SCRIPTLOAD<'a>),
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
    SWAPDB(&'a SWAPDB<'a>),
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
    XACK(&'a XACK<'a>),
    XADD(&'a XADD<'a>),
    XCLAIM(&'a XCLAIM<'a>),
    XDEL(&'a XDEL<'a>),
    XGROUP(&'a XGROUP<'a>),
    XTRIM(&'a XTRIM<'a>),
    Other(RawCommand),
}

#[derive(Debug)]
pub struct RawCommand {
    pub name: String,
    pub args: Vec<Vec<u8>>,
}

pub(crate) fn parse(data: Vec<Vec<u8>>, cmd_handler: &mut RefMut<dyn EventHandler>) {
    let mut iter = data.iter();
    if let Some(cmd_name) = iter.next() {
        let cmd_name = String::from_utf8_lossy(cmd_name).to_uppercase();
        match cmd_name.as_str() {
            "APPEND" => {
                let cmd = strings::parse_append(iter);
                cmd_handler.handle(Event::AOF(Command::APPEND(&cmd)));
            }
            "BITFIELD" => {
                let cmd = strings::parse_bitfield(iter);
                cmd_handler.handle(Event::AOF(Command::BITFIELD(&cmd)));
            }
            "BITOP" => {
                let cmd = strings::parse_bitop(iter);
                cmd_handler.handle(Event::AOF(Command::BITOP(&cmd)));
            }
            "BRPOPLPUSH" => {
                let cmd = lists::parse_brpoplpush(iter);
                cmd_handler.handle(Event::AOF(Command::BRPOPLPUSH(&cmd)));
            }
            "DEL" => {
                let cmd = keys::parse_del(iter);
                cmd_handler.handle(Event::AOF(Command::DEL(&cmd)));
            }
            "DECR" => {
                let cmd = strings::parse_decr(iter);
                cmd_handler.handle(Event::AOF(Command::DECR(&cmd)));
            }
            "DECRBY" => {
                let cmd = strings::parse_decrby(iter);
                cmd_handler.handle(Event::AOF(Command::DECRBY(&cmd)));
            }
            "EVAL" => {
                let cmd = scripting::parse_eval(iter);
                cmd_handler.handle(Event::AOF(Command::EVAL(&cmd)));
            }
            "EVALSHA" => {
                let cmd = scripting::parse_evalsha(iter);
                cmd_handler.handle(Event::AOF(Command::EVALSHA(&cmd)));
            }
            "EXPIRE" => {
                let cmd = keys::parse_expire(iter);
                cmd_handler.handle(Event::AOF(Command::EXPIRE(&cmd)));
            }
            "EXPIREAT" => {
                let cmd = keys::parse_expireat(iter);
                cmd_handler.handle(Event::AOF(Command::EXPIREAT(&cmd)));
            }
            "EXEC" => {
                cmd_handler.handle(Event::AOF(Command::EXEC));
            }
            "FLUSHALL" => {
                let cmd = server::parse_flushall(iter);
                cmd_handler.handle(Event::AOF(Command::FLUSHALL(&cmd)));
            }
            "FLUSHDB" => {
                let cmd = server::parse_flushdb(iter);
                cmd_handler.handle(Event::AOF(Command::FLUSHDB(&cmd)));
            }
            "GETSET" => {
                let cmd = strings::parse_getset(iter);
                cmd_handler.handle(Event::AOF(Command::GETSET(&cmd)));
            }
            "HDEL" => {
                let cmd = hashes::parse_hdel(iter);
                cmd_handler.handle(Event::AOF(Command::HDEL(&cmd)));
            }
            "HINCRBY" => {
                let cmd = hashes::parse_hincrby(iter);
                cmd_handler.handle(Event::AOF(Command::HINCRBY(&cmd)));
            }
            "HMSET" => {
                let cmd = hashes::parse_hmset(iter);
                cmd_handler.handle(Event::AOF(Command::HMSET(&cmd)));
            }
            "HSET" => {
                let cmd = hashes::parse_hset(iter);
                cmd_handler.handle(Event::AOF(Command::HSET(&cmd)));
            }
            "HSETNX" => {
                let cmd = hashes::parse_hsetnx(iter);
                cmd_handler.handle(Event::AOF(Command::HSETNX(&cmd)));
            }
            "INCR" => {
                let cmd = strings::parse_incr(iter);
                cmd_handler.handle(Event::AOF(Command::INCR(&cmd)));
            }
            "INCRBY" => {
                let cmd = strings::parse_incrby(iter);
                cmd_handler.handle(Event::AOF(Command::INCRBY(&cmd)));
            }
            "LINSERT" => {
                let cmd = lists::parse_linsert(iter);
                cmd_handler.handle(Event::AOF(Command::LINSERT(&cmd)));
            }
            "LPOP" => {
                let cmd = lists::parse_lpop(iter);
                cmd_handler.handle(Event::AOF(Command::LPOP(&cmd)));
            }
            "LPUSH" => {
                let cmd = lists::parse_lpush(iter);
                cmd_handler.handle(Event::AOF(Command::LPUSH(&cmd)));
            }
            "LPUSHX" => {
                let cmd = lists::parse_lpushx(iter);
                cmd_handler.handle(Event::AOF(Command::LPUSHX(&cmd)));
            }
            "LREM" => {
                let cmd = lists::parse_lrem(iter);
                cmd_handler.handle(Event::AOF(Command::LREM(&cmd)));
            }
            "LSET" => {
                let cmd = lists::parse_lset(iter);
                cmd_handler.handle(Event::AOF(Command::LSET(&cmd)));
            }
            "LTRIM" => {
                let cmd = lists::parse_ltrim(iter);
                cmd_handler.handle(Event::AOF(Command::LTRIM(&cmd)));
            }
            "RENAME" => {
                let cmd = keys::parse_rename(iter);
                cmd_handler.handle(Event::AOF(Command::RENAME(&cmd)));
            }
            "RENAMENX" => {
                let cmd = keys::parse_renamenx(iter);
                cmd_handler.handle(Event::AOF(Command::RENAMENX(&cmd)));
            }
            "RESTORE" => {
                let cmd = keys::parse_restore(iter);
                cmd_handler.handle(Event::AOF(Command::RESTORE(&cmd)));
            }
            "RPOP" => {
                let cmd = lists::parse_rpop(iter);
                cmd_handler.handle(Event::AOF(Command::RPOP(&cmd)));
            }
            "RPOPLPUSH" => {
                let cmd = lists::parse_rpoplpush(iter);
                cmd_handler.handle(Event::AOF(Command::RPOPLPUSH(&cmd)));
            }
            "RPUSH" => {
                let cmd = lists::parse_rpush(iter);
                cmd_handler.handle(Event::AOF(Command::RPUSH(&cmd)));
            }
            "RPUSHX" => {
                let cmd = lists::parse_rpushx(iter);
                cmd_handler.handle(Event::AOF(Command::RPUSHX(&cmd)));
            }
            "SADD" => {
                let cmd = sets::parse_sadd(iter);
                cmd_handler.handle(Event::AOF(Command::SADD(&cmd)));
            }
            "SCRIPT" => {
                let cmd = iter.next().unwrap();
                let cmd = String::from_utf8_lossy(cmd).to_uppercase();
                if &cmd == "LOAD" {
                    let cmd = scripting::parse_script_load(iter);
                    cmd_handler.handle(Event::AOF(Command::SCRIPTLOAD(&cmd)));
                } else if &cmd == "FLUSH" {
                    cmd_handler.handle(Event::AOF(Command::SCRIPTFLUSH));
                }
            }
            "SDIFFSTORE" => {
                let cmd = sets::parse_sdiffstore(iter);
                cmd_handler.handle(Event::AOF(Command::SDIFFSTORE(&cmd)));
            }
            "SMOVE" => {
                let cmd = sets::parse_smove(iter);
                cmd_handler.handle(Event::AOF(Command::SMOVE(&cmd)));
            }
            "SET" => {
                let cmd = strings::parse_set(iter);
                cmd_handler.handle(Event::AOF(Command::SET(&cmd)));
            }
            "SELECT" => {
                let cmd = connection::parse_select(iter);
                cmd_handler.handle(Event::AOF(Command::SELECT(&cmd)));
            }
            "SORT" => {
                let cmd = keys::parse_sort(iter);
                cmd_handler.handle(Event::AOF(Command::SORT(&cmd)));
            }
            "SREM" => {
                let cmd = sets::parse_srem(iter);
                cmd_handler.handle(Event::AOF(Command::SREM(&cmd)));
            }
            "SUNIONSTORE" => {
                let cmd = sets::parse_sunionstore(iter);
                cmd_handler.handle(Event::AOF(Command::SUNIONSTORE(&cmd)));
            }
            "SWAPDB" => {
                let cmd = connection::parse_swapdb(iter);
                cmd_handler.handle(Event::AOF(Command::SWAPDB(&cmd)));
            }
            "UNLINK" => {
                let cmd = keys::parse_unlink(iter);
                cmd_handler.handle(Event::AOF(Command::UNLINK(&cmd)));
            }
            "MOVE" => {
                let cmd = keys::parse_move(iter);
                cmd_handler.handle(Event::AOF(Command::MOVE(&cmd)));
            }
            "MSET" => {
                let cmd = strings::parse_mset(iter);
                cmd_handler.handle(Event::AOF(Command::MSET(&cmd)));
            }
            "MSETNX" => {
                let cmd = strings::parse_msetnx(iter);
                cmd_handler.handle(Event::AOF(Command::MSETNX(&cmd)));
            }
            "MULTI" => {
                cmd_handler.handle(Event::AOF(Command::MULTI));
            }
            "PFADD" => {
                let cmd = hyperloglog::parse_pfadd(iter);
                cmd_handler.handle(Event::AOF(Command::PFADD(&cmd)));
            }
            "PFCOUNT" => {
                let cmd = hyperloglog::parse_pfcount(iter);
                cmd_handler.handle(Event::AOF(Command::PFCOUNT(&cmd)));
            }
            "PFMERGE" => {
                let cmd = hyperloglog::parse_pfmerge(iter);
                cmd_handler.handle(Event::AOF(Command::PFMERGE(&cmd)));
            }
            "SETEX" => {
                let cmd = strings::parse_setex(iter);
                cmd_handler.handle(Event::AOF(Command::SETEX(&cmd)));
            }
            "SETNX" => {
                let cmd = strings::parse_setnx(iter);
                cmd_handler.handle(Event::AOF(Command::SETNX(&cmd)));
            }
            "PSETEX" => {
                let cmd = strings::parse_psetex(iter);
                cmd_handler.handle(Event::AOF(Command::PSETEX(&cmd)));
            }
            "PUBLISH" => {
                let cmd = pub_sub::parse_publish(iter);
                cmd_handler.handle(Event::AOF(Command::PUBLISH(&cmd)));
            }
            "PEXPIRE" => {
                let cmd = keys::parse_pexpire(iter);
                cmd_handler.handle(Event::AOF(Command::PEXPIRE(&cmd)));
            }
            "PEXPIREAT" => {
                let cmd = keys::parse_pexpireat(iter);
                cmd_handler.handle(Event::AOF(Command::PEXPIREAT(&cmd)));
            }
            "PERSIST" => {
                let cmd = keys::parse_persist(iter);
                cmd_handler.handle(Event::AOF(Command::PERSIST(&cmd)));
            }
            "SETRANGE" => {
                let cmd = strings::parse_setrange(iter);
                cmd_handler.handle(Event::AOF(Command::SETRANGE(&cmd)));
            }
            "SETBIT" => {
                let cmd = strings::parse_setbit(iter);
                cmd_handler.handle(Event::AOF(Command::SETBIT(&cmd)));
            }
            "SINTERSTORE" => {
                let cmd = sets::parse_sinterstore(iter);
                cmd_handler.handle(Event::AOF(Command::SINTERSTORE(&cmd)));
            }
            "ZADD" => {
                let cmd = sorted_sets::parse_zadd(iter);
                cmd_handler.handle(Event::AOF(Command::ZADD(&cmd)));
            }
            "ZINCRBY" => {
                let cmd = sorted_sets::parse_zincrby(iter);
                cmd_handler.handle(Event::AOF(Command::ZINCRBY(&cmd)));
            }
            "ZINTERSTORE" => {
                let cmd = sorted_sets::parse_zinterstore(iter);
                cmd_handler.handle(Event::AOF(Command::ZINTERSTORE(&cmd)));
            }
            "ZPOPMAX" => {
                let cmd = sorted_sets::parse_zpopmax(iter);
                cmd_handler.handle(Event::AOF(Command::ZPOPMAX(&cmd)));
            }
            "ZPOPMIN" => {
                let cmd = sorted_sets::parse_zpopmin(iter);
                cmd_handler.handle(Event::AOF(Command::ZPOPMIN(&cmd)));
            }
            "ZREM" => {
                let cmd = sorted_sets::parse_zrem(iter);
                cmd_handler.handle(Event::AOF(Command::ZREM(&cmd)));
            }
            "ZREMRANGEBYLEX" => {
                let cmd = sorted_sets::parse_zremrangebylex(iter);
                cmd_handler.handle(Event::AOF(Command::ZREMRANGEBYLEX(&cmd)));
            }
            "ZREMRANGEBYRANK" => {
                let cmd = sorted_sets::parse_zremrangebyrank(iter);
                cmd_handler.handle(Event::AOF(Command::ZREMRANGEBYRANK(&cmd)));
            }
            "ZREMRANGEBYSCORE" => {
                let cmd = sorted_sets::parse_zremrangebyscore(iter);
                cmd_handler.handle(Event::AOF(Command::ZREMRANGEBYSCORE(&cmd)));
            }
            "ZUNIONSTORE" => {
                let cmd = sorted_sets::parse_zunionstore(iter);
                cmd_handler.handle(Event::AOF(Command::ZUNIONSTORE(&cmd)));
            }
            "XACK" => {
                let cmd = streams::parse_xack(iter);
                cmd_handler.handle(Event::AOF(Command::XACK(&cmd)));
            }
            "XADD" => {
                let cmd = streams::parse_xadd(iter);
                cmd_handler.handle(Event::AOF(Command::XADD(&cmd)));
            }
            "XCLAIM" => {
                let cmd = streams::parse_xclaim(iter);
                cmd_handler.handle(Event::AOF(Command::XCLAIM(&cmd)));
            }
            "XDEL" => {
                let cmd = streams::parse_xdel(iter);
                cmd_handler.handle(Event::AOF(Command::XDEL(&cmd)));
            }
            "XGROUP" => {
                let cmd = streams::parse_xgroup(iter);
                cmd_handler.handle(Event::AOF(Command::XGROUP(&cmd)));
            }
            "XTRIM" => {
                let cmd = streams::parse_xtrim(iter);
                cmd_handler.handle(Event::AOF(Command::XTRIM(&cmd)));
            }
            "PING" => {
                // PING命令是由Redis master主动发送过来，判断下游节点是否活跃，不需要处理
            }
            _ => {
                let mut args = Vec::new();
                while let Some(arg) = iter.next() {
                    args.push(arg.clone());
                }
                let cmd = RawCommand {
                    name: cmd_name,
                    args,
                };
                cmd_handler.handle(Event::AOF(Command::Other(cmd)))
            }
        };
    }
}