use std::io::{BufWriter, Cursor, Error, ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::result::Result;
use std::result::Result::Ok;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::config::Config;
use crate::Data::{Bytes, BytesVec, Empty};

mod config;
mod lzf;

pub trait RedisEventListener {
    fn open(&mut self) -> Result<(), Error>;
    
    fn close(&self);
}

// 用于监听Redis单点的事件
pub struct StandaloneEventListener {
    addr: SocketAddr,
    password: &'static str,
    config: Config,
    stream: Option<TcpStream>,
    id: &'static str,
    offset: i64,
    rdb_listener: Vec<Box<dyn EventListener>>,
    command_listener: Vec<Box<dyn EventListener>>,
}

impl StandaloneEventListener {
    fn connect(&mut self) -> Result<(), Error> {
        let stream = TcpStream::connect(self.addr)?;
        println!("connected to server!");
        self.stream = Option::Some(stream);
        Ok(())
    }
    
    fn auth(&mut self) -> Result<(), Error> {
        if !self.password.is_empty() {
            self.send(b"AUTH", &[self.password.as_bytes()])?;
            self.response(read_bytes)?;
        }
        Ok(())
    }
    
    fn send_port(&mut self) -> Result<(), Error> {
        let stream = self.stream.as_ref().unwrap();
        let port = stream.local_addr()?.port().to_string();
        let port = port.as_bytes();
        self.send(b"REPLCONF", &[b"listening-port", port])?;
        self.response(read_bytes)?;
        Ok(())
    }
    
    fn send(&mut self, command: &[u8], args: &[&[u8]]) -> Result<(), Error> {
        let stream = self.stream.as_ref().unwrap();
        let mut writer = BufWriter::new(stream);
        writer.write(&[STAR])?;
        let args_len = args.len() + 1;
        writer.write(&args_len.to_string().into_bytes())?;
        writer.write(&[CR, LF, DOLLAR])?;
        writer.write(&command.len().to_string().into_bytes())?;
        writer.write(&[CR, LF])?;
        writer.write(command)?;
        writer.write(&[CR, LF])?;
        for arg in args {
            writer.write(&[DOLLAR])?;
            writer.write(&arg.len().to_string().into_bytes())?;
            writer.write(&[CR, LF])?;
            writer.write(arg)?;
            writer.write(&[CR, LF])?;
        }
        writer.flush()
    }
    
    fn response(&mut self, func: fn(&mut dyn Read, isize, &mut Vec<Box<dyn EventListener>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error>)
                -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
        let mut socket = self.stream.as_ref().unwrap();
        let response_type = socket.read_u8()?;
        match response_type {
            // Plus: Simple String
            // Minus: Error
            // Colon: Integer
            PLUS | MINUS | COLON => {
                let mut bytes = vec![];
                loop {
                    let byte = socket.read_u8()?;
                    if byte != CR {
                        bytes.push(byte);
                    } else {
                        break;
                    }
                }
                let byte = socket.read_u8()?;
                if byte == LF {
                    if response_type == PLUS || response_type == COLON {
                        return Ok(Bytes(bytes));
                    } else {
                        let message = String::from_utf8(bytes).unwrap();
                        return Err(Error::new(ErrorKind::InvalidInput, message));
                    }
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Expect LF after CR"));
                }
            }
            DOLLAR => { // Bulk String
                let mut bytes = vec![];
                loop {
                    let byte = socket.read_u8()?;
                    if byte != CR {
                        bytes.push(byte);
                    } else {
                        break;
                    }
                }
                let byte = socket.read_u8()?;
                if byte == LF {
                    let length = String::from_utf8(bytes).unwrap();
                    let length = length.parse::<isize>().unwrap();
                    let stream = self.stream.as_mut().unwrap();
                    return func(stream, length, &mut self.rdb_listener);
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Expect LF after CR"));
                }
            }
            STAR => { // Array
                let mut bytes = vec![];
                loop {
                    let byte = socket.read_u8()?;
                    if byte != CR {
                        bytes.push(byte);
                    } else {
                        break;
                    }
                }
                let byte = socket.read_u8()?;
                if byte == LF {
                    let length = String::from_utf8(bytes).unwrap();
                    let length = length.parse::<isize>().unwrap();
                    if length <= 0 {
                        return Ok(Empty);
                    } else {
                        let mut result = vec![];
                        for _ in 0..length {
                            match self.response(read_bytes)? {
                                Bytes(resp) => {
                                    result.push(resp);
                                }
                                BytesVec(mut resp) => {
                                    result.append(&mut resp);
                                }
                                Empty => {
                                    return Err(Error::new(ErrorKind::InvalidData, "Expect Redis response, but got empty"));
                                }
                            }
                        }
                        return Ok(BytesVec(result));
                    }
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Expect LF after CR"));
                }
            }
            _ => {}
        }
        Ok(Empty)
    }
    
    fn new(addr: SocketAddr, password: &'static str) -> StandaloneEventListener {
        StandaloneEventListener {
            addr,
            password,
            config: config::default(),
            stream: Option::None,
            id: "?",
            offset: -1,
            rdb_listener: Vec::new(),
            command_listener: Vec::new(),
        }
    }
    
    fn add_rdb_listener(&mut self, listener: Box<dyn EventListener>) {
        self.rdb_listener.push(listener)
    }
    
    fn add_command_listener(&mut self, listener: Box<dyn EventListener>) {
        self.command_listener.push(listener)
    }
}

impl RedisEventListener for StandaloneEventListener {
    fn open(&mut self) -> Result<(), Error> {
        self.connect()?;
        self.auth()?;
        self.send_port()?;
        
        let offset = self.offset.to_string();
        let replica_offset = offset.as_bytes();
        
        self.send(b"PSYNC", &[self.id.as_bytes(), replica_offset])?;
        if let Bytes(resp) = self.response(read_bytes)? {
            let resp = String::from_utf8(resp).unwrap();
            if resp.starts_with("FULLRESYNC") {
                self.response(parse_rdb)?;
            }
        } else {
            return Err(Error::new(ErrorKind::InvalidData, "Expect Redis string response"));
        }
        Ok(())
    }
    
    fn close(&self) {
        let option = self.stream.as_ref();
        if self.stream.is_some() {
            println!("close connection with server...");
            option.unwrap().shutdown(Shutdown::Both).unwrap();
        }
    }
}

// 用于包装redis的返回值
enum Data<B, V> {
    // 包装Vec<u8>
    Bytes(B),
    // 包装Vec<Vec<u8>>
    BytesVec(V),
    // 空返回
    Empty,
}

// redis事件
pub trait Event {}

// redis key-value
struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Event for KeyValue {}

// 监听redis事件
pub trait EventListener {
    fn handle(&mut self, e: &dyn Event);
}

// 当redis响应的数据是Bulk string时，使用此方法读取指定length的字节, 并返回
fn read_bytes(socket: &mut dyn Read, length: isize, _: &mut Vec<Box<dyn EventListener>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    if length > 0 {
        let mut bytes = vec![];
        for _ in 0..length {
            bytes.push(socket.read_u8()?);
        }
        let end = &mut [0; 2];
        socket.read_exact(end)?;
        if end == b"\r\n" {
            return Ok(Bytes(bytes));
        } else {
            return Err(Error::new(ErrorKind::Other, "Expect CRLF after bulk string"));
        }
    } else if length == 0 {
        // length == 0 代表空字符，后面还有CRLF
        socket.read_exact(&mut [0; 2])?;
        return Ok(Empty);
    } else {
        // length < 0 代表null
        return Ok(Empty);
    }
}

// 读取、解析rdb
fn parse_rdb(socket: &mut dyn Read, length: isize, rdb_listeners: &mut Vec<Box<dyn EventListener>>) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    println!("rdb size: {} bytes", length);
    let mut bytes = vec![0; 5];
    // 开头5个字节: REDIS
    socket.read_exact(&mut bytes)?;
    // 4个字节: rdb版本
    socket.read_exact(&mut bytes[..=3])?;
    let rdb_version = String::from_utf8(bytes[..=3].to_vec()).unwrap();
    let rdb_version = rdb_version.parse::<isize>().unwrap();
    loop {
        let data_type = socket.read_u8()?;
        match data_type {
            AUX => {
                if let Bytes(data) = read_string(socket)? {
                    let data = String::from_utf8(data).unwrap();
                    print!("{}: ", data);
                }
                if let Bytes(data) = read_string(socket)? {
                    let data = String::from_utf8(data).unwrap();
                    print!("{}\r\n", data);
                }
            }
            SELECT_DB => {
                let db = read_length(socket)?;
                println!("db: {}", db.val);
            }
            DB_SIZE => {
                let db = read_length(socket)?;
                println!("db total keys: {}", db.val);
                let db = read_length(socket)?;
                println!("db expired keys: {}", db.val);
            }
            STRING => {
                let key;
                let value;
                if let Bytes(data) = read_string(socket)? {
                    key = data;
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                }
                if let Bytes(data) = read_string(socket)? {
                    value = data;
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                }
            }
            HASH_ZIP_LIST | ZSET_ZIP_LIST => {
                let key;
                if let Bytes(data) = read_string(socket)? {
                    key = data;
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                }
                let bytes;
                if let Bytes(data) = read_string(socket)? {
                    bytes = data;
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                }
                let cursor = &mut Cursor::new(&bytes);
                // 跳过ZL_BYTES和ZL_TAIL
                cursor.set_position(8);
                let mut length = cursor.read_u16::<LittleEndian>()? as usize;
                let mut args: Vec<Vec<u8>> = Vec::with_capacity(length + 1);
                args[0] = key;
                
                let mut index = 1;
                while length > 0 {
                    let field_name = read_zip_list_entry(cursor)?;
                    let field_val = read_zip_list_entry(cursor)?;
                    args[index] = field_name;
                    args[index + 1] = field_val;
                    index += 2;
                    length -= 2;
                }
            }
            LIST_QUICK_LIST => {
                let key;
                if let Bytes(data) = read_string(socket)? {
                    key = data;
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                }
                let count = read_length(socket)?;
                let mut args: Vec<Vec<u8>> = vec![];
                args.insert(0, key);
                
                let mut index = 0;
                while index < count.val {
                    if let Bytes(data) = read_string(socket)? {
                        let cursor = &mut Cursor::new(&data);
                        // 跳过ZL_BYTES和ZL_TAIL
                        cursor.set_position(8);
                        let mut length = cursor.read_u16::<LittleEndian>()? as usize;
                        while length > 0 {
                            let list_item = read_zip_list_entry(cursor)?;
                            args.push(list_item);
                            length -= 1;
                        }
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                    }
                    index += 1;
                }
            }
            LIST | SET => {
                let key;
                if let Bytes(data) = read_string(socket)? {
                    key = data;
                } else {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                }
                let count = read_length(socket)?;
                let mut args: Vec<Vec<u8>> = Vec::with_capacity((count.val as usize) + 1);
                args[0] = key;
                let mut index = 1;
                while index < count.val {
                    if let Bytes(data) = read_string(socket)? {
                        args.push(data);
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData, "Invalid string data"));
                    }
                    index += 1;
                }
            }
            EOF => {
                if rdb_version >= 5 {
                    read_integer(socket, 8, true)?;
                }
                break;
            }
            _ => break
        };
    };
    Ok(Empty)
}

// 读取一个string
fn read_string(socket: &mut dyn Read) -> Result<Data<Vec<u8>, Vec<Vec<u8>>>, Error> {
    let length = read_length(socket)?;
    if length.is_special {
        match length.val {
            0 => {
                let int = socket.read_i8()?;
                return Ok(Bytes(int.to_string().into_bytes().to_vec()));
            }
            1 => {
                let int = read_integer(socket, 2, false)?;
                return Ok(Bytes(int.to_string().into_bytes().to_vec()));
            }
            2 => {
                let int = read_integer(socket, 4, false)?;
                return Ok(Bytes(int.to_string().into_bytes().to_vec()));
            }
            3 => {
                let compressed_len = read_length(socket)?;
                let origin_len = read_length(socket)?;
                let mut compressed = vec![0; compressed_len.val as usize];
                socket.read_exact(&mut compressed)?;
                let mut origin = vec![0; origin_len.val as usize];
                lzf::decompress(&mut compressed, compressed_len.val, &mut origin, origin_len.val);
                return Ok(Bytes(origin));
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid string length"))
        };
    };
    let mut buff = vec![0; length.val as usize];
    socket.read_exact(&mut buff)?;
    Ok(Bytes(buff))
}

struct Length {
    val: isize,
    is_special: bool,
}

// 读取redis响应中下一条数据的长度
fn read_length(socket: &mut dyn Read) -> Result<Length, Error> {
    let byte = socket.read_u8()?;
    
    let byte_and = byte & 0xFF;
    
    let _type = (byte & 0xC0) >> 6;
    
    let mut result = -1;
    let mut is_special = false;
    
    if _type == 3 {
        result = (byte & 0x3F) as isize;
        is_special = true;
    } else if _type == 0 {
        result = (byte & 0x3F) as isize;
    } else if _type == 1 {
        let next_byte = socket.read_u8()?;
        result = (((byte as u16 & 0x3F) << 8) | next_byte as u16) as isize;
    } else if byte_and == 0x80 {
        result = read_integer(socket, 4, true)?;
    } else if byte_and == 0x81 {
        result = read_integer(socket, 8, true)?;
    };
    Ok(Length { val: result, is_special })
}

fn read_integer(socket: &mut dyn Read, size: isize, is_big_endian: bool) -> Result<isize, Error> {
    let mut buff = vec![0; size as usize];
    socket.read_exact(&mut buff)?;
    let mut cursor = Cursor::new(&buff);
    
    if is_big_endian {
        if size == 2 {
            return Ok(cursor.read_i16::<BigEndian>()? as isize);
        } else if size == 4 {
            return Ok(cursor.read_i32::<BigEndian>()? as isize);
        } else if size == 8 {
            return Ok(cursor.read_i64::<BigEndian>()? as isize);
        };
    } else {
        if size == 2 {
            return Ok(cursor.read_i16::<LittleEndian>()? as isize);
        } else if size == 4 {
            return Ok(cursor.read_i32::<LittleEndian>()? as isize);
        } else if size == 8 {
            return Ok(cursor.read_i64::<LittleEndian>()? as isize);
        };
    }
    Err(Error::new(ErrorKind::InvalidData, "Invalid integer size"))
}

fn read_zip_list_entry(cursor: &mut Cursor<&Vec<u8>>) -> Result<Vec<u8>, Error> {
    if cursor.read_u8()? >= 254 {
        cursor.read_u32::<LittleEndian>()?;
    }
    let flag = cursor.read_u8()?;
    match flag >> 6 {
        0 => {
            let length = flag & 0x3F;
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        1 => {
            let next_byte = cursor.read_u8()?;
            let length = (((flag as u16) & 0x3F) << 8) | (next_byte as u16);
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        2 => {
            let length = cursor.read_u32::<BigEndian>()?;
            let mut buff = vec![0; length as usize];
            cursor.read_exact(&mut buff)?;
            return Ok(buff);
        }
        _ => {}
    }
    match flag {
        ZIP_INT_8BIT => {
            let int = cursor.read_i8()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_16BIT => {
            let int = cursor.read_i16::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_24BIT => {
            let int = cursor.read_i24::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_32BIT => {
            let int = cursor.read_i32::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        ZIP_INT_64BIT => {
            let int = cursor.read_i64::<LittleEndian>()?;
            return Ok(int.to_string().into_bytes());
        }
        _ => {
            let result = (flag - 0xF1) as isize;
            return Ok(result.to_string().into_bytes());
        }
    }
}

// 回车换行，在redis响应中一般表示终结符，或用作分隔符以分隔数据
const CR: u8 = b'\r';
const LF: u8 = b'\n';
// 代表array响应
const STAR: u8 = b'*';
// 代表bulk string响应
const DOLLAR: u8 = b'$';
// 代表simple string响应
const PLUS: u8 = b'+';
// 代表error响应
const MINUS: u8 = b'-';
// 代表integer响应
const COLON: u8 = b':';
// 代表 aux field
const AUX: u8 = 0xFA;
// 当前redis db
const SELECT_DB: u8 = 0xFE;
// db的key数量
const DB_SIZE: u8 = 0xFB;
// 代表字符串的数据
const STRING: u8 = 0;
const LIST: u8 = 1;
const SET: u8 = 2;
const ZSET_ZIP_LIST: u8 = 12;
const HASH_ZIP_LIST: u8 = 13;
const LIST_QUICK_LIST: u8 = 14;
// end of file
const EOF: u8 = 0xFF;
const ZIP_INT_8BIT: u8 = 0xFE;
const ZIP_INT_16BIT: u8 = 0xC0;
const ZIP_INT_24BIT: u8 = 0xF0;
const ZIP_INT_32BIT: u8 = 0xD0;
const ZIP_INT_64BIT: u8 = 0xE0;

// 11100000
// 测试用例
#[cfg(test)]
mod tests {
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;
    
    use crate::{RedisEventListener, StandaloneEventListener};
    
    #[test]
    fn open() {
        let ip = IpAddr::from_str("127.0.0.1").unwrap();
        let mut redis_listener = StandaloneEventListener::new(SocketAddr::new(ip, 6379), "123456");
        if let Err(error) = redis_listener.open() {
            panic!("couldn't connect to server: {}", error)
        }
        redis_listener.close();
    }
}
