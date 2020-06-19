use std::io::{BufReader, Cursor, Read, Result};
use std::io::BufRead;

use byteorder::ReadBytesExt;

use crate::io::{COLON, CR, DOLLAR, LF, MINUS, PLUS, STAR};
use crate::to_string;

struct Decoder<'a> {
    input: BufReader<&'a mut dyn Read>
}

pub(crate) fn decode(input: &mut dyn Read) -> Result<Resp> {
    let mut decoder = Decoder { input: BufReader::new(input) };
    decoder.decode_resp()
}

impl Decoder<'_> {
    fn decode_resp(&mut self) -> Result<Resp> {
        match self.decode_type()? {
            Type::String => Ok(Resp::String(self.decode_string()?)),
            Type::Int => self.decode_int(),
            Type::Error => Ok(Resp::Error(self.decode_string()?)),
            Type::BulkBytes => self.decode_bulk_bytes(),
            Type::Array => self.decode_array(),
            _ => {
                unimplemented!()
            }
        }
    }
    
    fn decode_type(&mut self) -> Result<Type> {
        loop {
            let b = self.input.read_u8()?;
            if b == LF {
                continue;
            } else {
                match b {
                    PLUS => return Ok(Type::String),
                    MINUS => return Ok(Type::Error),
                    COLON => return Ok(Type::Int),
                    DOLLAR => return Ok(Type::BulkBytes),
                    STAR => return Ok(Type::Array),
                    _ => return Ok(Type::Other)
                }
            }
        }
    }
    
    fn decode_string(&mut self) -> Result<String> {
        let mut buf = vec![];
        self.input.read_until(LF, &mut buf)?;
        let n = buf.len() - 2;
        if (n as isize) < 0 || buf[n] != CR {
            panic!("Excepted CRLF");
        } else {
            buf.pop();
            buf.pop();
            Ok(to_string(buf))
        }
    }
    
    fn decode_int(&mut self) -> Result<Resp> {
        let s = self.decode_string()?;
        let i = s.parse::<i64>().unwrap();
        return Ok(Resp::Int(i));
    }
    
    fn decode_bulk_bytes(&mut self) -> Result<Resp> {
        let r = self.decode_int()?;
        if let Resp::Int(i) = r {
            let mut buf = vec![0; i as usize];
            self.input.read_exact(&mut buf)?;
            let mut end = vec![0; 2];
            self.input.read_exact(&mut end)?;
            if !end.eq(&[CR, LF]) {
                panic!("Expected CRLF");
            } else {
                return Ok(Resp::BulkBytes(buf));
            }
        }
        panic!("Excepted Int Response");
    }
    
    fn decode_array(&mut self) -> Result<Resp> {
        let r = self.decode_int()?;
        if let Resp::Int(i) = r {
            let mut arr = Vec::with_capacity(i as usize);
            for _ in 0..i {
                let resp = self.decode_resp()?;
                arr.push(resp);
            }
            return Ok(Resp::Array(arr));
        }
        panic!("Excepted Int Response");
    }
}

enum Type {
    String,
    Error,
    Int,
    BulkBytes,
    Array,
    Other,
}

pub enum Resp {
    String(String),
    Error(String),
    Int(i64),
    BulkBytes(Vec<u8>),
    Array(Vec<Resp>),
}


#[test]
fn test_decode_array() {
    let b = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n";
    let mut cursor = Cursor::new(b);
    let resp = decode(&mut cursor);
    match resp {
        Ok(r) => {
            match r {
                Resp::Array(arr) => {
                    let mut data = Vec::new();
                    for x in arr {
                        match x {
                            Resp::BulkBytes(bytes) => { data.push(bytes) }
                            _ => { panic!("wrong type") }
                        }
                    }
                    assert!(b"SELECT".eq(data.get(0).unwrap().as_slice()));
                    assert!(b"0".eq(data.get(1).unwrap().as_slice()));
                }
                _ => { panic!("wrong type") }
            }
        }
        Err(e) => { panic!(e) }
    }
}
