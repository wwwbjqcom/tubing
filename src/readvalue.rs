/*
@author: xiao cai niao
@datetime: 2019/9/19
*/

use std::str::from_utf8;
use std::io::{Cursor};
use byteorder::{ReadBytesExt, LittleEndian, WriteBytesExt};
use tracing::field::debug;

pub fn read_string_value(pack: &[u8]) -> String{
    match from_utf8(pack) {
        Ok(t) => t.parse().unwrap(),
        Err(e) => {
            debug(format!("{:?}", e));
            String::from("")
        }
    }

}

pub fn read_u16(pack: &[u8]) -> u16 {
    let mut rdr = Cursor::new(pack);
    rdr.read_u16::<LittleEndian>().unwrap()
}

pub fn read_u32(pack: &[u8]) -> u32 {
    let mut rdr = Cursor::new(pack);
    rdr.read_u32::<LittleEndian>().unwrap()
}

pub fn read_i32(pack: &[u8]) -> i32 {
    let mut rdr = Cursor::new(pack);
    rdr.read_i32::<LittleEndian>().unwrap()
}

pub fn write_u24(num: u32) -> Vec<u8> {
    let mut rdr = Vec::new();
    rdr.write_u24::<LittleEndian>(num).unwrap();
    return rdr;
}

pub fn write_u32(num: u32) -> Vec<u8> {
    let mut rdr = Vec::new();
    rdr.write_u32::<LittleEndian>(num).unwrap();
    return rdr;
}

pub fn write_i32(num: i32) -> Vec<u8> {
    let mut rdr = Vec::new();
    rdr.write_i32::<LittleEndian>(num).unwrap();
    return rdr;
}

pub fn write_u16(num: u16) -> Vec<u8> {
    let mut rdr = Vec::new();
    rdr.write_u16::<LittleEndian>(num).unwrap();
    return rdr;
}




