/*
@author: xiao cai niao
@datetime: 2019/9/19
*/

use std::str::from_utf8;
use std::io::{Cursor, Read};
use byteorder::{ReadBytesExt, LittleEndian, WriteBytesExt, BigEndian};
use std::io;
use std::net::TcpStream;
use std::error::Error;
use tracing::field::debug;

pub fn read_num_pack<R: Read, S: Into<usize>>(num: S, buf: &mut R) -> Vec<u8> {
    let mut pack = vec![0u8; num.into()];
    buf.read_exact(&mut pack).unwrap();
    pack.to_vec()
}

pub fn read_string_value_from_len<R: Read, S: Into<usize>>(buf: &mut R,num: S) -> String {
    let pack = read_num_pack(num,buf);
    String::from_utf8_lossy(&pack).to_string()
    //from_utf8(&pack).unwrap().parse().unwrap()
}

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

pub fn read_i16(pack: &[u8]) -> i16 {
    let mut rdr = Cursor::new(pack);
    rdr.read_i16::<LittleEndian>().unwrap()
}

pub fn read_u24(pack: &[u8]) -> u32 {
    let mut rdr = Cursor::new(pack);
    rdr.read_u24::<LittleEndian>().unwrap()
}

pub fn read_i24(pack: &[u8]) -> i32 {
    let mut rdr = Cursor::new(pack);
    rdr.read_i24::<LittleEndian>().unwrap()
}

pub fn read_u32(pack: &[u8]) -> u32 {
    let mut rdr = Cursor::new(pack);
    rdr.read_u32::<LittleEndian>().unwrap()
}

pub fn read_i32(pack: &[u8]) -> i32 {
    let mut rdr = Cursor::new(pack);
    rdr.read_i32::<LittleEndian>().unwrap()
}

pub fn read_u40(pack: &[u8]) -> usize {
    let a = pack[0] as usize;
    let b = read_u32(&pack[1..]) as usize;
    a + (b << 8)
}

pub fn read_u48(pack: &[u8]) -> usize {
    let a = read_u16(&pack[0..2]) as usize;
    let b = read_u16(&pack[2..4]) as usize;
    let c = read_u16(&pack[4..]) as usize;
    a + (b << 16) + (c << 32)
}

pub fn read_u56(pack: &[u8]) -> usize {
    let a = pack[0] as usize;
    let b = read_u16(&pack[1..3]) as usize;
    let c = read_u32(&pack[3..]) as usize;
    a + (b << 8) + (c << 24)
}


pub fn read_u64(pack: &[u8]) -> u64 {
    let mut rdr = Cursor::new(pack);
    rdr.read_u64::<LittleEndian>().unwrap()
}

pub fn write_u64(num: u64) -> Vec<u8> {
    let mut rdr = Vec::new();
    rdr.write_u64::<LittleEndian>(num).unwrap();
    return rdr;
}


pub fn read_i64(pack: &[u8]) -> i64 {
    let mut rdr = Cursor::new(pack);
    rdr.read_i64::<LittleEndian>().unwrap()
}

pub fn read_big_u64(pack: &[u8]) -> u64 {
    let mut rdr = Cursor::new(pack);
    rdr.read_u64::<BigEndian>().unwrap()
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

pub fn read_f32(pack: &[u8]) -> f32 {
    let mut rdr = Cursor::new(pack);
    rdr.read_f32::<LittleEndian>().unwrap()
}

pub fn read_f64(pack: &[u8]) -> f64 {
    let mut rdr = Cursor::new(pack);
    rdr.read_f64::<LittleEndian>().unwrap()
}

pub fn read_nbytes<R: Read, S: Into<usize>>(r: &mut R, desired_bytes: S) -> io::Result<Vec<u8>> {
    let mut into = vec![0u8; desired_bytes.into()];
    r.read_exact(&mut into)?;
    Ok(into)
}

///
/// 接收client返回的数据
///
pub fn rec_packet(conn: &mut TcpStream) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buf: Vec<u8> = vec![];
    let mut header: Vec<u8> = vec![0u8;9];
    conn.read_exact(&mut header)?;
    let payload = crate::readvalue::read_u64(&header[1..]);
    let mut payload_buf: Vec<u8> = vec![0u8; payload as usize];
    conn.read_exact(&mut payload_buf)?;
    buf.extend(header);
    buf.extend(payload_buf);
    Ok(buf)
}

