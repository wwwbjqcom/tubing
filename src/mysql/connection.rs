/*
@author: xiao cai niao
@datetime: 2019/9/21
*/

use crate::{Config, readvalue, MyConfig, Platform};
use std::net::{TcpStream, IpAddr, Ipv4Addr, SocketAddr};
use std::process;
use std::io::{Cursor, Read, Seek, Write};
use std::borrow::Borrow;
use byteorder::ReadBytesExt;
use mysql_common::crypto::encrypt;
use byteorder::LittleEndian;
use crate::mysql::Result;
use crate::mysql::connection::response::pack_header;
use tracing::field::debug;
use std::collections::HashMap;

pub mod pack;
pub mod response;


/// 记录客户端验证使用的用户信息
#[derive(Clone, Debug)]
pub struct UserInfo{
    pub user: String,
    pub password: String,
    pub platform: String,
}
impl UserInfo{
    pub fn new(conf: &Platform) -> UserInfo{
        UserInfo{
            user: conf.user.clone(),
            password: conf.clipassword.clone(),
            platform: conf.platform.clone()
        }
    }
}

/// 所有客户端验证的用户/密码信息， 用于验证登陆及权限
#[derive(Clone, Debug)]
pub struct AllUserInfo{
    pub all_info: HashMap<String, UserInfo>     //user作为key, 方便查找
}
impl AllUserInfo{
    pub fn new(conf: &MyConfig) -> AllUserInfo{
        let mut all_info = HashMap::new();
        let admin_user_info = UserInfo{user: conf.user.clone(), password: conf.password.clone(), platform: "admin".to_string()};
        all_info.insert(conf.user.clone(), admin_user_info);
        for platform in &conf.platform{
            let platform_user_info = UserInfo::new(platform);
            all_info.insert(platform.user.clone(), platform_user_info);
        }
        AllUserInfo{
            all_info
        }
    }

    /// 检查用户名是否存在
    pub fn check_user_name(&self, user_name: &String) -> bool{
        if self.all_info.contains_key(user_name) {
            return true;
        }
        return false;
    }

    /// 获取用户密码
    pub fn get_user_password(&self, user_name: &String) -> String{
        match self.all_info.get(user_name){
            Some(user_info) => {
                return user_info.password.clone();
            }
            None => {
                return "".to_string();
            }
        }
    }

    /// 检查用户权限， 是否能连接对应的业务平台
    pub fn check_platform_privileges(&self, platform: &String, user_name: &String) -> bool{
        match self.all_info.get(user_name){
            Some(user_info) => {
                if user_info.platform == "admin".to_string() || &user_info.platform == platform{
                    return true;
                }
                return false;
            }
            None => {
                return false;
            }
        }
    }
}

/// mysql procotol packet header
#[derive(Debug)]
pub struct PacketHeader {
    pub payload: u32,
    pub seq_id: u8,
}

impl PacketHeader{
    pub fn new(buf: &[u8]) -> Result<PacketHeader>{
        let mut buf = Cursor::new(buf.clone());
        let payload = buf.read_u24::<LittleEndian>()?;
        Ok(PacketHeader{
            payload,
            seq_id: buf.read_u8()?
        })
    }
}

#[derive(Debug)]
pub enum MysqlPacketType {
//    Ok,
    Error,
    Handshake,
//    EOF,
    Unknown
}

///
/// 记录每个mysql连接信息
#[derive(Debug)]
pub struct MysqlConnection {
    pub conn: TcpStream,
    pub packet_type: MysqlPacketType,
    pub status: bool,
    pub errorcode: usize,
    pub error: String,
}

impl MysqlConnection{
    pub fn new(conf: &Config) -> Result<MysqlConnection> {
        let conn = crate::mysql::conn(&conf.host_info)?;
        Ok(MysqlConnection{
            conn,
            packet_type: MysqlPacketType::Unknown,
            status: false,
            errorcode: 0,
            error: "".to_string(),
        })
    }
    /// create remote mysql db connection
    ///
    /// shake hands with the backend mysql db to establish each connection
    pub fn create(&mut self, conf: &Config) -> Result<()>{
        let (packet_buf,_) = self.get_packet_from_stream()?;
        let mut read_buf = Cursor::new(packet_buf);
        self.get_packet_type(&mut read_buf)?;
        match self.packet_type{
            MysqlPacketType::Handshake => {
                let mut packet_buf = vec![];
                read_buf.read_to_end(&mut packet_buf)?;
                let handshake = pack::HandshakePacket::new(&packet_buf)?;
                self.create_conn(conf, &handshake)?;
            }
            MysqlPacketType::Error => {
                self.errorcode = read_buf.read_u16::<LittleEndian>()?.into();
                let mut a = vec![];
                read_buf.read_to_end(&mut a)?;
                self.error = readvalue::read_string_value(&a)
            }
            _ => {}
        }
        Ok(())
    }

    /// Establish a connection through the interaction of data packets
    pub fn create_conn(&mut self, conf: &Config, handshake: &pack::HandshakePacket) -> Result<()>{
        //organize handshake packet and send to server
        let handshake_response = response::LocalInfo::new(conf.program_name.borrow(), conf.database.len() as u8);
        let packet_type = crate::mysql::PackType::HandShakeResponse;
        let v = response::LocalInfo::pack_payload(
            &handshake_response,&handshake,&packet_type,conf)?;

        self.conn.write_all(v.as_ref())?;

        // check response packet for server
        let (packet_buf,_) = self.get_packet_from_stream()?;


        let mut tmp_auth_data = vec![];
        if packet_buf[0] == 0xFE {
            //Re-verify password
            let (auth_data, tmp) = response::authswitchrequest(&handshake, packet_buf.as_ref(), conf);
            tmp_auth_data = tmp;
            self.conn.write_all(auth_data.as_ref())?;
        }

        let (packet_buf,_) = self.get_packet_from_stream()?;
        if pack::check_pack(&packet_buf) {
            if packet_buf[1] == 4{
                if self.sha2_auth( &tmp_auth_data, conf)?{
                    self.status = true;
                }
            }else if packet_buf[1] == 3 {
                let (packet_buf,_) = self.get_packet_from_stream()?;
                if !pack::check_pack(&packet_buf) {
                    self.erro_pack(&packet_buf);
                }else {
                    self.status = true;
                }
            }else {
                self.status = true;
            }

        } else {
            self.erro_pack(&packet_buf);
        }
        Ok(())
    }

    ///
    pub fn sha2_auth(&mut self, auth_data: &Vec<u8>, conf: &Config) -> Result<bool> {
        let (payload, seq_id) = ([0x02],5);
        let mut packet: Vec<u8> = vec![];
        packet.extend(pack_header(&payload, seq_id));
        packet.extend(payload.iter());
        self.conn.write_all(&packet)?;

        let (packet_buf,_) = self.get_packet_from_stream()?;

        let key = &packet_buf[1..];
        let mut password = conf.password.as_bytes().to_vec();
        password.push(0);
        for i in 0..password.len() {
            password[i] ^= auth_data[i % auth_data.len()];
        }
        let encrypted_pass = encrypt(&password, &key);
        let mut packet: Vec<u8> = vec![];
        packet.extend(pack_header(&encrypted_pass, 7));
        packet.extend(encrypted_pass.iter());
        write_value(&mut self.conn, &packet)?;

        let (packet_buf,_) = self.get_packet_from_stream()?;
        if pack::check_pack(&packet_buf) {
            return Ok(true);
        } else {
            self.erro_pack(&packet_buf);
            return Ok(false);
        }
    }

    fn get_packet_type<F: Read + Seek>(&mut self, buf: &mut F) -> Result<()>{
        let packet_type = buf.read_u8()?;
        if packet_type == 255{
            self.packet_type = MysqlPacketType::Error;
        }else if packet_type == 10 {
            self.packet_type = MysqlPacketType::Handshake;
        }
        return Ok(())
    }


    /// Type	    Name	            Description
    /// int<1>	    header	            0xFF ERR packet header
    /// int<2>	    error_code	        error-code
    /// if capabilities & CLIENT_PROTOCOL_41 {
    /// string[1]	sql_state_marker	# marker of the SQL state
    /// string[5]	sql_state	        SQL state
    /// }
    /// string<EOF>	error_message	    human readable error message

    pub fn erro_pack(&mut self, pack: &Vec<u8>) {
        self.errorcode = readvalue::read_u16(&pack[1..1+2]) as usize;
        self.error = readvalue::read_string_value(&pack[3..]);

    }

    /// read packet from socket
    ///
    /// if payload = 0xffffff： this packet more than the 64MB
    pub fn get_packet_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let (mut buf,header) = self.get_from_stream()?;
        while header.payload == 0xffffff{
            debug(header.payload);
            let (buf_tmp,_) = self.get_from_stream()?;
            buf.extend(buf_tmp);
        }
        Ok((buf, header))
    }

    /// read on packet from socket
    fn get_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let mut header_buf = vec![0 as u8; 4];
        let mut header: PacketHeader = PacketHeader { payload: 0, seq_id: 0 };
        loop {
            match self.conn.read_exact(&mut header_buf){
                Ok(_) => {
                    header = PacketHeader::new(&header_buf)?;
                    if header.payload > 0 {
                        break;
                    }
                }
                Err(e) => {
                    debug(e);
                }
            }

        }

        //read the actual data through the payload data obtained through the packet header
        let mut packet_buf  = vec![0 as u8; header.payload as usize];
        match self.conn.read_exact(&mut packet_buf) {
            Ok(_) =>{}
            Err(e) => {
                debug(format!("read packet error:{}",e));
            }
        }
        return Ok((packet_buf,header));
    }
}

//向连接写入数据
pub fn write_value(stream: &mut TcpStream, buf: &Vec<u8>) -> Result<()> {
    stream.write_all(buf)?;
    Ok(())
}





