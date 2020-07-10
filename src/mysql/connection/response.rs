/*
@author: xiao cai niao
@datetime: 2019/9/19
*/

use crate::readvalue;
use crate::mysql::connection::pack::HandshakePacket;
use crate::mysql::PackType;
use crate::mysql::FlagsMeta;
use std::convert::TryInto;
use crate::Config;
use crate::mysql::scramble;
use crate::mysql::connection::pack;
use std::net::TcpStream;
use mysql_common::crypto::encrypt;
use mysql_common;



//handshakereponse
#[derive(Debug)]
pub struct LocalInfo{
    pub client_name: String,
    pub pid: u32,
    pub client_version: String,
    pub program_name: String,
    pub client_flag: i32,
    pub max_packet_size : u32
}

impl LocalInfo {
    pub fn new(program: &String,database: u8) -> Self{
        let mut client_flag = 0;
        let flags_meta = FlagsMeta::new();
        let capabilities = flags_meta.long_password|
            flags_meta.long_flag|flags_meta.protocol_41|flags_meta.transactions|
            flags_meta.secure_connection|flags_meta.multi_results|
            flags_meta.client_plugin_auth| flags_meta.client_plugin_auth_lenenc_client_data|
            flags_meta.client_connect_attrs;
        client_flag |= capabilities;
        if database > 0{
            client_flag |= flags_meta.client_connect_with_db
        }
        client_flag |= flags_meta.multi_results;
        Self{
            client_name: String::from("MysqlBus"),
            pid:123,
            client_version:String::from("5.7.20"),
            program_name: program.clone(),
            client_flag,
            max_packet_size:16777215
        }
    }

    pub unsafe fn pack_handshake(&self, buf: &HandshakePacket, config: &Config, database: u8) -> Vec<u8>{
        //Package
        let mut rdr = Vec::new();
        let flags_meta = FlagsMeta::new();
        //rdr.extend(buf.server_version.clone().into_bytes());
        rdr.extend(readvalue::write_i32(self.client_flag));
        rdr.extend(readvalue::write_u32(self.max_packet_size));
        rdr.push(buf.character_set_id);
        let mut num = 0;
        while num < 23{
            //rdr.extend(String::from("").into_bytes());
            rdr.push(0);
            num += 1;
        }

        rdr.extend(config.muser.clone().into_bytes());
        rdr.push(0);

        let sha1_pass= scramble::get_sha1_pass(&config.mpassword, &buf.auth_plugin_name, &buf.auth_plugin_data);

        if buf.capability_flags & (flags_meta.client_plugin_auth_lenenc_client_data as u32) > 0{
            rdr.push(sha1_pass.len() as u8);
            rdr.extend(sha1_pass);
        }else if buf.capability_flags & (flags_meta.secure_connection as u32) > 0 {
            rdr.push(sha1_pass.len() as u8);
            rdr.extend(sha1_pass);
        }else {
            rdr.extend(sha1_pass);
            rdr.push(0);
        }

        if buf.capability_flags & (flags_meta.client_connect_with_db as u32) > 0{
            if database > 0{
                rdr.extend(config.database.clone().into_bytes())
            }
            rdr.push(0);
        }
        if buf.capability_flags & flags_meta.client_plugin_auth as u32 > 0{
            //rdr.extend(String::from("").into_bytes());
            rdr.push(0);
            rdr.push(0);
        }

        let connect_attrs = Self::pack_connect_attrs(self);
        if buf.capability_flags & (flags_meta.client_connect_attrs as u32) > 0{
            rdr.push(connect_attrs.len().try_into().unwrap());
            rdr.extend(connect_attrs);
        }

        return rdr;
    }

    fn pack_connect_attrs(&self) -> Vec<u8> {
        let mut connect_attrs = Vec::new();
        connect_attrs.push(self.client_name.len() as u8);
        connect_attrs.extend(self.client_name.clone().into_bytes());
        connect_attrs.push(self.client_version.len() as u8);
        connect_attrs.extend(self.client_version.clone().into_bytes());
        connect_attrs.push(self.program_name.len() as u8);
        connect_attrs.extend(self.program_name.clone().into_bytes());

        return connect_attrs;
    }



}

impl LocalInfo {

    pub fn pack_payload(&self, buf: &HandshakePacket, pack_type: &PackType, config: &Config) -> Result<Vec<u8> ,&'static str> {
        //assemble the handshakeresponse package from here, return a complete return package
        //all data is put in verctor type
        let mut _payload = Vec::new();
        let mut payload_value: Vec<u8> = vec![];
        let mut packet_header: Vec<u8> = vec![];
        match pack_type {
            PackType::HandShake => {},
            PackType::HandShakeResponse => unsafe {
                if config.database.len() > 0 {
                    payload_value = Self::pack_handshake(&self,buf, config, 1);
                }else {
                    payload_value = Self::pack_handshake(&self,buf, config, 0) ;
                }
            }
            _ => payload_value = vec![]
        }

        if payload_value.len() > 0{
            packet_header = pack_header(&payload_value,1)
        }else {
            return Err("paket payload is failed!!");
        }
        _payload.extend(packet_header);
        _payload.extend(payload_value);
        return Ok(_payload);
    }
}

/// auth_switch needs to verify the password again
pub fn authswitchrequest(handshake: &HandshakePacket,buf: &Vec<u8>,conf: &Config) -> (Vec<u8>,Vec<u8>) {
    let mut packet: Vec<u8> = vec![];
    let mut payload: Vec<u8> = vec![];
    let mut offset = 1;
    let mut auth_plugin_name = String::from("");
    for (b,item )in buf[offset..].iter().enumerate(){
        if item == &0x00 {
            auth_plugin_name = readvalue::read_string_value(&buf[offset..offset+b]);
            offset += b + 1;
            break;
        }
    }
    let auth_plugin_data = &buf[offset..];

    if auth_plugin_name.len() > 0 {
        let flags_meta = FlagsMeta::new();
        if handshake.capability_flags & flags_meta.client_plugin_auth as u32 > 0 {
            payload = scramble::get_sha1_pass(&conf.mpassword, &auth_plugin_name, &auth_plugin_data.to_vec());
        }
    }

    packet.extend(pack_header(payload.as_ref(), 3));
    packet.extend(payload);
    return (packet, auth_plugin_data.to_vec());
}




/// assemble the heder part
pub fn pack_header(buf: &[u8], seq: u8) -> Vec<u8> {
    let mut _header = Vec::new();
    let payload = readvalue::write_u24(buf.len() as u32);
    _header.extend(payload);
    _header.push(seq);
    return _header;
}
