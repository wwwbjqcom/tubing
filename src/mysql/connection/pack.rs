/*
@author: xiao cai niao
@datetime: 2019/9/21
*/


use crate::readvalue;
use std::cmp::max;

/// the handshake packet sent by the server
#[derive(Debug)]
pub struct HandshakePacket {
    pub packet_type: u8,
    pub server_version: String,
    pub thread_id: u32,
    pub auth_plugin_data: Vec<u8>,
    pub capability_flags: u32,
    pub capability_flags_2: u32,
    pub character_set_id: u8,
    pub status_flags: u16,
    pub auth_plugin_data_len: u8,
    pub auth_plugin_name: String,
}

impl HandshakePacket{
    pub fn new(buf: &Vec<u8>) -> Result<Self, &'static str>{
        let plugin_auth = 1<<19;

        if buf.len() < 1 {
            return Err("packet is error");
        }
        let mut offset: usize = 0;
//        let packet_type = buf[0];
//
//        if packet_type != 10{
//            return Err("packet type invalid");
//        }

        offset += 1;
        let mut index= 0 ;
        for (b,item )in buf[1..].iter().enumerate() {
            if item == &0x00 {
                index = b;
                break;
            }
        }

        let server_version = readvalue::read_string_value(&buf[offset..=index]);
        offset += index + 1;

        let thread_id = readvalue::read_u32(&buf[offset..offset+4]);
        offset += 4;

        let mut auth_plugin_data: Vec<u8> = buf[offset..offset + 8].to_owned();
        //let mut auth_plugin_data = readvalue::read_string_value(&buf[offset..offset + 8]);
        offset += 8 + 1;

        let mut capability_flags = readvalue::read_u16(&buf[offset..offset+2]) as u32;
        offset += 2;

        let character_set_id = buf[offset];
        offset += 1;

        let status_flags = readvalue::read_u16(&buf[offset..offset+2]);
        offset += 2 ;

        let capability_flags_2 = readvalue::read_u16(&buf[offset..offset + 2]) as u32;
        offset += 2 ;
        capability_flags |= capability_flags_2 << 16;


        let mut auth_plugin_data_len = buf[offset];
        auth_plugin_data_len = max(auth_plugin_data_len - 8,13);
        offset += 1 ;
        offset += 10;

        if buf.len() >= offset + auth_plugin_data_len as usize {
            auth_plugin_data.extend(buf[offset..offset+auth_plugin_data_len as usize].iter().clone());
            //auth_plugin_data.push_str(readvalue::read_string_value(&buf[offset..offset + auth_plugin_data_len as usize]).as_ref());
            offset += auth_plugin_data_len as usize;
        }

        let mut auth_plugin_name= "".to_string();
        if capability_flags & plugin_auth > 1{
            if buf.len() >= offset{
                for (b,item )in buf[offset..].iter().enumerate() {
                    if item == &0x00 {
                        auth_plugin_name = readvalue::read_string_value(&buf[offset..offset+b]);
                        break;
                    }
                }
            }
        }

        Ok(
            Self {
                packet_type:10,
                server_version,
                thread_id,
                auth_plugin_data,
                capability_flags,
                capability_flags_2,
                character_set_id,
                status_flags,
                auth_plugin_data_len,
                auth_plugin_name
            }
        )
    }
}

/// check ok_packet/err_packet
pub fn check_pack(pack: &Vec<u8>) -> bool {
    let pack_type = Some(pack[0]);
    match pack_type {
        Some(0) => true,
        Some(254) => true,
        Some(255) => false,
        _ => true
    }
}

